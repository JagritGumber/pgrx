//! Per-query byte arena for zero-allocation query execution.
//!
//! VictoriaMetrics-inspired: strings are byte ranges into a single contiguous buffer.
//! ArenaValue is Copy (16 bytes) — no Arc, no Drop, no refcount.
//! All memory freed at once when QueryArena drops.

use std::hash::{Hash, Hasher};

/// Per-query byte arena. Strings and vectors are packed contiguously.
/// Created at query start, dropped at query end — single deallocation.
#[derive(Debug)]
pub struct QueryArena {
    bytes: Vec<u8>,
    floats: Vec<f32>,
}

/// Reference to a string in the arena's byte buffer. 8 bytes, Copy.
#[derive(Clone, Copy, Debug)]
pub struct ArenaStr {
    pub offset: u32,
    pub len: u32,
}

/// Reference to a vector (f32 slice) in the arena's float buffer. 8 bytes, Copy.
#[derive(Clone, Copy, Debug)]
pub struct ArenaVec {
    pub offset: u32,
    pub len: u32,
}

/// Arena-backed value. 16 bytes, Copy, no Drop.
/// Compared to Value (32 bytes, has Arc<str> and Vec<f32> needing Drop),
/// this eliminates all per-value heap allocation and reference counting.
#[derive(Clone, Copy, Debug)]
pub enum ArenaValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(ArenaStr),
    Bytea(ArenaStr),
    Vector(ArenaVec),
}

impl QueryArena {
    pub fn new() -> Self {
        Self {
            bytes: Vec::with_capacity(4096),
            floats: Vec::with_capacity(256),
        }
    }

    pub fn with_capacity(bytes: usize, floats: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(bytes),
            floats: Vec::with_capacity(floats),
        }
    }

    /// Allocate a string into the arena. Returns an ArenaStr reference.
    pub fn alloc_str(&mut self, s: &str) -> ArenaStr {
        let offset = self.bytes.len() as u32;
        self.bytes.extend_from_slice(s.as_bytes());
        ArenaStr {
            offset,
            len: s.len() as u32,
        }
    }

    /// Resolve an ArenaStr to &str.
    #[inline(always)]
    pub fn get_str(&self, s: ArenaStr) -> &str {
        let start = s.offset as usize;
        let end = start + s.len as usize;
        // SAFETY: we only store valid UTF-8 via alloc_str
        unsafe { std::str::from_utf8_unchecked(&self.bytes[start..end]) }
    }

    /// Allocate a vector (f32 slice) into the arena.
    pub fn alloc_vec(&mut self, v: &[f32]) -> ArenaVec {
        let offset = self.floats.len() as u32;
        self.floats.extend_from_slice(v);
        ArenaVec {
            offset,
            len: v.len() as u32,
        }
    }

    /// Raw byte buffer access (for direct comparison).
    #[inline(always)]
    pub fn bytes_ref(&self) -> &[u8] {
        &self.bytes
    }

    /// Resolve an ArenaVec to &[f32].
    pub fn get_vec(&self, v: ArenaVec) -> &[f32] {
        let start = v.offset as usize;
        let end = start + v.len as usize;
        &self.floats[start..end]
    }
}

// ── ArenaValue methods ───────────────────────────────────────────────

impl ArenaValue {
    /// Convert a storage Value to an ArenaValue, copying data into the arena.
    pub fn from_value(val: &crate::types::Value, arena: &mut QueryArena) -> Self {
        use crate::types::Value;
        match val {
            Value::Null => ArenaValue::Null,
            Value::Bool(b) => ArenaValue::Bool(*b),
            Value::Int(i) => ArenaValue::Int(*i),
            Value::Float(f) => ArenaValue::Float(*f),
            Value::Text(s) => ArenaValue::Text(arena.alloc_str(s)),
            Value::Bytea(b) => {
                let offset = arena.bytes.len() as u32;
                arena.bytes.extend_from_slice(b);
                ArenaValue::Bytea(ArenaStr {
                    offset,
                    len: b.len() as u32,
                })
            }
            Value::Vector(v) => ArenaValue::Vector(arena.alloc_vec(v)),
        }
    }

    /// Convert back to storage Value (allocates Arc/Vec on heap).
    /// Used at boundaries: arena → wire protocol, arena → storage.
    pub fn to_value(&self, arena: &QueryArena) -> crate::types::Value {
        use crate::types::Value;
        use std::sync::Arc;
        match self {
            ArenaValue::Null => Value::Null,
            ArenaValue::Bool(b) => Value::Bool(*b),
            ArenaValue::Int(i) => Value::Int(*i),
            ArenaValue::Float(f) => Value::Float(*f),
            ArenaValue::Text(s) => Value::Text(Arc::from(arena.get_str(*s))),
            ArenaValue::Bytea(s) => {
                let start = s.offset as usize;
                let end = start + s.len as usize;
                Value::Bytea(arena.bytes[start..end].to_vec())
            }
            ArenaValue::Vector(v) => Value::Vector(arena.get_vec(*v).to_vec()),
        }
    }

    /// Format to text representation (for wire protocol).
    pub fn to_text(&self, arena: &QueryArena) -> Option<String> {
        match self {
            ArenaValue::Null => None,
            ArenaValue::Bool(b) => Some(if *b { "t" } else { "f" }.into()),
            ArenaValue::Int(i) => Some(i.to_string()),
            ArenaValue::Float(f) => Some(crate::types::format_float(*f)),
            ArenaValue::Text(s) => Some(arena.get_str(*s).to_string()),
            ArenaValue::Bytea(s) => {
                let start = s.offset as usize;
                let end = start + s.len as usize;
                Some(format!(
                    "\\x{}",
                    arena.bytes[start..end]
                        .iter()
                        .map(|b| format!("{:02x}", b))
                        .collect::<String>()
                ))
            }
            ArenaValue::Vector(v) => {
                let data = arena.get_vec(*v);
                let inner: Vec<String> = data.iter().map(|f| {
                    if *f == f.trunc() && f.is_finite() && *f >= -2_147_483_648.0 && *f < 2_147_483_648.0 {
                        let i = *f as i32;
                        if i as f32 == *f {
                            format!("{}", i)
                        } else {
                            format!("{}", f)
                        }
                    } else {
                        format!("{}", f)
                    }
                }).collect();
                Some(format!("[{}]", inner.join(",")))
            }
        }
    }

    /// Compare two ArenaValues. Returns ordering or None for incompatible types.
    #[inline]
    pub fn compare(&self, other: &Self, arena: &QueryArena) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering;
        match (self, other) {
            (ArenaValue::Null, _) | (_, ArenaValue::Null) => None,
            (ArenaValue::Int(a), ArenaValue::Int(b)) => a.partial_cmp(b),
            (ArenaValue::Float(a), ArenaValue::Float(b)) => a.partial_cmp(b),
            (ArenaValue::Int(a), ArenaValue::Float(b)) => (*a as f64).partial_cmp(b),
            (ArenaValue::Float(a), ArenaValue::Int(b)) => a.partial_cmp(&(*b as f64)),
            (ArenaValue::Bool(a), ArenaValue::Bool(b)) => a.partial_cmp(b),
            (ArenaValue::Text(a), ArenaValue::Text(b)) => {
                Some(arena.get_str(*a).cmp(arena.get_str(*b)))
            }
            (ArenaValue::Vector(_), ArenaValue::Vector(_)) => None, // vectors are not orderable (matches Value::compare)
            _ => None,
        }
    }

    /// Equality check with arena context.
    #[inline]
    pub fn eq_with(&self, other: &Self, arena: &QueryArena) -> bool {
        match (self, other) {
            (ArenaValue::Null, ArenaValue::Null) => true,
            (ArenaValue::Bool(a), ArenaValue::Bool(b)) => a == b,
            (ArenaValue::Int(a), ArenaValue::Int(b)) => a == b,
            (ArenaValue::Float(a), ArenaValue::Float(b)) => {
                // NaN == NaN for GROUP BY (matches Value::PartialEq behavior)
                if a.is_nan() && b.is_nan() {
                    return true;
                }
                a == b
            }
            // No cross-type Int/Float equality — matches Value::PartialEq behavior.
            // Cross-type comparison lives in compare() (for ORDER BY), not eq_with (for Hash/Eq).
            (ArenaValue::Text(a), ArenaValue::Text(b)) => {
                // Fast path: same offset = same string
                if a.offset == b.offset && a.len == b.len {
                    return true;
                }
                arena.get_str(*a) == arena.get_str(*b)
            }
            (ArenaValue::Bytea(a), ArenaValue::Bytea(b)) => {
                let sa = a.offset as usize;
                let sb = b.offset as usize;
                arena.bytes[sa..sa + a.len as usize] == arena.bytes[sb..sb + b.len as usize]
            }
            (ArenaValue::Vector(a), ArenaValue::Vector(b)) => {
                // Bitwise comparison (NaN == NaN for GROUP BY), matching Value::PartialEq.
                let va = arena.get_vec(*a);
                let vb = arena.get_vec(*b);
                va.len() == vb.len()
                    && va.iter().zip(vb.iter()).all(|(x, y)| x.to_bits() == y.to_bits())
            }
            _ => false,
        }
    }

    /// Hash with arena context (for GROUP BY keys).
    pub fn hash_with(&self, arena: &QueryArena, state: &mut impl Hasher) {
        std::mem::discriminant(self).hash(state);
        match self {
            ArenaValue::Null => {}
            ArenaValue::Bool(b) => b.hash(state),
            ArenaValue::Int(i) => i.hash(state),
            ArenaValue::Float(f) => {
                // Canonicalize NaN and negative zero for consistent hashing
                let bits = if f.is_nan() {
                    f64::NAN.to_bits()
                } else if *f == 0.0 {
                    0u64
                } else {
                    f.to_bits()
                };
                bits.hash(state);
            }
            ArenaValue::Text(s) => arena.get_str(*s).hash(state),
            ArenaValue::Bytea(s) => {
                let start = s.offset as usize;
                arena.bytes[start..start + s.len as usize].hash(state);
            }
            ArenaValue::Vector(v) => {
                let data = arena.get_vec(*v);
                data.len().hash(state);
                for f in data {
                    f.to_bits().hash(state);
                }
            }
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, ArenaValue::Null)
    }
}

/// Wrapper for using ArenaValue slices as HashMap keys.
/// Carries arena reference for Hash/Eq resolution.
#[derive(Debug)]
pub struct ArenaKey<'a> {
    pub values: Vec<ArenaValue>,
    pub arena: &'a QueryArena,
}

impl Hash for ArenaKey<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for v in &self.values {
            v.hash_with(self.arena, state);
        }
    }
}

impl PartialEq for ArenaKey<'_> {
    fn eq(&self, other: &Self) -> bool {
        if self.values.len() != other.values.len() {
            return false;
        }
        self.values
            .iter()
            .zip(other.values.iter())
            .all(|(a, b)| a.eq_with(b, self.arena))
    }
}

impl Eq for ArenaKey<'_> {}

/// Single-value key wrapper for hash join.
pub struct ArenaSingleKey<'a> {
    pub value: ArenaValue,
    pub arena: &'a QueryArena,
}

impl Hash for ArenaSingleKey<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash_with(self.arena, state);
    }
}

impl PartialEq for ArenaSingleKey<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.value.eq_with(&other.value, self.arena)
    }
}

impl Eq for ArenaSingleKey<'_> {}

/// Convert a slice of storage rows to arena rows.
pub fn rows_to_arena(
    rows: &[Vec<crate::types::Value>],
    arena: &mut QueryArena,
) -> Vec<Vec<ArenaValue>> {
    rows.iter()
        .map(|row| {
            row.iter()
                .map(|val| ArenaValue::from_value(val, arena))
                .collect()
        })
        .collect()
}

/// Convert arena rows back to storage rows.
pub fn rows_from_arena(
    rows: &[Vec<ArenaValue>],
    arena: &QueryArena,
) -> Vec<Vec<crate::types::Value>> {
    rows.iter()
        .map(|row| row.iter().map(|val| val.to_value(arena)).collect())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;
    use std::sync::Arc;

    #[test]
    fn arena_alloc_and_resolve_str() {
        let mut arena = QueryArena::new();
        let s1 = arena.alloc_str("hello");
        let s2 = arena.alloc_str("world");
        assert_eq!(arena.get_str(s1), "hello");
        assert_eq!(arena.get_str(s2), "world");
    }

    #[test]
    fn arena_alloc_and_resolve_vec() {
        let mut arena = QueryArena::new();
        let v = arena.alloc_vec(&[1.0, 2.0, 3.0]);
        assert_eq!(arena.get_vec(v), &[1.0, 2.0, 3.0]);
    }

    #[test]
    fn arena_value_roundtrip() {
        let mut arena = QueryArena::new();
        let values = vec![
            Value::Null,
            Value::Bool(true),
            Value::Int(42),
            Value::Float(3.14),
            Value::Text(Arc::from("test")),
            Value::Vector(vec![1.0, 2.0]),
        ];
        let arena_vals: Vec<ArenaValue> =
            values.iter().map(|v| ArenaValue::from_value(v, &mut arena)).collect();
        let back: Vec<Value> = arena_vals.iter().map(|v| v.to_value(&arena)).collect();
        assert_eq!(values, back);
    }

    #[test]
    fn arena_value_is_copy() {
        let mut arena = QueryArena::new();
        let v = ArenaValue::Text(arena.alloc_str("hello"));
        let v2 = v; // Copy, not move
        assert!(v.eq_with(&v2, &arena));
    }

    #[test]
    fn arena_value_compare() {
        let mut arena = QueryArena::new();
        let a = ArenaValue::Text(arena.alloc_str("abc"));
        let b = ArenaValue::Text(arena.alloc_str("def"));
        assert_eq!(
            a.compare(&b, &arena),
            Some(std::cmp::Ordering::Less)
        );
    }

    #[test]
    fn arena_key_hash_eq() {
        use std::collections::HashMap;
        let mut arena = QueryArena::new();
        // Allocate strings first (mutable borrow), then create keys (immutable borrow)
        let s1 = arena.alloc_str("a");
        let s2 = arena.alloc_str("a"); // different offset, same content
        let k1 = ArenaKey {
            values: vec![ArenaValue::Int(1), ArenaValue::Text(s1)],
            arena: &arena,
        };
        let k2 = ArenaKey {
            values: vec![ArenaValue::Int(1), ArenaValue::Text(s2)],
            arena: &arena,
        };
        assert_eq!(k1, k2);

        let mut map: HashMap<ArenaKey, usize> = HashMap::new();
        map.insert(k1, 0);
        let k3 = ArenaKey {
            values: vec![ArenaValue::Int(1), ArenaValue::Text(s2)],
            arena: &arena,
        };
        assert!(map.contains_key(&k3));
    }

    #[test]
    fn arena_value_size() {
        assert_eq!(std::mem::size_of::<ArenaValue>(), 16);
    }

    #[test]
    fn arena_float_infinity_wire_format() {
        let arena = QueryArena::new();
        let inf = ArenaValue::Float(f64::INFINITY);
        assert_eq!(inf.to_text(&arena), Some("Infinity".to_string()));
        let neg_inf = ArenaValue::Float(f64::NEG_INFINITY);
        assert_eq!(neg_inf.to_text(&arena), Some("-Infinity".to_string()));
        let nan = ArenaValue::Float(f64::NAN);
        assert_eq!(nan.to_text(&arena), Some("NaN".to_string()));
    }
}
