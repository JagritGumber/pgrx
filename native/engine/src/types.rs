use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::sync::Arc;

/// Runtime values for SQL expressions.
///
/// PartialEq, Eq, and Hash are manually implemented to satisfy HashMap's
/// contract for Float values: NaN == NaN (for GROUP BY grouping, matching
/// PostgreSQL behavior) and +0.0 == -0.0 (IEEE 754 equality).
///
/// Text uses Arc<str> for zero-copy sharing: identical strings are stored once
/// in memory, reducing heap allocation for repeated values (VM philosophy).
#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(Arc<str>),
    Bytea(Vec<u8>),
    Vector(Vec<f32>),
}

// Manual Serialize/Deserialize to handle Arc<str> transparently as String.
impl Serialize for Value {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Value::Null => serializer.serialize_unit_variant("Value", 0, "Null"),
            Value::Bool(b) => serializer.serialize_newtype_variant("Value", 1, "Bool", b),
            Value::Int(i) => serializer.serialize_newtype_variant("Value", 2, "Int", i),
            Value::Float(f) => serializer.serialize_newtype_variant("Value", 3, "Float", f),
            Value::Text(s) => serializer.serialize_newtype_variant("Value", 4, "Text", s.as_ref()),
            Value::Bytea(b) => serializer.serialize_newtype_variant("Value", 5, "Bytea", b),
            Value::Vector(v) => serializer.serialize_newtype_variant("Value", 6, "Vector", v),
        }
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        enum ValueHelper {
            Null,
            Bool(bool),
            Int(i64),
            Float(f64),
            Text(String),
            Bytea(Vec<u8>),
            Vector(Vec<f32>),
        }
        let helper = ValueHelper::deserialize(deserializer)?;
        Ok(match helper {
            ValueHelper::Null => Value::Null,
            ValueHelper::Bool(b) => Value::Bool(b),
            ValueHelper::Int(i) => Value::Int(i),
            ValueHelper::Float(f) => Value::Float(f),
            ValueHelper::Text(s) => Value::Text(Arc::from(s)),
            ValueHelper::Bytea(b) => Value::Bytea(b),
            ValueHelper::Vector(v) => Value::Vector(v),
        })
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => {
                if a.is_nan() && b.is_nan() {
                    true // NaN groups with NaN in GROUP BY (PostgreSQL behavior)
                } else {
                    a == b // IEEE 754: +0.0 == -0.0
                }
            }
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Bytea(a), Value::Bytea(b)) => a == b,
            (Value::Vector(a), Value::Vector(b)) => a.len() == b.len() && a.iter().zip(b.iter()).all(|(x, y)| x.to_bits() == y.to_bits()),
            _ => false,
        }
    }
}

impl Eq for Value {}

impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Value::Null => {}
            Value::Bool(b) => b.hash(state),
            Value::Int(i) => i.hash(state),
            Value::Float(f) => {
                if f.is_nan() {
                    // All NaN bit patterns hash identically
                    f64::NAN.to_bits().hash(state);
                } else if *f == 0.0 {
                    // Canonicalize +0.0 and -0.0 to match PartialEq
                    0.0f64.to_bits().hash(state);
                } else {
                    f.to_bits().hash(state);
                }
            }
            Value::Text(s) => s.hash(state),
            Value::Bytea(b) => b.hash(state),
            Value::Vector(v) => {
                v.len().hash(state);
                for f in v {
                    f.to_bits().hash(state);
                }
            }
        }
    }
}

impl Value {
    pub fn compare(&self, other: &Value) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Value::Null, _) | (_, Value::Null) => None,
            (Value::Int(a), Value::Int(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Int(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::Int(b)) => a.partial_cmp(&(*b as f64)),
            (Value::Text(a), Value::Text(b)) => a.partial_cmp(b),
            (Value::Bool(a), Value::Bool(b)) => a.partial_cmp(b),
            (Value::Vector(_), Value::Vector(_)) => None, // vectors are not orderable
            _ => None,
        }
    }

    pub fn to_text(&self) -> Option<String> {
        match self {
            Value::Null => None,
            Value::Bool(b) => Some(if *b { "t" } else { "f" }.to_string()),
            Value::Int(i) => Some(i.to_string()),
            Value::Float(f) => Some(format_float(*f)),
            Value::Text(s) => Some(s.to_string()),
            Value::Bytea(b) => Some(format!("\\x{}", hex_encode(b))),
            Value::Vector(v) => {
                let inner: Vec<String> = v.iter().map(|f| {
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
}

pub fn format_float(f: f64) -> String {
    if f.is_nan() {
        "NaN".to_string()
    } else if f == f64::INFINITY {
        "Infinity".to_string()
    } else if f == f64::NEG_INFINITY {
        "-Infinity".to_string()
    } else {
        f.to_string()
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Column type OIDs matching PostgreSQL.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum TypeOid {
    Bool = 16,
    Bytea = 17,
    Int8 = 20,
    Int2 = 21,
    Int4 = 23,
    Text = 25,
    Float4 = 700,
    Float8 = 701,
    Varchar = 1043,
    Numeric = 1700,
    Vector = 16385,
}

impl TypeOid {
    pub fn from_name(name: &str) -> Self {
        match name.to_lowercase().as_str() {
            "bool" | "boolean" => TypeOid::Bool,
            "int2" | "smallint" => TypeOid::Int2,
            "int4" | "integer" | "int" | "serial" => TypeOid::Int4,
            "int8" | "bigint" | "bigserial" => TypeOid::Int8,
            "float4" | "real" => TypeOid::Float4,
            "float8" | "double precision" => TypeOid::Float8,
            "numeric" | "decimal" => TypeOid::Numeric,
            "text" => TypeOid::Text,
            "varchar" | "character varying" => TypeOid::Varchar,
            "bytea" => TypeOid::Bytea,
            "vector" => TypeOid::Vector,
            _ => TypeOid::Text,
        }
    }

    pub fn oid(&self) -> i32 {
        *self as i32
    }

    pub fn from_oid(oid: i32) -> Self {
        match oid {
            16 => TypeOid::Bool,
            17 => TypeOid::Bytea,
            20 => TypeOid::Int8,
            21 => TypeOid::Int2,
            23 => TypeOid::Int4,
            25 => TypeOid::Text,
            700 => TypeOid::Float4,
            701 => TypeOid::Float8,
            1043 => TypeOid::Varchar,
            1700 => TypeOid::Numeric,
            16385 => TypeOid::Vector,
            _ => TypeOid::Text,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn float_infinity_wire_format() {
        assert_eq!(Value::Float(f64::INFINITY).to_text(), Some("Infinity".to_string()));
        assert_eq!(Value::Float(f64::NEG_INFINITY).to_text(), Some("-Infinity".to_string()));
        assert_eq!(Value::Float(f64::NAN).to_text(), Some("NaN".to_string()));
    }
}
