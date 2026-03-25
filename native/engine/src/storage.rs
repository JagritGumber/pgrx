use crate::types::Value;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

/// In-memory row storage. Each table maps to a Vec of rows.
/// This is the Phase 1 storage — will be replaced with a persistent
/// B-tree engine with undo-log MVCC and direct I/O in Phase 2.
///
/// Per-table RwLock: the outer HashMap RwLock is only held during
/// table creation/deletion. Individual table locks allow concurrent
/// reads on different tables and concurrent same-table reads via
/// parking_lot's reader-writer fairness.
static STORE: LazyLock<RwLock<HashMap<String, Arc<RwLock<TableStore>>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

type Row = Vec<Value>;

struct TableStore {
    rows: Vec<Row>,
}

fn key(schema: &str, name: &str) -> String {
    format!("{}.{}", schema, name)
}

/// Fast table lookup: acquires outer read lock briefly, clones the Arc, releases.
/// The caller then works with the per-table lock independently.
fn get_table(schema: &str, name: &str) -> Result<Arc<RwLock<TableStore>>, String> {
    let store = STORE.read();
    store
        .get(&key(schema, name))
        .cloned()
        .ok_or_else(|| format!("table \"{}.{}\" not found in storage", schema, name))
}

pub fn create_table(schema: &str, name: &str) {
    let mut store = STORE.write();
    store.insert(
        key(schema, name),
        Arc::new(RwLock::new(TableStore { rows: Vec::new() })),
    );
}

pub fn drop_table(schema: &str, name: &str) {
    let mut store = STORE.write();
    store.remove(&key(schema, name));
}

pub fn insert(schema: &str, name: &str, row: Row) -> Result<(), String> {
    let tbl = get_table(schema, name)?;
    let mut table = tbl.write();
    table.rows.push(row);
    Ok(())
}

pub fn scan(schema: &str, name: &str) -> Result<Vec<Row>, String> {
    let tbl = get_table(schema, name)?;
    let table = tbl.read();
    Ok(table.rows.clone())
}

/// Zero-copy scan: runs callback with borrowed rows, avoids cloning.
pub fn scan_with<F, R>(schema: &str, name: &str, f: F) -> Result<R, String>
where
    F: FnOnce(&[Row]) -> Result<R, String>,
{
    let tbl = get_table(schema, name)?;
    let table = tbl.read();
    f(&table.rows)
}

pub fn delete_all(schema: &str, name: &str) -> Result<u64, String> {
    let tbl = get_table(schema, name)?;
    let mut table = tbl.write();
    let count = table.rows.len() as u64;
    table.rows.clear();
    Ok(count)
}

pub fn delete_where(
    schema: &str,
    name: &str,
    predicate: impl Fn(&Row) -> bool,
) -> Result<u64, String> {
    let tbl = get_table(schema, name)?;
    let mut table = tbl.write();
    let before = table.rows.len();
    table.rows.retain(|row| !predicate(row));
    Ok((before - table.rows.len()) as u64)
}

/// Delete matching rows and return the deleted rows (for RETURNING clause).
pub fn delete_where_returning(
    schema: &str,
    name: &str,
    predicate: impl Fn(&Row) -> bool,
) -> Result<Vec<Row>, String> {
    let tbl = get_table(schema, name)?;
    let mut table = tbl.write();
    let mut deleted = Vec::new();
    let mut kept = Vec::new();
    for row in table.rows.drain(..) {
        if predicate(&row) {
            deleted.push(row);
        } else {
            kept.push(row);
        }
    }
    table.rows = kept;
    Ok(deleted)
}

pub fn update_rows(
    schema: &str,
    name: &str,
    predicate: impl Fn(&Row) -> bool,
    updater: impl Fn(&mut Row),
) -> Result<u64, String> {
    let tbl = get_table(schema, name)?;
    let mut table = tbl.write();
    let mut count = 0u64;
    for row in table.rows.iter_mut() {
        if predicate(row) {
            updater(row);
            count += 1;
        }
    }
    Ok(count)
}

/// Insert with uniqueness check under a single write lock (no TOCTOU race).
/// `unique_checks` is a list of (column_index, constraint_name) pairs.
/// `pk_cols` is a list of column indices forming the composite primary key (if any).
pub fn insert_checked(
    schema: &str,
    name: &str,
    row: Row,
    unique_checks: &[(usize, String)],
    pk_cols: &[usize],
) -> Result<(), String> {
    let tbl = get_table(schema, name)?;
    let mut table = tbl.write();

    // Composite PK check
    if pk_cols.len() > 1 {
        let new_key: Vec<&Value> = pk_cols.iter().map(|&i| &row[i]).collect();
        for erow in &table.rows {
            let ekey: Vec<&Value> = pk_cols.iter().map(|&i| &erow[i]).collect();
            if new_key == ekey {
                return Err(format!(
                    "duplicate key value violates unique constraint \"{}.{}_pkey\"",
                    schema, name
                ));
            }
        }
    }

    // Per-column unique checks
    for &(col_idx, ref cname) in unique_checks {
        if matches!(row[col_idx], Value::Null) {
            continue; // NULLs don't violate UNIQUE
        }
        for erow in &table.rows {
            if col_idx < erow.len() && erow[col_idx] == row[col_idx] {
                return Err(format!(
                    "duplicate key value violates unique constraint \"{}\"",
                    cname
                ));
            }
        }
    }

    table.rows.push(row);
    Ok(())
}

/// Update matching rows with validation. Returns error if any updater fails.
pub fn update_rows_checked(
    schema: &str,
    name: &str,
    predicate: impl Fn(&Row) -> bool,
    updater: impl FnMut(&Row) -> Result<Row, String>,
    validator: impl Fn(&Row, &[Row], usize) -> Result<(), String>,
) -> Result<u64, String> {
    let tbl = get_table(schema, name)?;
    let mut table = tbl.write();

    // First pass: compute new rows and validate
    let mut updates: Vec<(usize, Row)> = Vec::new();
    let mut updater = updater;
    for (idx, row) in table.rows.iter().enumerate() {
        if predicate(row) {
            let new_row = updater(row)?;
            // Validate against all OTHER rows (excluding current)
            validator(&new_row, &table.rows, idx)?;
            updates.push((idx, new_row));
        }
    }

    // Second pass: apply
    let count = updates.len() as u64;
    for (idx, new_row) in updates {
        table.rows[idx] = new_row;
    }
    Ok(count)
}

/// Delete all rows and return them (for DELETE ... RETURNING without WHERE).
/// Single write lock — no TOCTOU race.
pub fn delete_all_returning(schema: &str, name: &str) -> Result<Vec<Row>, String> {
    let tbl = get_table(schema, name)?;
    let mut table = tbl.write();
    Ok(std::mem::take(&mut table.rows))
}

pub fn row_count(schema: &str, name: &str) -> Result<u64, String> {
    let tbl = get_table(schema, name)?;
    let table = tbl.read();
    Ok(table.rows.len() as u64)
}

pub fn reset() {
    let mut store = STORE.write();
    store.clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[serial_test::serial]
    fn insert_and_scan() {
        reset();
        create_table("public", "t");
        insert(
            "public",
            "t",
            vec![Value::Int(1), Value::Text("hello".into())],
        )
        .unwrap();
        insert(
            "public",
            "t",
            vec![Value::Int(2), Value::Text("world".into())],
        )
        .unwrap();
        let rows = scan("public", "t").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Int(1));
    }

    #[test]
    #[serial_test::serial]
    fn delete_all_works() {
        reset();
        create_table("public", "t");
        insert("public", "t", vec![Value::Int(1)]).unwrap();
        insert("public", "t", vec![Value::Int(2)]).unwrap();
        let count = delete_all("public", "t").unwrap();
        assert_eq!(count, 2);
        assert_eq!(scan("public", "t").unwrap().len(), 0);
    }
}
