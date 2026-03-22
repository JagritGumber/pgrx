use crate::types::Value;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::LazyLock;

/// In-memory row storage. Each table maps to a Vec of rows.
/// This is the Phase 1 storage — will be replaced with a persistent
/// B-tree engine with undo-log MVCC and direct I/O in Phase 2.
static STORE: LazyLock<RwLock<Storage>> = LazyLock::new(|| RwLock::new(Storage::new()));

type Row = Vec<Value>;

struct TableStore {
    rows: Vec<Row>,
}

struct Storage {
    tables: HashMap<String, TableStore>,
}

impl Storage {
    fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }
}

fn key(schema: &str, name: &str) -> String {
    format!("{}.{}", schema, name)
}

pub fn create_table(schema: &str, name: &str) {
    let mut store = STORE.write();
    store
        .tables
        .insert(key(schema, name), TableStore { rows: Vec::new() });
}

pub fn drop_table(schema: &str, name: &str) {
    let mut store = STORE.write();
    store.tables.remove(&key(schema, name));
}

pub fn insert(schema: &str, name: &str, row: Row) -> Result<(), String> {
    let mut store = STORE.write();
    let table = store
        .tables
        .get_mut(&key(schema, name))
        .ok_or_else(|| format!("table \"{}.{}\" not found in storage", schema, name))?;
    table.rows.push(row);
    Ok(())
}

pub fn scan(schema: &str, name: &str) -> Result<Vec<Row>, String> {
    let store = STORE.read();
    let table = store
        .tables
        .get(&key(schema, name))
        .ok_or_else(|| format!("table \"{}.{}\" not found in storage", schema, name))?;
    Ok(table.rows.clone())
}

pub fn delete_all(schema: &str, name: &str) -> Result<u64, String> {
    let mut store = STORE.write();
    let table = store
        .tables
        .get_mut(&key(schema, name))
        .ok_or_else(|| format!("table \"{}.{}\" not found in storage", schema, name))?;
    let count = table.rows.len() as u64;
    table.rows.clear();
    Ok(count)
}

pub fn row_count(schema: &str, name: &str) -> Result<u64, String> {
    let store = STORE.read();
    let table = store
        .tables
        .get(&key(schema, name))
        .ok_or_else(|| format!("table \"{}.{}\" not found in storage", schema, name))?;
    Ok(table.rows.len() as u64)
}

pub fn reset() {
    let mut store = STORE.write();
    store.tables.clear();
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
