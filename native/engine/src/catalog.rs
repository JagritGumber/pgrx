use crate::types::TypeOid;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::LazyLock;

/// Global catalog — lightweight in-memory metadata store.
/// No MVCC, no heap storage, no vacuum. Just a RwLock'd HashMap.
static CATALOG: LazyLock<RwLock<Catalog>> = LazyLock::new(|| RwLock::new(Catalog::new()));

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub type_oid: TypeOid,
    pub nullable: bool,
    pub primary_key: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub schema: String,
    pub columns: Vec<Column>,
}

struct Catalog {
    tables: HashMap<String, Table>,
    next_oid: u32,
}

impl Catalog {
    fn new() -> Self {
        Self {
            tables: HashMap::new(),
            next_oid: 16384,
        }
    }
}

/// Fully qualified table name.
fn fqn(schema: &str, name: &str) -> String {
    format!("{}.{}", schema, name)
}

pub fn create_table(table: Table) -> Result<(), String> {
    let mut cat = CATALOG.write();
    let key = fqn(&table.schema, &table.name);
    if cat.tables.contains_key(&key) {
        return Err(format!("relation \"{}\" already exists", table.name));
    }
    cat.next_oid += 1;
    cat.tables.insert(key, table);
    Ok(())
}

pub fn drop_table(schema: &str, name: &str) -> Result<(), String> {
    let mut cat = CATALOG.write();
    let key = fqn(schema, name);
    if cat.tables.remove(&key).is_none() {
        return Err(format!("table \"{}\" does not exist", name));
    }
    Ok(())
}

pub fn get_table(schema: &str, name: &str) -> Option<Table> {
    let cat = CATALOG.read();
    cat.tables.get(&fqn(schema, name)).cloned()
}

pub fn table_exists(schema: &str, name: &str) -> bool {
    let cat = CATALOG.read();
    cat.tables.contains_key(&fqn(schema, name))
}

pub fn list_tables(schema: &str) -> Vec<Table> {
    let cat = CATALOG.read();
    let prefix = format!("{}.", schema);
    cat.tables
        .iter()
        .filter(|(k, _)| k.starts_with(&prefix))
        .map(|(_, t)| t.clone())
        .collect()
}

pub fn reset() {
    let mut cat = CATALOG.write();
    cat.tables.clear();
    cat.next_oid = 16384;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_table() -> Table {
        Table {
            name: "users".to_string(),
            schema: "public".to_string(),
            columns: vec![
                Column {
                    name: "id".to_string(),
                    type_oid: TypeOid::Int4,
                    nullable: false,
                    primary_key: true,
                },
                Column {
                    name: "name".to_string(),
                    type_oid: TypeOid::Text,
                    nullable: false,
                    primary_key: false,
                },
            ],
        }
    }

    #[test]
    #[serial_test::serial]
    fn create_and_get() {
        reset();
        create_table(test_table()).unwrap();
        let t = get_table("public", "users").unwrap();
        assert_eq!(t.columns.len(), 2);
    }

    #[test]
    #[serial_test::serial]
    fn duplicate_create_fails() {
        reset();
        create_table(test_table()).unwrap();
        assert!(create_table(test_table()).is_err());
    }

    #[test]
    #[serial_test::serial]
    fn drop_works() {
        reset();
        create_table(test_table()).unwrap();
        drop_table("public", "users").unwrap();
        assert!(!table_exists("public", "users"));
    }
}
