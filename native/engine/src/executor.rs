use pg_query::NodeEnum;
use serde::Serialize;

use crate::catalog::{self, Column, Table};
use crate::storage;
use crate::types::{TypeOid, Value};

#[derive(Debug, Serialize)]
pub struct QueryResult {
    pub tag: String,
    pub columns: Vec<(String, i32)>,
    pub rows: Vec<Vec<Option<String>>>,
}

pub fn execute(sql: &str) -> Result<QueryResult, String> {
    let parsed = pg_query::parse(sql).map_err(|e| e.to_string())?;

    let raw_stmt = parsed
        .protobuf
        .stmts
        .first()
        .ok_or("empty query")?;

    let stmt = raw_stmt
        .stmt
        .as_ref()
        .ok_or("missing statement")?;

    let node = stmt.node.as_ref().ok_or("missing node")?;

    match node {
        NodeEnum::CreateStmt(create) => exec_create_table(create),
        NodeEnum::DropStmt(drop) => exec_drop(drop),
        NodeEnum::InsertStmt(insert) => exec_insert(insert),
        NodeEnum::SelectStmt(select) => exec_select(select, sql),
        NodeEnum::DeleteStmt(delete) => exec_delete(delete),
        NodeEnum::TruncateStmt(trunc) => exec_truncate(trunc),
        NodeEnum::VariableSetStmt(_) => Ok(QueryResult {
            tag: "SET".into(),
            columns: vec![],
            rows: vec![],
        }),
        NodeEnum::VariableShowStmt(_) => Ok(QueryResult {
            tag: "SHOW".into(),
            columns: vec![],
            rows: vec![],
        }),
        NodeEnum::TransactionStmt(_) => Ok(QueryResult {
            tag: "OK".into(),
            columns: vec![],
            rows: vec![],
        }),
        _ => Err(format!("unsupported statement type")),
    }
}

fn exec_create_table(
    create: &pg_query::protobuf::CreateStmt,
) -> Result<QueryResult, String> {
    let rel = create
        .relation
        .as_ref()
        .ok_or("CREATE TABLE missing relation")?;
    let table_name = &rel.relname;
    let schema = if rel.schemaname.is_empty() {
        "public"
    } else {
        &rel.schemaname
    };

    let mut columns = Vec::new();
    for elt in &create.table_elts {
        let node = elt.node.as_ref().ok_or("missing table element")?;
        if let NodeEnum::ColumnDef(col) = node {
            let type_name = extract_type_name(col);
            let nullable = !col.is_not_null;
            columns.push(Column {
                name: col.colname.clone(),
                type_oid: TypeOid::from_name(&type_name),
                nullable,
                primary_key: false,
            });
        }
    }

    let table = Table {
        name: table_name.clone(),
        schema: schema.to_string(),
        columns,
    };

    catalog::create_table(table)?;
    storage::create_table(schema, table_name);

    Ok(QueryResult {
        tag: "CREATE TABLE".into(),
        columns: vec![],
        rows: vec![],
    })
}

fn extract_type_name(col: &pg_query::protobuf::ColumnDef) -> String {
    col.type_name
        .as_ref()
        .map(|tn| {
            tn.names
                .iter()
                .filter_map(|n| n.node.as_ref())
                .filter_map(|node| {
                    if let NodeEnum::String(s) = node {
                        Some(s.sval.clone())
                    } else {
                        None
                    }
                })
                .last()
                .unwrap_or_else(|| "text".into())
        })
        .unwrap_or_else(|| "text".into())
}

fn exec_drop(drop: &pg_query::protobuf::DropStmt) -> Result<QueryResult, String> {
    for obj in &drop.objects {
        if let Some(NodeEnum::List(list)) = obj.node.as_ref() {
            let parts: Vec<String> = list
                .items
                .iter()
                .filter_map(|i| i.node.as_ref())
                .filter_map(|n| {
                    if let NodeEnum::String(s) = n {
                        Some(s.sval.clone())
                    } else {
                        None
                    }
                })
                .collect();

            let (schema, name) = if parts.len() >= 2 {
                (parts[0].as_str(), parts[1].as_str())
            } else if parts.len() == 1 {
                ("public", parts[0].as_str())
            } else {
                continue;
            };

            catalog::drop_table(schema, name)?;
            storage::drop_table(schema, name);
        }
    }

    Ok(QueryResult {
        tag: "DROP TABLE".into(),
        columns: vec![],
        rows: vec![],
    })
}

fn exec_insert(
    insert: &pg_query::protobuf::InsertStmt,
) -> Result<QueryResult, String> {
    let rel = insert
        .relation
        .as_ref()
        .ok_or("INSERT missing relation")?;
    let table_name = &rel.relname;
    let schema = if rel.schemaname.is_empty() {
        "public"
    } else {
        &rel.schemaname
    };

    let table_def = catalog::get_table(schema, table_name)
        .ok_or_else(|| format!("relation \"{}\" does not exist", table_name))?;

    // Get target column names (or all columns if not specified)
    let target_cols: Vec<String> = if insert.cols.is_empty() {
        table_def.columns.iter().map(|c| c.name.clone()).collect()
    } else {
        insert
            .cols
            .iter()
            .filter_map(|n| n.node.as_ref())
            .filter_map(|node| {
                if let NodeEnum::ResTarget(rt) = node {
                    Some(rt.name.clone())
                } else {
                    None
                }
            })
            .collect()
    };

    // Extract VALUES rows from the select_stmt
    let select = insert
        .select_stmt
        .as_ref()
        .and_then(|s| s.node.as_ref())
        .ok_or("INSERT missing VALUES")?;

    let mut row_count = 0u64;

    if let NodeEnum::SelectStmt(sel) = select {
        let values_lists = &sel.values_lists;
        for values_list in values_lists {
            if let Some(NodeEnum::List(list)) = values_list.node.as_ref() {
                let mut row = vec![Value::Null; table_def.columns.len()];

                for (i, val_node) in list.items.iter().enumerate() {
                    if i >= target_cols.len() {
                        break;
                    }
                    let col_name = &target_cols[i];
                    let col_idx = table_def
                        .columns
                        .iter()
                        .position(|c| &c.name == col_name)
                        .ok_or_else(|| {
                            format!("column \"{}\" does not exist", col_name)
                        })?;

                    row[col_idx] = eval_const(val_node.node.as_ref());
                }
                storage::insert(schema, table_name, row)?;
                row_count += 1;
            }
        }
    }

    Ok(QueryResult {
        tag: format!("INSERT 0 {}", row_count),
        columns: vec![],
        rows: vec![],
    })
}

fn eval_const(node: Option<&NodeEnum>) -> Value {
    match node {
        Some(NodeEnum::Integer(i)) => Value::Int(i.ival as i64),
        Some(NodeEnum::Float(f)) => {
            f.fval.parse::<f64>().map(Value::Float).unwrap_or(Value::Null)
        }
        Some(NodeEnum::String(s)) => Value::Text(s.sval.clone()),
        Some(NodeEnum::AConst(ac)) => {
            if let Some(val) = &ac.val {
                match val {
                    pg_query::protobuf::a_const::Val::Ival(i) => Value::Int(i.ival as i64),
                    pg_query::protobuf::a_const::Val::Fval(f) => f
                        .fval
                        .parse::<f64>()
                        .map(Value::Float)
                        .unwrap_or(Value::Null),
                    pg_query::protobuf::a_const::Val::Sval(s) => {
                        Value::Text(s.sval.clone())
                    }
                    pg_query::protobuf::a_const::Val::Bsval(s) => {
                        Value::Text(s.bsval.clone())
                    }
                    pg_query::protobuf::a_const::Val::Boolval(b) => {
                        Value::Bool(b.boolval)
                    }
                }
            } else if ac.isnull {
                Value::Null
            } else {
                Value::Null
            }
        }
        Some(NodeEnum::TypeCast(tc)) => {
            eval_const(tc.arg.as_ref().and_then(|a| a.node.as_ref()))
        }
        _ => Value::Null,
    }
}

fn exec_select(
    select: &pg_query::protobuf::SelectStmt,
    _raw_sql: &str,
) -> Result<QueryResult, String> {
    // Check for VALUES-less SELECT (e.g., SELECT 1, SELECT version())
    if select.from_clause.is_empty() {
        return exec_select_no_from(select);
    }

    // Get table from FROM clause
    let from = select.from_clause.first().ok_or("missing FROM")?;
    let (schema, table_name) = extract_from_table(from.node.as_ref())?;

    let table_def = catalog::get_table(&schema, &table_name)
        .ok_or_else(|| format!("relation \"{}\" does not exist", table_name))?;

    let rows = storage::scan(&schema, &table_name)?;

    // Resolve target columns
    let (col_names, col_indices) = resolve_select_targets(select, &table_def)?;
    let columns: Vec<(String, i32)> = col_names
        .iter()
        .zip(col_indices.iter())
        .map(|(name, &idx)| (name.clone(), table_def.columns[idx].type_oid.oid()))
        .collect();

    // Project rows
    let result_rows: Vec<Vec<Option<String>>> = rows
        .iter()
        .map(|row| {
            col_indices
                .iter()
                .map(|&idx| row.get(idx).and_then(|v| v.to_text()))
                .collect()
        })
        .collect();

    let count = result_rows.len();
    Ok(QueryResult {
        tag: format!("SELECT {}", count),
        columns,
        rows: result_rows,
    })
}

fn exec_select_no_from(
    select: &pg_query::protobuf::SelectStmt,
) -> Result<QueryResult, String> {
    let mut columns = Vec::new();
    let mut row = Vec::new();

    for target in &select.target_list {
        if let Some(NodeEnum::ResTarget(rt)) = target.node.as_ref() {
            let alias = if rt.name.is_empty() {
                "?column?".to_string()
            } else {
                rt.name.clone()
            };
            let val = eval_const(rt.val.as_ref().and_then(|v| v.node.as_ref()));
            columns.push((alias, TypeOid::Text.oid()));
            row.push(val.to_text());
        }
    }

    Ok(QueryResult {
        tag: "SELECT 1".into(),
        columns,
        rows: vec![row],
    })
}

fn extract_from_table(node: Option<&NodeEnum>) -> Result<(String, String), String> {
    match node {
        Some(NodeEnum::RangeVar(rv)) => {
            let schema = if rv.schemaname.is_empty() {
                "public".to_string()
            } else {
                rv.schemaname.clone()
            };
            Ok((schema, rv.relname.clone()))
        }
        _ => Err("unsupported FROM clause".into()),
    }
}

fn resolve_select_targets(
    select: &pg_query::protobuf::SelectStmt,
    table: &Table,
) -> Result<(Vec<String>, Vec<usize>), String> {
    let mut names = Vec::new();
    let mut indices = Vec::new();

    for target in &select.target_list {
        if let Some(NodeEnum::ResTarget(rt)) = target.node.as_ref() {
            match rt.val.as_ref().and_then(|v| v.node.as_ref()) {
                Some(NodeEnum::ColumnRef(cref)) => {
                    let col_name = cref
                        .fields
                        .iter()
                        .filter_map(|f| f.node.as_ref())
                        .filter_map(|n| match n {
                            NodeEnum::String(s) => Some(s.sval.clone()),
                            NodeEnum::AStar(_) => Some("*".to_string()),
                            _ => None,
                        })
                        .last()
                        .unwrap_or_default();

                    if col_name == "*" {
                        for (i, col) in table.columns.iter().enumerate() {
                            names.push(col.name.clone());
                            indices.push(i);
                        }
                    } else {
                        let idx = table
                            .columns
                            .iter()
                            .position(|c| c.name == col_name)
                            .ok_or_else(|| {
                                format!("column \"{}\" does not exist", col_name)
                            })?;
                        let alias = if rt.name.is_empty() {
                            col_name
                        } else {
                            rt.name.clone()
                        };
                        names.push(alias);
                        indices.push(idx);
                    }
                }
                _ => {
                    names.push("?column?".to_string());
                    indices.push(0);
                }
            }
        }
    }

    Ok((names, indices))
}

fn exec_delete(
    delete: &pg_query::protobuf::DeleteStmt,
) -> Result<QueryResult, String> {
    let rel = delete
        .relation
        .as_ref()
        .ok_or("DELETE missing relation")?;
    let table_name = &rel.relname;
    let schema = if rel.schemaname.is_empty() {
        "public"
    } else {
        &rel.schemaname
    };

    // For now: DELETE without WHERE deletes all rows
    if delete.where_clause.is_some() {
        return Err("DELETE with WHERE not yet implemented".into());
    }

    let count = storage::delete_all(schema, table_name)?;
    Ok(QueryResult {
        tag: format!("DELETE {}", count),
        columns: vec![],
        rows: vec![],
    })
}

fn exec_truncate(
    trunc: &pg_query::protobuf::TruncateStmt,
) -> Result<QueryResult, String> {
    for rel_node in &trunc.relations {
        if let Some(NodeEnum::RangeVar(rv)) = rel_node.node.as_ref() {
            let schema = if rv.schemaname.is_empty() {
                "public"
            } else {
                &rv.schemaname
            };
            storage::delete_all(schema, &rv.relname)?;
        }
    }
    Ok(QueryResult {
        tag: "TRUNCATE TABLE".into(),
        columns: vec![],
        rows: vec![],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() {
        catalog::reset();
        storage::reset();
    }

    #[test]
    #[serial_test::serial]
    fn create_and_insert_and_select() {
        setup();
        execute("CREATE TABLE users (id integer, name text)").unwrap();
        execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
        execute("INSERT INTO users VALUES (2, 'bob')").unwrap();

        let result = execute("SELECT * FROM users").unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.rows[0][0], Some("1".into()));
        assert_eq!(result.rows[0][1], Some("alice".into()));
        assert_eq!(result.rows[1][1], Some("bob".into()));
    }

    #[test]
    #[serial_test::serial]
    fn select_specific_columns() {
        setup();
        execute("CREATE TABLE t (a int, b text, c int)").unwrap();
        execute("INSERT INTO t VALUES (1, 'x', 10)").unwrap();

        let result = execute("SELECT b, c FROM t").unwrap();
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.columns[0].0, "b");
        assert_eq!(result.rows[0][0], Some("x".into()));
        assert_eq!(result.rows[0][1], Some("10".into()));
    }

    #[test]
    #[serial_test::serial]
    fn select_no_from() {
        setup();
        let result = execute("SELECT 42").unwrap();
        assert_eq!(result.rows[0][0], Some("42".into()));
    }

    #[test]
    #[serial_test::serial]
    fn drop_table() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        execute("DROP TABLE t").unwrap();
        assert!(execute("SELECT * FROM t").is_err());
    }

    #[test]
    #[serial_test::serial]
    fn insert_into_nonexistent() {
        setup();
        assert!(execute("INSERT INTO ghost VALUES (1)").is_err());
    }

    #[test]
    #[serial_test::serial]
    fn truncate() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        execute("INSERT INTO t VALUES (1)").unwrap();
        execute("INSERT INTO t VALUES (2)").unwrap();
        execute("TRUNCATE t").unwrap();
        let result = execute("SELECT * FROM t").unwrap();
        assert_eq!(result.rows.len(), 0);
    }
}
