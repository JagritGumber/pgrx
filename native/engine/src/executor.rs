// TODO(#5): Describe Portal sends NoData — requires parsing SQL to determine
//   column types without execution. Needs a type-inference pass over the AST
//   to return RowDescription for prepared statements. Phase 2 work.
//
// TODO(#8): O(N) uniqueness check on INSERT/UPDATE — needs hash indexes on
//   unique columns for O(1) constraint validation. Phase 2 work (index engine).

use std::collections::HashMap;
use std::hash::{DefaultHasher, Hasher};
use std::sync::{Arc, LazyLock};

use parking_lot::RwLock;
use pg_query::NodeEnum;
use serde::Serialize;

use crate::arena::{ArenaValue, QueryArena, rows_to_arena};
use crate::catalog::{self, Column, Table};
use crate::storage;
use crate::types::{TypeOid, Value};



/// Parse cache: concurrent reads via RwLock, write only on cache miss.
/// Bounded to MAX_PARSE_CACHE entries — clears on overflow to avoid unbounded heap growth.
const MAX_PARSE_CACHE: usize = 1024;
static PARSE_CACHE: LazyLock<RwLock<HashMap<String, pg_query::protobuf::ParseResult>>> =
    LazyLock::new(|| RwLock::new(HashMap::with_capacity(MAX_PARSE_CACHE)));

#[derive(Debug, Serialize)]
pub struct QueryResult {
    pub tag: String,
    pub columns: Vec<(String, i32)>,
    pub rows: Vec<Vec<Option<String>>>,
}

// ── JoinContext: multi-table column resolution ───────────────────────

/// A table source in a multi-table join context.
struct JoinSource {
    alias: String,
    table_name: String,
    #[allow(dead_code)] // needed for future cross-schema joins
    schema: String,
    table_def: Table,
    col_offset: usize,
}

/// Context for column resolution across potentially multiple tables.
struct JoinContext {
    sources: Vec<JoinSource>,
    total_columns: usize,
}

impl JoinContext {
    /// Build a single-table context (backward compat for DELETE/UPDATE).
    fn single(schema: &str, table_name: &str, table_def: Table) -> Self {
        let ncols = table_def.columns.len();
        JoinContext {
            sources: vec![JoinSource {
                alias: table_name.to_string(),
                table_name: table_name.to_string(),
                schema: schema.to_string(),
                table_def,
                col_offset: 0,
            }],
            total_columns: ncols,
        }
    }
}

/// Extract string fields from a ColumnRef.
fn extract_string_fields(cref: &pg_query::protobuf::ColumnRef) -> Vec<String> {
    cref.fields
        .iter()
        .filter_map(|f| f.node.as_ref())
        .filter_map(|n| {
            if let NodeEnum::String(s) = n {
                Some(s.sval.clone())
            } else {
                None
            }
        })
        .collect()
}

/// Resolve a column reference to an index into the joined row.
fn resolve_column(
    cref: &pg_query::protobuf::ColumnRef,
    ctx: &JoinContext,
) -> Result<usize, String> {
    // Fast path: single-source, single-field (most common case)
    // Avoids Vec allocation from extract_string_fields entirely
    if ctx.sources.len() == 1 {
        if let Some(last_field) = cref.fields.last().and_then(|f| f.node.as_ref()) {
            if let NodeEnum::String(s) = last_field {
                let src = &ctx.sources[0];
                if let Some(pos) = src.table_def.columns.iter().position(|c| c.name == s.sval) {
                    return Ok(src.col_offset + pos);
                }
                // For 2-field qualified refs on single source, also fast-path
                if cref.fields.len() == 2 {
                    return Err(format!("column \"{}\" does not exist", s.sval));
                }
            }
        }
    }

    // General path for multi-source (JOINs) and edge cases
    let fields = extract_string_fields(cref);

    match fields.len() {
        1 => {
            let col_name = &fields[0];
            let mut found = Vec::new();
            for src in &ctx.sources {
                if let Some(pos) = src.table_def.columns.iter().position(|c| c.name == *col_name) {
                    found.push(src.col_offset + pos);
                }
            }
            match found.len() {
                0 => Err(format!("column \"{}\" does not exist", col_name)),
                1 => Ok(found[0]),
                _ => Err(format!(
                    "column reference \"{}\" is ambiguous",
                    col_name
                )),
            }
        }
        2 => {
            // Qualified: qualifier.column
            let (qualifier, col_name) = (&fields[0], &fields[1]);
            // First try exact alias match, then table_name match with ambiguity check
            let matches: Vec<_> = ctx.sources.iter()
                .filter(|s| s.alias == *qualifier)
                .collect();
            let src = if matches.len() == 1 {
                matches[0]
            } else if matches.is_empty() {
                // Fall back to table_name match
                let by_name: Vec<_> = ctx.sources.iter()
                    .filter(|s| s.table_name == *qualifier)
                    .collect();
                match by_name.len() {
                    0 => return Err(format!(
                        "missing FROM-clause entry for table \"{}\"", qualifier
                    )),
                    1 => by_name[0],
                    _ => return Err(format!(
                        "table reference \"{}\" is ambiguous", qualifier
                    )),
                }
            } else {
                return Err(format!(
                    "table reference \"{}\" is ambiguous", qualifier
                ));
            };
            let pos = src
                .table_def
                .columns
                .iter()
                .position(|c| c.name == *col_name)
                .ok_or_else(|| {
                    format!("column \"{}.{}\" does not exist", qualifier, col_name)
                })?;
            Ok(src.col_offset + pos)
        }
        _ => Err("unsupported column reference".into()),
    }
}

/// Get the type OID for a column index in a JoinContext.
fn column_type_oid(idx: usize, ctx: &JoinContext) -> Result<i32, String> {
    for src in &ctx.sources {
        let end = src.col_offset + src.table_def.columns.len();
        if idx >= src.col_offset && idx < end {
            return Ok(src.table_def.columns[idx - src.col_offset].type_oid.oid());
        }
    }
    // Fallback for expression columns or empty context
    if ctx.sources.is_empty() {
        Ok(TypeOid::Text.oid())
    } else {
        Err(format!(
            "internal error: column index {} not found in any source (total: {})",
            idx, ctx.total_columns
        ))
    }
}

pub fn execute(sql: &str) -> Result<QueryResult, String> {
    let protobuf = {
        let cache = PARSE_CACHE.read();
        if let Some(cached) = cache.get(sql) {
            cached.clone()
        } else {
            drop(cache);
            let parsed = pg_query::parse(sql).map_err(|e| e.to_string())?;
            let proto = parsed.protobuf;
            let mut cache = PARSE_CACHE.write();
            if cache.len() >= MAX_PARSE_CACHE {
                cache.clear();
            }
            cache.insert(sql.to_string(), proto.clone());
            proto
        }
    };

    let raw_stmt = protobuf
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
        NodeEnum::SelectStmt(select) => exec_select(select),
        NodeEnum::DeleteStmt(delete) => exec_delete(delete),
        NodeEnum::UpdateStmt(update) => exec_update(update),
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
        NodeEnum::AlterTableStmt(alter) => exec_alter_table(alter),
        NodeEnum::RenameStmt(rename) => exec_rename(rename),
        NodeEnum::CreateTableAsStmt(ctas) => exec_create_table_as(ctas),
        _ => Err("unsupported statement type".into()),
    }
}

// ── CREATE TABLE ──────────────────────────────────────────────────────

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
    let mut created_sequences: Vec<(String, String)> = Vec::new();
    for elt in &create.table_elts {
        let node = elt.node.as_ref().ok_or_else(|| {
            for (s, n) in &created_sequences { crate::sequence::drop_sequence(s, n); }
            "missing table element".to_string()
        })?;
        if let NodeEnum::ColumnDef(col) = node {
            let type_name = extract_type_name(col);
            let mut nullable = !col.is_not_null;
            let mut primary_key = false;
            let mut unique = false;
            let mut default_expr = None;

            // Detect SERIAL/BIGSERIAL — create sequence and set default
            let is_serial = matches!(
                type_name.to_lowercase().as_str(),
                "serial" | "bigserial"
            );
            if is_serial {
                let seq_name = format!("{}_{}_seq", table_name, col.colname);
                crate::sequence::create_sequence(schema, &seq_name, 1, 1).map_err(|e| {
                    for (s, n) in &created_sequences { crate::sequence::drop_sequence(s, n); }
                    e
                })?;
                created_sequences.push((schema.to_string(), seq_name.clone()));
                default_expr = Some(catalog::DefaultExpr::NextVal(
                    format!("{}.{}", schema, seq_name),
                ));
                nullable = false; // SERIAL implies NOT NULL
            }

            // Parse inline constraints
            for cnode in &col.constraints {
                if let Some(NodeEnum::Constraint(c)) = cnode.node.as_ref() {
                    match c.contype {
                        x if x == pg_query::protobuf::ConstrType::ConstrPrimary as i32 => {
                            primary_key = true;
                            nullable = false;
                            unique = true;
                        }
                        x if x == pg_query::protobuf::ConstrType::ConstrUnique as i32 => {
                            unique = true;
                        }
                        x if x == pg_query::protobuf::ConstrType::ConstrNotnull as i32 => {
                            nullable = false;
                        }
                        x if x == pg_query::protobuf::ConstrType::ConstrDefault as i32 => {
                            // Parse DEFAULT expression
                            if let Some(raw) = c.raw_expr.as_ref().and_then(|n| n.node.as_ref()) {
                                let func_node = match raw {
                                    NodeEnum::FuncCall(_) => Some(raw),
                                    NodeEnum::TypeCast(tc) => {
                                        tc.arg.as_ref()
                                            .and_then(|a| a.node.as_ref())
                                            .filter(|n| matches!(n, NodeEnum::FuncCall(_)))
                                    }
                                    _ => None,
                                };
                                if let Some(NodeEnum::FuncCall(fc)) = func_node {
                                    let func_name: String = fc.funcname.iter()
                                        .filter_map(|n| n.node.as_ref())
                                        .filter_map(|n| if let NodeEnum::String(s) = n { Some(s.sval.as_str()) } else { None })
                                        .collect::<Vec<_>>().join(".");
                                    for (s, n) in &created_sequences { crate::sequence::drop_sequence(s, n); }
                                    return Err(format!(
                                        "DEFAULT function expressions are not yet supported: {}",
                                        func_name
                                    ));
                                }
                                default_expr = Some(catalog::DefaultExpr::Literal(
                                    eval_const(Some(raw)),
                                ));
                            }
                        }
                        _ => {}
                    }
                }
            }

            columns.push(Column {
                name: col.colname.clone(),
                type_oid: TypeOid::from_name(&type_name),
                nullable,
                primary_key,
                unique,
                default_expr,
            });
        }
    }

    // Handle table-level constraints (e.g., PRIMARY KEY (a, b))
    for elt in &create.table_elts {
        if let Some(NodeEnum::Constraint(c)) = elt.node.as_ref() {
            let key_cols: Vec<String> = c
                .keys
                .iter()
                .filter_map(|k| k.node.as_ref())
                .filter_map(|n| {
                    if let NodeEnum::String(s) = n {
                        Some(s.sval.clone())
                    } else {
                        None
                    }
                })
                .collect();

            match c.contype {
                x if x == pg_query::protobuf::ConstrType::ConstrPrimary as i32 => {
                    for col in &mut columns {
                        if key_cols.contains(&col.name) {
                            col.primary_key = true;
                            col.nullable = false;
                            col.unique = true;
                        }
                    }
                }
                x if x == pg_query::protobuf::ConstrType::ConstrUnique as i32 => {
                    for col in &mut columns {
                        if key_cols.contains(&col.name) {
                            col.unique = true;
                        }
                    }
                }
                _ => {}
            }
        }
    }

    let table = Table {
        name: table_name.clone(),
        schema: schema.to_string(),
        columns,
    };

    // IF NOT EXISTS — skip if table already exists
    if create.if_not_exists && catalog::get_table(schema, table_name).is_some() {
        return Ok(QueryResult {
            tag: "CREATE TABLE".into(),
            columns: vec![],
            rows: vec![],
        });
    }

    catalog::create_table(table.clone())?;
    storage::create_table(schema, table_name);

    // Wire up hash indexes for PK/UNIQUE columns — O(1) constraint checks
    let pk_cols: Vec<usize> = table
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.primary_key)
        .map(|(i, _)| i)
        .collect();

    for (i, col) in table.columns.iter().enumerate() {
        // For composite PK, individual columns get their own unique index
        // only if they also have a standalone UNIQUE constraint.
        // Single-column PK always gets an index.
        if col.primary_key && pk_cols.len() == 1 {
            storage::add_unique_index(schema, table_name, i)?;
        } else if col.unique {
            storage::add_unique_index(schema, table_name, i)?;
        }
    }

    if pk_cols.len() > 1 {
        storage::add_pk_index(schema, table_name, &pk_cols)?;
    }

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

// ── DROP ──────────────────────────────────────────────────────────────

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

            if drop.missing_ok && catalog::get_table(schema, name).is_none() {
                continue; // IF EXISTS — skip silently
            }
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

// ── ALTER TABLE ──────────────────────────────────────────────────────

fn exec_alter_table(
    alter: &pg_query::protobuf::AlterTableStmt,
) -> Result<QueryResult, String> {
    let rel = alter.relation.as_ref().ok_or("ALTER TABLE missing relation")?;
    let table_name = &rel.relname;
    let schema = if rel.schemaname.is_empty() { "public" } else { &rel.schemaname };

    for cmd_node in &alter.cmds {
        let cmd = match cmd_node.node.as_ref() {
            Some(NodeEnum::AlterTableCmd(c)) => c,
            _ => continue,
        };

        use pg_query::protobuf::AlterTableType;

        if cmd.subtype == AlterTableType::AtAddColumn as i32 {
            // ADD COLUMN
            let col_def = match cmd.def.as_ref().and_then(|n| n.node.as_ref()) {
                Some(NodeEnum::ColumnDef(cd)) => cd,
                _ => return Err("ALTER TABLE ADD COLUMN missing column definition".into()),
            };
            let type_name = extract_type_name(col_def);
            let type_oid = TypeOid::from_name(&type_name);

            // Parse default value from constraints (same as CREATE TABLE)
            let mut default_expr = None;
            for cnode in &col_def.constraints {
                if let Some(NodeEnum::Constraint(c)) = cnode.node.as_ref() {
                    if c.contype == pg_query::protobuf::ConstrType::ConstrDefault as i32 {
                        if let Some(raw) = c.raw_expr.as_ref().and_then(|n| n.node.as_ref()) {
                            default_expr = Some(catalog::DefaultExpr::Literal(eval_const(Some(raw))));
                        }
                    }
                }
            }

            let col = catalog::Column {
                name: col_def.colname.clone(),
                type_oid,
                nullable: !col_def.is_not_null,
                primary_key: false,
                unique: false,
                default_expr: default_expr.clone(),
            };
            catalog::alter_table_add_column(schema, table_name, col)?;
            // Backfill existing rows with default or NULL
            let default_val = match &default_expr {
                Some(catalog::DefaultExpr::Literal(v)) => v.clone(),
                _ => crate::types::Value::Null,
            };
            storage::alter_add_column(schema, table_name, default_val);
        } else if cmd.subtype == AlterTableType::AtDropColumn as i32 {
            // DROP COLUMN
            let col_name = &cmd.name;
            let col_idx = catalog::get_column_index(schema, table_name, col_name)?;
            catalog::alter_table_drop_column(schema, table_name, col_name)?;
            storage::alter_drop_column(schema, table_name, col_idx);
        } else {
            return Err(format!("unsupported ALTER TABLE subcommand: {}", cmd.subtype));
        }
    }

    Ok(QueryResult {
        tag: "ALTER TABLE".into(),
        columns: vec![],
        rows: vec![],
    })
}

fn exec_rename(
    rename: &pg_query::protobuf::RenameStmt,
) -> Result<QueryResult, String> {
    let rel = rename.relation.as_ref().ok_or("RENAME missing relation")?;
    let table_name = &rel.relname;
    let schema = if rel.schemaname.is_empty() { "public" } else { &rel.schemaname };

    use pg_query::protobuf::ObjectType;
    if rename.rename_type == ObjectType::ObjectTable as i32 {
        // ALTER TABLE ... RENAME TO new_name
        catalog::rename_table(schema, table_name, &rename.newname)?;
        storage::rename_table(schema, table_name, &rename.newname);
    } else if rename.rename_type == ObjectType::ObjectColumn as i32 {
        // ALTER TABLE ... RENAME COLUMN old TO new
        catalog::rename_column(schema, table_name, &rename.subname, &rename.newname)?;
    } else {
        return Err("unsupported RENAME type".into());
    }

    Ok(QueryResult {
        tag: "ALTER TABLE".into(),
        columns: vec![],
        rows: vec![],
    })
}

fn exec_create_table_as(
    ctas: &pg_query::protobuf::CreateTableAsStmt,
) -> Result<QueryResult, String> {
    let into = ctas.into.as_ref().ok_or("CREATE TABLE AS missing INTO")?;
    let rel = into.rel.as_ref().ok_or("CREATE TABLE AS missing relation")?;
    let table_name = &rel.relname;
    let schema = if rel.schemaname.is_empty() { "public" } else { &rel.schemaname };

    // Execute the source SELECT
    let select_node = ctas.query.as_ref().and_then(|n| n.node.as_ref())
        .ok_or("CREATE TABLE AS missing query")?;
    let NodeEnum::SelectStmt(sel) = select_node else {
        return Err("CREATE TABLE AS requires SELECT".into());
    };

    let mut arena = QueryArena::new();
    let (columns, raw_rows) = exec_select_raw(sel, None, &mut arena)?;

    // Create table from column metadata
    let table_columns: Vec<catalog::Column> = columns.iter().map(|(name, oid)| {
        catalog::Column {
            name: name.clone(),
            type_oid: TypeOid::from_oid(*oid),
            nullable: true,
            primary_key: false,
            unique: false,
            default_expr: None,
        }
    }).collect();

    let table = crate::catalog::Table {
        name: table_name.clone(),
        schema: schema.to_string(),
        columns: table_columns,
    };
    catalog::create_table(table)?;
    storage::create_table(schema, table_name);

    // Insert rows
    let rows: Vec<Vec<crate::types::Value>> = raw_rows.iter().map(|row| {
        row.iter().map(|v| v.to_value(&arena)).collect()
    }).collect();

    if !rows.is_empty() {
        storage::insert_batch(schema, table_name, rows);
    }

    Ok(QueryResult {
        tag: format!("SELECT {}", raw_rows.len()),
        columns: vec![],
        rows: vec![],
    })
}

// ── RETURNING clause evaluation ───────────────────────────────────────

fn eval_returning(
    returning_list: &[pg_query::protobuf::Node],
    affected_rows: &[Vec<Value>],
    table: &Table,
    schema: &str,
    table_name: &str,
    tag: &str,
) -> Result<QueryResult, String> {
    if returning_list.is_empty() {
        return Ok(QueryResult {
            tag: tag.into(),
            columns: vec![],
            rows: vec![],
        });
    }

    let ctx = JoinContext::single(schema, table_name, table.clone());

    // Resolve RETURNING target columns (same logic as SELECT targets)
    let mut columns = Vec::new();
    let mut col_exprs: Vec<ReturningTarget> = Vec::new();

    for node in returning_list {
        if let Some(NodeEnum::ResTarget(rt)) = node.node.as_ref() {
            let val_node = rt.val.as_ref().and_then(|v| v.node.as_ref());
            match val_node {
                Some(NodeEnum::ColumnRef(cref)) => {
                    let fields = extract_string_fields(cref);
                    let has_star = cref.fields.iter().any(|f| {
                        matches!(f.node.as_ref(), Some(NodeEnum::AStar(_)))
                    });
                    if has_star {
                        // RETURNING *
                        for (i, col) in table.columns.iter().enumerate() {
                            columns.push((col.name.clone(), col.type_oid.oid()));
                            col_exprs.push(ReturningTarget::Column(i));
                        }
                    } else {
                        let idx = resolve_column(cref, &ctx)?;
                        let alias = if rt.name.is_empty() {
                            fields.last().cloned().unwrap_or("?column?".into())
                        } else {
                            rt.name.clone()
                        };
                        columns.push((alias, column_type_oid(idx, &ctx)?));
                        col_exprs.push(ReturningTarget::Column(idx));
                    }
                }
                Some(expr) => {
                    let alias = if rt.name.is_empty() {
                        "?column?".into()
                    } else {
                        rt.name.clone()
                    };
                    columns.push((alias, TypeOid::Text.oid()));
                    col_exprs.push(ReturningTarget::Expr(expr.clone()));
                }
                None => {
                    return Err("RETURNING clause contains an invalid expression".into());
                }
            }
        }
    }

    // Evaluate RETURNING expressions against each affected row
    let mut rows = Vec::new();
    let mut ret_arena = QueryArena::new();
    for row in affected_rows {
        let mut result_row = Vec::new();
        for target in &col_exprs {
            let val = match target {
                ReturningTarget::Column(idx) => {
                    if *idx < row.len() {
                        row[*idx].clone()
                    } else {
                        return Err(format!(
                            "internal error: RETURNING column index {} out of range for row of width {}",
                            idx, row.len()
                        ));
                    }
                }
                ReturningTarget::Expr(expr) => {
                    let arena_row: Vec<ArenaValue> = row.iter().map(|v| ArenaValue::from_value(v, &mut ret_arena)).collect();
                    let arena_val = eval_expr(expr, &arena_row, &ctx, &mut ret_arena)?;
                    arena_val.to_value(&ret_arena)
                }
            };
            result_row.push(val.to_text());
        }
        rows.push(result_row);
    }

    Ok(QueryResult {
        tag: tag.into(),
        columns,
        rows,
    })
}

enum ReturningTarget {
    Column(usize),
    Expr(NodeEnum),
}

// ── INSERT ────────────────────────────────────────────────────────────

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

    let select = insert
        .select_stmt
        .as_ref()
        .and_then(|s| s.node.as_ref())
        .ok_or("INSERT missing VALUES")?;

    let has_returning = !insert.returning_list.is_empty();
    let mut all_rows: Vec<Vec<Value>> = Vec::new();

    if let NodeEnum::SelectStmt(sel) = select {
        if !sel.values_lists.is_empty() {
            // INSERT ... VALUES (...)
            for values_list in &sel.values_lists {
                if let Some(NodeEnum::List(list)) = values_list.node.as_ref() {
                    let mut row: Vec<Value> = table_def
                        .columns
                        .iter()
                        .map(|col| {
                            if target_cols.contains(&col.name) {
                                Ok(Value::Null)
                            } else {
                                apply_default(&col.default_expr, schema)
                            }
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    for (i, val_node) in list.items.iter().enumerate() {
                        if i >= target_cols.len() {
                            break;
                        }
                        let col_name = &target_cols[i];
                        let col_idx = table_def
                            .columns
                            .iter()
                            .position(|c| &c.name == col_name)
                            .ok_or_else(|| format!("column \"{}\" does not exist", col_name))?;

                        // Handle DEFAULT keyword — apply column default instead of literal
                        if matches!(val_node.node.as_ref(), Some(NodeEnum::SetToDefault(_))) {
                            row[col_idx] = apply_default(&table_def.columns[col_idx].default_expr, schema)?;
                        } else {
                            row[col_idx] = eval_const(val_node.node.as_ref());
                        }
                    }

                    for (i, col) in table_def.columns.iter().enumerate() {
                        if col.type_oid == TypeOid::Vector {
                            if let Value::Text(s) = &row[i] {
                                row[i] = parse_vector_literal(s.trim())?;
                            }
                        }
                    }

                    check_not_null(&table_def, &row)?;
                    all_rows.push(row);
                }
            }
        } else {
            // INSERT ... SELECT ...
            let mut arena = QueryArena::new();
            let (_cols, raw_rows) = exec_select_raw(sel, None, &mut arena)?;
            for raw_row in &raw_rows {
                let mut row: Vec<Value> = table_def.columns.iter().map(|_| Value::Null).collect();
                for (i, val) in raw_row.iter().enumerate() {
                    if i >= target_cols.len() { break; }
                    let col_name = &target_cols[i];
                    let col_idx = table_def.columns.iter().position(|c| &c.name == col_name)
                        .ok_or_else(|| format!("column \"{}\" does not exist", col_name))?;
                    row[col_idx] = val.to_value(&arena);
                }
                check_not_null(&table_def, &row)?;
                all_rows.push(row);
            }
        }
    }

    // Atomic batch insert: validate ALL rows against existing data AND each other,
    // then insert all at once. If any row fails, nothing is committed. (#4)
    let (unique_checks, pk_cols) = build_unique_checks(&table_def);
    let row_count = all_rows.len() as u64;

    // Auto-create HNSW index on first INSERT if table has a vector column
    let vector_col_idx = table_def
        .columns
        .iter()
        .position(|c| c.type_oid == crate::types::TypeOid::Vector);
    if let Some(col_idx) = vector_col_idx {
        // Ensure index exists (no-op if already created)
        let _ = storage::ensure_hnsw_index(
            schema,
            table_name,
            col_idx,
            crate::hnsw::DistanceMetric::L2,
        );
    }

    let needs_row_copy = has_returning || vector_col_idx.is_some();
    let inserted_rows = if needs_row_copy { all_rows.clone() } else { Vec::new() };

    let base_row_id = storage::insert_batch_checked(
        schema, table_name, all_rows, &unique_checks, &pk_cols,
    )?;

    // Insert vectors into HNSW index using the correct base_row_id from inside the write lock
    if let Some(col_idx) = vector_col_idx {
        if storage::has_hnsw_index(schema, table_name).is_some() {
            for (i, row) in inserted_rows.iter().enumerate() {
                if let Some(crate::types::Value::Vector(v)) = row.get(col_idx) {
                    let _ = storage::hnsw_insert(schema, table_name, base_row_id + i, v.clone());
                }
            }
        }
    }

    let tag = format!("INSERT 0 {}", row_count);
    if has_returning {
        eval_returning(&insert.returning_list, &inserted_rows, &table_def, schema, table_name, &tag)
    } else {
        Ok(QueryResult { tag, columns: vec![], rows: vec![] })
    }
}

// ── DEFAULT value application ─────────────────────────────────────────

fn apply_default(default_expr: &Option<catalog::DefaultExpr>, _schema: &str) -> Result<Value, String> {
    match default_expr {
        Some(catalog::DefaultExpr::Literal(v)) => Ok(v.clone()),
        Some(catalog::DefaultExpr::NextVal(seq_fqn)) => {
            let (seq_schema, seq_name) = parse_seq_name(seq_fqn);
            let val = crate::sequence::nextval(seq_schema, seq_name)?;
            Ok(Value::Int(val))
        }
        None => Ok(Value::Null),
    }
}

// ── Constraint enforcement ────────────────────────────────────────────

fn check_not_null(table: &Table, row: &[Value]) -> Result<(), String> {
    for (i, col) in table.columns.iter().enumerate() {
        if !col.nullable && matches!(row[i], Value::Null) {
            return Err(format!(
                "null value in column \"{}\" violates not-null constraint",
                col.name
            ));
        }
    }
    Ok(())
}

fn build_unique_checks(table: &Table) -> (Vec<(usize, String)>, Vec<usize>) {
    let pk_cols: Vec<usize> = table
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| c.primary_key)
        .map(|(i, _)| i)
        .collect();

    let mut unique_checks = Vec::new();
    for (i, col) in table.columns.iter().enumerate() {
        if col.primary_key && pk_cols.len() > 1 {
            continue; // composite PK checked separately
        }
        if col.primary_key || col.unique {
            let cname = if col.primary_key {
                format!("{}_pkey", table.name)
            } else {
                format!("{}_{}_key", table.name, col.name)
            };
            unique_checks.push((i, cname));
        }
    }

    (unique_checks, pk_cols)
}

/// Validate uniqueness of `new_row` against `all_rows`, excluding row at `skip_idx`.
fn check_unique_against(
    table: &Table,
    new_row: &[Value],
    all_rows: &[Vec<Value>],
    skip_idx: usize,
) -> Result<(), String> {
    let (unique_checks, pk_cols) = build_unique_checks(table);

    // Composite PK
    if pk_cols.len() > 1 {
        let new_key: Vec<&Value> = pk_cols.iter().map(|&i| &new_row[i]).collect();
        for (idx, erow) in all_rows.iter().enumerate() {
            if idx == skip_idx {
                continue;
            }
            let ekey: Vec<&Value> = pk_cols.iter().map(|&i| &erow[i]).collect();
            if new_key == ekey {
                return Err(format!(
                    "duplicate key value violates unique constraint \"{}_pkey\"",
                    table.name
                ));
            }
        }
    }

    for &(col_idx, ref cname) in &unique_checks {
        if matches!(new_row[col_idx], Value::Null) {
            continue;
        }
        for (idx, erow) in all_rows.iter().enumerate() {
            if idx == skip_idx {
                continue;
            }
            if col_idx < erow.len() && erow[col_idx] == new_row[col_idx] {
                return Err(format!(
                    "duplicate key value violates unique constraint \"{}\"",
                    cname
                ));
            }
        }
    }

    Ok(())
}

// ── Expression evaluation ─────────────────────────────────────────────

fn parse_vector_literal(s: &str) -> Result<Value, String> {
    if s.len() < 2 || !s.starts_with('[') || !s.ends_with(']') {
        return Err(format!("malformed vector literal: \"{}\"", s));
    }
    let inner = &s[1..s.len() - 1];
    if inner.trim().is_empty() {
        return Err("vector must have at least 1 dimension".into());
    }
    let parts: Vec<f32> = inner
        .split(',')
        .map(|p| {
            p.trim()
                .parse::<f32>()
                .map_err(|e| format!("invalid vector element \"{}\": {}", p.trim(), e))
        })
        .collect::<Result<_, _>>()?;
    // Reject NaN and Infinity (matches pgvector behavior)
    if let Some(bad) = parts.iter().find(|f| !f.is_finite()) {
        return Err(format!("vector elements must be finite, got {}", bad));
    }
    Ok(Value::Vector(parts))
}

fn eval_const(node: Option<&NodeEnum>) -> Value {
    match node {
        Some(NodeEnum::Integer(i)) => Value::Int(i.ival as i64),
        Some(NodeEnum::Float(f)) => {
            f.fval.parse::<f64>().map(Value::Float).unwrap_or(Value::Null)
        }
        Some(NodeEnum::String(s)) => {
            let trimmed = s.sval.trim();
            if trimmed.starts_with('[') && trimmed.ends_with(']') {
                parse_vector_literal(trimmed).unwrap_or(Value::Text(Arc::from(s.sval.as_str())))
            } else {
                Value::Text(Arc::from(s.sval.as_str()))
            }
        }
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
                        let trimmed = s.sval.trim();
                        if trimmed.starts_with('[') && trimmed.ends_with(']') {
                            parse_vector_literal(trimmed).unwrap_or(Value::Text(Arc::from(s.sval.as_str())))
                        } else {
                            Value::Text(Arc::from(s.sval.as_str()))
                        }
                    }
                    pg_query::protobuf::a_const::Val::Bsval(s) => Value::Text(Arc::from(s.bsval.as_str())),
                    pg_query::protobuf::a_const::Val::Boolval(b) => Value::Bool(b.boolval),
                }
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

#[inline(always)]
fn eval_expr(node: &NodeEnum, row: &[ArenaValue], ctx: &JoinContext, arena: &mut QueryArena) -> Result<ArenaValue, String> {
    match node {
        NodeEnum::ColumnRef(cref) => {
            let idx = resolve_column(cref, ctx)?;
            if idx < row.len() {
                Ok(row[idx]) // ArenaValue is Copy
            } else if ctx.total_columns == 0 {
                Ok(ArenaValue::Null) // no-FROM context (SELECT 1)
            } else {
                Err(format!(
                    "internal error: column index {} out of range for row of width {}",
                    idx, row.len()
                ))
            }
        }
        NodeEnum::AConst(ac) => {
            if ac.isnull {
                return Ok(ArenaValue::Null);
            }
            if let Some(val) = &ac.val {
                match val {
                    pg_query::protobuf::a_const::Val::Ival(i) => Ok(ArenaValue::Int(i.ival as i64)),
                    pg_query::protobuf::a_const::Val::Fval(f) => f
                        .fval
                        .parse::<f64>()
                        .map(ArenaValue::Float)
                        .map_err(|e| e.to_string()),
                    pg_query::protobuf::a_const::Val::Sval(s) => {
                        let trimmed = s.sval.trim();
                        if trimmed.starts_with('[') && trimmed.ends_with(']') {
                            let v = parse_vector_literal(trimmed)?;
                            if let Value::Vector(data) = v {
                                Ok(ArenaValue::Vector(arena.alloc_vec(&data)))
                            } else {
                                Ok(ArenaValue::Null)
                            }
                        } else {
                            Ok(ArenaValue::Text(arena.alloc_str(&s.sval)))
                        }
                    }
                    pg_query::protobuf::a_const::Val::Bsval(s) => Ok(ArenaValue::Text(arena.alloc_str(&s.bsval))),
                    pg_query::protobuf::a_const::Val::Boolval(b) => Ok(ArenaValue::Bool(b.boolval)),
                }
            } else {
                Ok(ArenaValue::Null)
            }
        }
        NodeEnum::Integer(i) => Ok(ArenaValue::Int(i.ival as i64)),
        NodeEnum::Float(f) => f
            .fval
            .parse::<f64>()
            .map(ArenaValue::Float)
            .map_err(|e| e.to_string()),
        NodeEnum::String(s) => {
            let trimmed = s.sval.trim();
            if trimmed.starts_with('[') && trimmed.ends_with(']') {
                let v = parse_vector_literal(trimmed)?;
                if let Value::Vector(data) = v {
                    Ok(ArenaValue::Vector(arena.alloc_vec(&data)))
                } else {
                    Ok(ArenaValue::Null)
                }
            } else {
                Ok(ArenaValue::Text(arena.alloc_str(&s.sval)))
            }
        }
        NodeEnum::TypeCast(tc) => {
            let inner = tc
                .arg
                .as_ref()
                .and_then(|a| a.node.as_ref())
                .ok_or("TypeCast missing arg")?;
            let val = eval_expr(inner, row, ctx, arena)?;
            if let Some(tn) = &tc.type_name {
                let type_name: String = tn
                    .names
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
                    .unwrap_or_default();
                match type_name.as_str() {
                    "vector" => {
                        if let ArenaValue::Text(s) = &val {
                            let text = arena.get_str(*s).to_string();
                            let trimmed = text.trim();
                            if trimmed.starts_with('[') && trimmed.ends_with(']') {
                                let v = parse_vector_literal(trimmed)?;
                                if let Value::Vector(data) = v {
                                    return Ok(ArenaValue::Vector(arena.alloc_vec(&data)));
                                }
                            }
                        }
                    }
                    "int4" | "int" | "integer" | "int8" | "bigint" => {
                        match &val {
                            ArenaValue::Text(s) => {
                                let text = arena.get_str(*s);
                                let i = text.trim().parse::<i64>().map_err(|_| format!("invalid input syntax for integer: \"{}\"", text))?;
                                return Ok(ArenaValue::Int(i));
                            }
                            ArenaValue::Float(f) => return Ok(ArenaValue::Int(f.round() as i64)),
                            ArenaValue::Int(_) => return Ok(val),
                            ArenaValue::Bool(b) => return Ok(ArenaValue::Int(if *b { 1 } else { 0 })),
                            _ => {}
                        }
                    }
                    "float4" | "float8" | "real" | "double precision" | "numeric" => {
                        match &val {
                            ArenaValue::Text(s) => {
                                let text = arena.get_str(*s);
                                let f = text.trim().parse::<f64>().map_err(|_| format!("invalid input syntax for type double precision: \"{}\"", text))?;
                                return Ok(ArenaValue::Float(f));
                            }
                            ArenaValue::Int(i) => return Ok(ArenaValue::Float(*i as f64)),
                            ArenaValue::Float(_) => return Ok(val),
                            _ => {}
                        }
                    }
                    "text" | "varchar" | "char" | "character varying" => {
                        let text = match &val {
                            ArenaValue::Int(i) => i.to_string(),
                            ArenaValue::Float(f) => f.to_string(),
                            ArenaValue::Bool(b) => (if *b { "true" } else { "false" }).to_string(),
                            ArenaValue::Text(_) => return Ok(val),
                            _ => return Ok(val),
                        };
                        return Ok(ArenaValue::Text(arena.alloc_str(&text)));
                    }
                    "bool" | "boolean" => {
                        match &val {
                            ArenaValue::Text(s) => {
                                let text = arena.get_str(*s).trim().to_lowercase();
                                let b = match text.as_str() {
                                    "t" | "true" | "yes" | "on" | "1" => true,
                                    "f" | "false" | "no" | "off" | "0" => false,
                                    _ => return Err(format!("invalid input syntax for type boolean: \"{}\"", text)),
                                };
                                return Ok(ArenaValue::Bool(b));
                            }
                            ArenaValue::Int(i) => return Ok(ArenaValue::Bool(*i != 0)),
                            ArenaValue::Bool(_) => return Ok(val),
                            _ => {}
                        }
                    }
                    _ => {} // unknown cast type — pass through
                }
            }
            Ok(val)
        }
        NodeEnum::AExpr(expr) => eval_a_expr(expr, row, ctx, arena),
        NodeEnum::BoolExpr(bexpr) => eval_bool_expr(bexpr, row, ctx, arena),
        NodeEnum::NullTest(nt) => {
            let inner = nt
                .arg
                .as_ref()
                .and_then(|a| a.node.as_ref())
                .ok_or("NullTest missing arg")?;
            let val = eval_expr(inner, row, ctx, arena)?;
            let is_null = val.is_null();
            if nt.nulltesttype == pg_query::protobuf::NullTestType::IsNull as i32 {
                Ok(ArenaValue::Bool(is_null))
            } else {
                Ok(ArenaValue::Bool(!is_null))
            }
        }
        NodeEnum::CaseExpr(case_expr) => eval_case_expr(case_expr, row, ctx, arena),
        NodeEnum::CoalesceExpr(ce) => {
            for arg in &ce.args {
                if let Some(ref node) = arg.node {
                    let val = eval_expr(node, row, ctx, arena)?;
                    if !val.is_null() {
                        return Ok(val);
                    }
                }
            }
            Ok(ArenaValue::Null)
        }
        NodeEnum::NullIfExpr(ni) => {
            if ni.args.len() != 2 {
                return Err("NULLIF requires exactly 2 arguments".into());
            }
            let a = eval_expr(ni.args[0].node.as_ref().ok_or("NULLIF missing arg1")?, row, ctx, arena)?;
            let b = eval_expr(ni.args[1].node.as_ref().ok_or("NULLIF missing arg2")?, row, ctx, arena)?;
            if a.is_null() || b.is_null() {
                Ok(a) // NULLIF(NULL, x) = NULL, NULLIF(x, NULL) = x (since they're not equal)
            } else if a.eq_with(&b, arena) || a.compare(&b, arena) == Some(std::cmp::Ordering::Equal) {
                Ok(ArenaValue::Null)
            } else {
                Ok(a)
            }
        }
        NodeEnum::FuncCall(fc) => eval_func_call(fc, row, ctx, arena),
        NodeEnum::SubLink(sl) => {
            let inner = sl
                .subselect
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or("SubLink missing subselect")?;

            match inner {
                NodeEnum::SelectStmt(sel) => {
                    let sub_type = sl.sub_link_type;

                    // EXISTS
                    if sub_type == pg_query::protobuf::SubLinkType::ExistsSublink as i32 {
                        let (_cols, inner_rows) =
                            exec_select_raw(sel, Some((row, ctx)), arena)?;
                        return Ok(ArenaValue::Bool(!inner_rows.is_empty()));
                    }

                    // ANY / IN (sub_link_type = AnySublink)
                    if sub_type == pg_query::protobuf::SubLinkType::AnySublink as i32 {
                        let test_node = sl
                            .testexpr
                            .as_ref()
                            .and_then(|n| n.node.as_ref())
                            .ok_or("IN subquery missing test expression")?;
                        let test_val = eval_expr(test_node, row, ctx, arena)?;

                        let (cols, inner_rows) =
                            exec_select_raw(sel, Some((row, ctx)), arena)?;

                        // Validate: subquery must return exactly one column
                        if !cols.is_empty() && cols.len() != 1 {
                            return Err("subquery must return only one column".into());
                        }

                        if test_val.is_null() {
                            return Ok(ArenaValue::Null);
                        }

                        let mut has_null = false;
                        for inner_row in &inner_rows {
                            let inner_val =
                                inner_row.first().copied().unwrap_or(ArenaValue::Null);
                            if inner_val.is_null() {
                                has_null = true;
                                continue;
                            }
                            let is_eq = test_val.eq_with(&inner_val, arena)
                                || test_val.compare(&inner_val, arena)
                                    == Some(std::cmp::Ordering::Equal);
                            if is_eq {
                                return Ok(ArenaValue::Bool(true));
                            }
                        }
                        return Ok(if has_null {
                            ArenaValue::Null
                        } else {
                            ArenaValue::Bool(false)
                        });
                    }

                    // ALL (sub_link_type = AllSublink)
                    if sub_type == pg_query::protobuf::SubLinkType::AllSublink as i32 {
                        let test_node = sl
                            .testexpr
                            .as_ref()
                            .and_then(|n| n.node.as_ref())
                            .ok_or("ALL subquery missing test expression")?;
                        let test_val = eval_expr(test_node, row, ctx, arena)?;

                        let (_cols, inner_rows) =
                            exec_select_raw(sel, Some((row, ctx)), arena)?;

                        // NULL op ALL(empty) = TRUE, NULL op ALL(non-empty) = NULL
                        if test_val.is_null() {
                            return Ok(if inner_rows.is_empty() {
                                ArenaValue::Bool(true)
                            } else {
                                ArenaValue::Null
                            });
                        }

                        let op = sl
                            .oper_name
                            .iter()
                            .filter_map(|n| n.node.as_ref())
                            .filter_map(|n| {
                                if let NodeEnum::String(s) = n {
                                    Some(s.sval.clone())
                                } else {
                                    None
                                }
                            })
                            .next()
                            .unwrap_or_else(|| "=".into());

                        // Three-valued logic: FALSE if any comparison is false,
                        // NULL if no false but some NULL, TRUE if all true.
                        let mut has_null = false;
                        for inner_row in &inner_rows {
                            let inner_val =
                                inner_row.first().copied().unwrap_or(ArenaValue::Null);
                            let cmp_result =
                                eval_comparison_op(&op, &test_val, &inner_val, arena)?;
                            match cmp_result {
                                ArenaValue::Bool(true) => continue,
                                ArenaValue::Bool(false) => return Ok(ArenaValue::Bool(false)),
                                _ => has_null = true,
                            }
                        }
                        return Ok(if has_null { ArenaValue::Null } else { ArenaValue::Bool(true) });
                    }

                    // Scalar subquery (ExprSublink)
                    if sub_type == pg_query::protobuf::SubLinkType::ExprSublink as i32 {
                        let (_cols, inner_rows) =
                            exec_select_raw(sel, Some((row, ctx)), arena)?;
                        if inner_rows.len() > 1 {
                            return Err("more than one row returned by a subquery used as an expression".into());
                        }
                        return Ok(inner_rows
                            .first()
                            .and_then(|r| r.first())
                            .copied()
                            .unwrap_or(ArenaValue::Null));
                    }

                    Err(format!("unsupported subquery type: {}", sub_type))
                }
                _ => Err("SubLink subselect is not a SELECT".into()),
            }
        }
        _ => Err(format!(
            "unsupported expression node: {:?}",
            std::mem::discriminant(node)
        )),
    }
}

#[inline(always)]
fn eval_comparison_op(op: &str, left: &ArenaValue, right: &ArenaValue, arena: &QueryArena) -> Result<ArenaValue, String> {
    if left.is_null() || right.is_null() {
        return Ok(ArenaValue::Null);
    }
    let cmp = left.compare(right, arena);
    let result = match op {
        "=" => cmp.map(|o| o == std::cmp::Ordering::Equal),
        "<>" | "!=" => cmp.map(|o| o != std::cmp::Ordering::Equal),
        "<" => cmp.map(|o| o == std::cmp::Ordering::Less),
        ">" => cmp.map(|o| o == std::cmp::Ordering::Greater),
        "<=" => cmp.map(|o| o != std::cmp::Ordering::Greater),
        ">=" => cmp.map(|o| o != std::cmp::Ordering::Less),
        _ => return Err(format!("unsupported operator in ALL: {}", op)),
    };
    Ok(result.map(ArenaValue::Bool).unwrap_or(ArenaValue::Null))
}

fn eval_a_expr(
    expr: &pg_query::protobuf::AExpr,
    row: &[ArenaValue],
    ctx: &JoinContext,
    arena: &mut QueryArena,
) -> Result<ArenaValue, String> {
    // Handle IN / NOT IN literal lists (AexprIn)
    if expr.kind == pg_query::protobuf::AExprKind::AexprIn as i32 {
        let in_op = extract_op_name(&expr.name).unwrap_or_default();
        let negated = in_op == "<>";

        let left_node = expr
            .lexpr
            .as_ref()
            .and_then(|n| n.node.as_ref())
            .ok_or("IN missing left operand")?;
        let left_val = eval_expr(left_node, row, ctx, arena)?;

        if left_val.is_null() {
            return Ok(ArenaValue::Null);
        }

        // rexpr is a List of values
        let right_list = expr
            .rexpr
            .as_ref()
            .and_then(|n| n.node.as_ref())
            .ok_or("IN missing right operand")?;

        if let NodeEnum::List(list) = right_list {
            let mut has_null = false;
            for item in &list.items {
                if let Some(item_node) = item.node.as_ref() {
                    let item_val = eval_expr(item_node, row, ctx, arena)?;
                    if item_val.is_null() {
                        has_null = true;
                        continue;
                    }
                    let is_eq = left_val.eq_with(&item_val, arena)
                        || left_val.compare(&item_val, arena) == Some(std::cmp::Ordering::Equal);
                    if is_eq {
                        return Ok(ArenaValue::Bool(!negated));
                    }
                }
            }
            return Ok(if has_null {
                ArenaValue::Null
            } else {
                ArenaValue::Bool(negated)
            });
        }

        return Err("IN requires a list on the right".into());
    }

    // Handle LIKE / ILIKE / NOT LIKE / NOT ILIKE
    if expr.kind == pg_query::protobuf::AExprKind::AexprLike as i32
        || expr.kind == pg_query::protobuf::AExprKind::AexprIlike as i32
    {
        let left_node = expr.lexpr.as_ref().and_then(|n| n.node.as_ref())
            .ok_or("LIKE missing left operand")?;
        let right_node = expr.rexpr.as_ref().and_then(|n| n.node.as_ref())
            .ok_or("LIKE missing right operand")?;
        let left = eval_expr(left_node, row, ctx, arena)?;
        let right = eval_expr(right_node, row, ctx, arena)?;

        if left.is_null() || right.is_null() {
            return Ok(ArenaValue::Null);
        }

        let text = left.to_text(arena).unwrap_or_default();
        let pattern = right.to_text(arena).unwrap_or_default();
        let case_insensitive = expr.kind == pg_query::protobuf::AExprKind::AexprIlike as i32;

        let matched = sql_like_match(&text, &pattern, case_insensitive);

        // NOT LIKE / NOT ILIKE: pg_query uses "!~~" / "!~~*" as the operator name
        let op_name = extract_op_name(&expr.name).unwrap_or_default();
        let negated = op_name.starts_with("!") || op_name == "not like" || op_name == "not ilike";

        return Ok(ArenaValue::Bool(if negated { !matched } else { matched }));
    }

    // Handle NULLIF(a, b) — parsed as AExpr with AexprNullif kind
    if expr.kind == pg_query::protobuf::AExprKind::AexprNullif as i32 {
        let left_node = expr.lexpr.as_ref().and_then(|n| n.node.as_ref())
            .ok_or("NULLIF missing first argument")?;
        let right_node = expr.rexpr.as_ref().and_then(|n| n.node.as_ref())
            .ok_or("NULLIF missing second argument")?;
        let a = eval_expr(left_node, row, ctx, arena)?;
        let b = eval_expr(right_node, row, ctx, arena)?;
        if a.is_null() { return Ok(ArenaValue::Null); }
        if b.is_null() { return Ok(a); }
        if a.eq_with(&b, arena) || a.compare(&b, arena) == Some(std::cmp::Ordering::Equal) {
            return Ok(ArenaValue::Null);
        }
        return Ok(a);
    }

    // Handle BETWEEN / NOT BETWEEN
    if expr.kind == pg_query::protobuf::AExprKind::AexprBetween as i32
        || expr.kind == pg_query::protobuf::AExprKind::AexprNotBetween as i32
    {
        let negated = expr.kind == pg_query::protobuf::AExprKind::AexprNotBetween as i32;
        let left_node = expr.lexpr.as_ref().and_then(|n| n.node.as_ref())
            .ok_or("BETWEEN missing left operand")?;
        let val = eval_expr(left_node, row, ctx, arena)?;
        if val.is_null() { return Ok(ArenaValue::Null); }

        // rexpr is a List of [low, high]
        let bounds = expr.rexpr.as_ref().and_then(|n| n.node.as_ref())
            .ok_or("BETWEEN missing bounds")?;
        if let NodeEnum::List(list) = bounds {
            if list.items.len() != 2 {
                return Err("BETWEEN requires exactly two bounds".into());
            }
            let low = eval_expr(list.items[0].node.as_ref().ok_or("BETWEEN missing low")?, row, ctx, arena)?;
            let high = eval_expr(list.items[1].node.as_ref().ok_or("BETWEEN missing high")?, row, ctx, arena)?;
            if low.is_null() || high.is_null() { return Ok(ArenaValue::Null); }
            let ge = val.compare(&low, arena).map(|o| o != std::cmp::Ordering::Less).unwrap_or(false);
            let le = val.compare(&high, arena).map(|o| o != std::cmp::Ordering::Greater).unwrap_or(false);
            let in_range = ge && le;
            return Ok(ArenaValue::Bool(if negated { !in_range } else { in_range }));
        }
        return Err("BETWEEN bounds must be a list".into());
    }

    let op = extract_op_name(&expr.name)?;

    // Unary minus
    if expr.lexpr.is_none() && op == "-" {
        let right = expr
            .rexpr
            .as_ref()
            .and_then(|n| n.node.as_ref())
            .ok_or("unary - missing operand")?;
        return match eval_expr(right, row, ctx, arena)? {
            ArenaValue::Int(n) => Ok(ArenaValue::Int(
                n.checked_neg().ok_or("integer out of range")?,
            )),
            ArenaValue::Float(f) => Ok(ArenaValue::Float(-f)),
            ArenaValue::Null => Ok(ArenaValue::Null),
            _ => Err("unary minus requires numeric".into()),
        };
    }

    let left_node = expr
        .lexpr
        .as_ref()
        .and_then(|n| n.node.as_ref())
        .ok_or("A_Expr missing left operand")?;
    let right_node = expr
        .rexpr
        .as_ref()
        .and_then(|n| n.node.as_ref())
        .ok_or("A_Expr missing right operand")?;

    let left = eval_expr(left_node, row, ctx, arena)?;
    let right = eval_expr(right_node, row, ctx, arena)?;

    // String concatenation
    if op == "||" {
        return match (&left, &right) {
            (ArenaValue::Null, _) | (_, ArenaValue::Null) => Ok(ArenaValue::Null),
            _ => {
                let l = left.to_text(arena).unwrap_or_default();
                let r = right.to_text(arena).unwrap_or_default();
                let combined = format!("{}{}", l, r);
                Ok(ArenaValue::Text(arena.alloc_str(&combined)))
            }
        };
    }

    // Arithmetic
    if matches!(op.as_str(), "+" | "-" | "*" | "/" | "%") {
        return eval_arithmetic(&op, &left, &right, arena);
    }

    // Vector distance operators (return Float, not Bool — no NULL propagation needed here)
    match op.as_str() {
        "<->" => {
            return match (&left, &right) {
                (ArenaValue::Null, _) | (_, ArenaValue::Null) => Ok(ArenaValue::Null),
                (ArenaValue::Vector(a), ArenaValue::Vector(b)) => {
                    let va = arena.get_vec(*a);
                    let vb = arena.get_vec(*b);
                    if va.len() != vb.len() {
                        return Err(format!("different vector dimensions {} and {}", va.len(), vb.len()));
                    }
                    let dist_sq: f32 = va.iter().zip(vb.iter())
                        .map(|(x, y)| { let d = x - y; d * d })
                        .sum();
                    Ok(ArenaValue::Float(dist_sq.sqrt() as f64))
                }
                _ => Err("operator <-> requires vector operands".into()),
            };
        }
        "<=>" => {
            return match (&left, &right) {
                (ArenaValue::Null, _) | (_, ArenaValue::Null) => Ok(ArenaValue::Null),
                (ArenaValue::Vector(a), ArenaValue::Vector(b)) => {
                    let va = arena.get_vec(*a);
                    let vb = arena.get_vec(*b);
                    if va.len() != vb.len() {
                        return Err(format!("different vector dimensions {} and {}", va.len(), vb.len()));
                    }
                    let (dot, norm_a_sq, norm_b_sq) = va.iter().zip(vb.iter())
                        .fold((0.0f32, 0.0f32, 0.0f32), |(d, na, nb), (x, y)| {
                            (d + x * y, na + x * x, nb + y * y)
                        });
                    let denom = norm_a_sq.sqrt() * norm_b_sq.sqrt();
                    if denom == 0.0 {
                        Ok(ArenaValue::Float(1.0))
                    } else {
                        Ok(ArenaValue::Float((1.0 - dot / denom) as f64))
                    }
                }
                _ => Err("operator <=> requires vector operands".into()),
            };
        }
        "<#>" => {
            return match (&left, &right) {
                (ArenaValue::Null, _) | (_, ArenaValue::Null) => Ok(ArenaValue::Null),
                (ArenaValue::Vector(a), ArenaValue::Vector(b)) => {
                    let va = arena.get_vec(*a);
                    let vb = arena.get_vec(*b);
                    if va.len() != vb.len() {
                        return Err(format!("different vector dimensions {} and {}", va.len(), vb.len()));
                    }
                    let dot: f32 = va.iter().zip(vb.iter()).map(|(x, y)| x * y).sum();
                    Ok(ArenaValue::Float((-dot) as f64))
                }
                _ => Err("operator <#> requires vector operands".into()),
            };
        }
        _ => {}
    }

    // NULL propagation for comparisons
    if left.is_null() || right.is_null() {
        return Ok(ArenaValue::Null);
    }

    let cmp = left.compare(&right, arena);
    let result = match op.as_str() {
        "=" => cmp.map(|o| o == std::cmp::Ordering::Equal),
        "<>" | "!=" => cmp.map(|o| o != std::cmp::Ordering::Equal),
        "<" => cmp.map(|o| o == std::cmp::Ordering::Less),
        ">" => cmp.map(|o| o == std::cmp::Ordering::Greater),
        "<=" => cmp.map(|o| o != std::cmp::Ordering::Greater),
        ">=" => cmp.map(|o| o != std::cmp::Ordering::Less),
        _ => return Err(format!("unsupported operator: {}", op)),
    };
    Ok(result.map(ArenaValue::Bool).unwrap_or(ArenaValue::Null))
}

fn eval_bool_expr(
    bexpr: &pg_query::protobuf::BoolExpr,
    row: &[ArenaValue],
    ctx: &JoinContext,
    arena: &mut QueryArena,
) -> Result<ArenaValue, String> {
    let boolop = bexpr.boolop;
    if boolop == pg_query::protobuf::BoolExprType::AndExpr as i32 {
        let mut has_null = false;
        for arg in &bexpr.args {
            let inner = arg.node.as_ref().ok_or("BoolExpr: missing arg")?;
            match eval_expr(inner, row, ctx, arena)? {
                ArenaValue::Bool(false) => return Ok(ArenaValue::Bool(false)),
                ArenaValue::Null => has_null = true,
                ArenaValue::Bool(true) => {}
                other => return Err(format!("AND expects bool, got {:?}", other)),
            }
        }
        Ok(if has_null { ArenaValue::Null } else { ArenaValue::Bool(true) })
    } else if boolop == pg_query::protobuf::BoolExprType::OrExpr as i32 {
        let mut has_null = false;
        for arg in &bexpr.args {
            let inner = arg.node.as_ref().ok_or("BoolExpr: missing arg")?;
            match eval_expr(inner, row, ctx, arena)? {
                ArenaValue::Bool(true) => return Ok(ArenaValue::Bool(true)),
                ArenaValue::Null => has_null = true,
                ArenaValue::Bool(false) => {}
                other => return Err(format!("OR expects bool, got {:?}", other)),
            }
        }
        Ok(if has_null { ArenaValue::Null } else { ArenaValue::Bool(false) })
    } else if boolop == pg_query::protobuf::BoolExprType::NotExpr as i32 {
        let inner = bexpr
            .args
            .first()
            .and_then(|a| a.node.as_ref())
            .ok_or("NOT missing arg")?;
        match eval_expr(inner, row, ctx, arena)? {
            ArenaValue::Bool(b) => Ok(ArenaValue::Bool(!b)),
            ArenaValue::Null => Ok(ArenaValue::Null),
            other => Err(format!("NOT expects bool, got {:?}", other)),
        }
    } else {
        Err(format!("unsupported BoolExpr op: {}", boolop))
    }
}

/// Deduplicate rows when DISTINCT is present. No-op if distinct_clause is empty.
fn dedup_distinct(
    distinct_clause: &[pg_query::protobuf::Node],
    rows: Vec<Vec<ArenaValue>>,
    arena: &QueryArena,
) -> Vec<Vec<ArenaValue>> {
    if distinct_clause.is_empty() {
        return rows;
    }
    let mut unique: Vec<Vec<ArenaValue>> = Vec::new();
    'outer: for row in rows {
        for existing in &unique {
            if existing.len() == row.len()
                && existing.iter().zip(row.iter()).all(|(a, b)| a.eq_with(b, arena))
            {
                continue 'outer;
            }
        }
        unique.push(row);
    }
    unique
}

/// Evaluate CASE WHEN ... THEN ... ELSE ... END expressions.
/// Two forms: simple CASE (has arg) and searched CASE (no arg).
fn eval_case_expr(
    case_expr: &pg_query::protobuf::CaseExpr,
    row: &[ArenaValue],
    ctx: &JoinContext,
    arena: &mut QueryArena,
) -> Result<ArenaValue, String> {
    // Simple CASE: CASE expr WHEN val1 THEN res1 ...
    // Searched CASE: CASE WHEN cond1 THEN res1 ...
    let test_val = if let Some(ref arg) = case_expr.arg {
        if let Some(ref node) = arg.node {
            Some(eval_expr(node, row, ctx, arena)?)
        } else {
            None
        }
    } else {
        None
    };

    for when_node in &case_expr.args {
        if let Some(NodeEnum::CaseWhen(when)) = when_node.node.as_ref() {
            let cond_node = when.expr.as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or("CASE WHEN missing condition")?;

            let matches = if let Some(ref tv) = test_val {
                // Simple CASE: compare test value with WHEN value
                let when_val = eval_expr(cond_node, row, ctx, arena)?;
                if tv.is_null() || when_val.is_null() {
                    false // NULL never matches in simple CASE
                } else {
                    tv.eq_with(&when_val, arena)
                        || tv.compare(&when_val, arena) == Some(std::cmp::Ordering::Equal)
                }
            } else {
                // Searched CASE: evaluate condition as boolean
                match eval_expr(cond_node, row, ctx, arena)? {
                    ArenaValue::Bool(b) => b,
                    ArenaValue::Null => false, // NULL condition = not matched
                    _ => false,
                }
            };

            if matches {
                let result_node = when.result.as_ref()
                    .and_then(|n| n.node.as_ref())
                    .ok_or("CASE WHEN missing result")?;
                return eval_expr(result_node, row, ctx, arena);
            }
        }
    }

    // No WHEN matched — use ELSE or NULL
    if let Some(ref def) = case_expr.defresult {
        if let Some(ref node) = def.node {
            return eval_expr(node, row, ctx, arena);
        }
    }
    Ok(ArenaValue::Null)
}

/// SQL LIKE pattern matching. `%` matches any sequence, `_` matches one char.
/// Backslash escapes the next character (e.g., `\%` matches literal `%`).
fn sql_like_match(text: &str, pattern: &str, case_insensitive: bool) -> bool {
    let text_owned;
    let pattern_owned;
    let (text, pattern) = if case_insensitive {
        text_owned = text.to_lowercase();
        pattern_owned = pattern.to_lowercase();
        (text_owned.as_str(), pattern_owned.as_str())
    } else {
        (text, pattern)
    };

    let t: Vec<char> = text.chars().collect();
    let p: Vec<char> = pattern.chars().collect();
    let (tlen, plen) = (t.len(), p.len());

    // dp[i][j] = true if text[0..i] matches pattern[0..j]
    let mut dp = vec![vec![false; plen + 1]; tlen + 1];
    dp[0][0] = true;

    // Leading % can match empty string
    for j in 1..=plen {
        if p[j - 1] == '%' {
            dp[0][j] = dp[0][j - 1];
        } else {
            break;
        }
    }

    for i in 1..=tlen {
        let mut j = 0;
        while j < plen {
            let pj = j;
            let pc = if p[pj] == '\\' && pj + 1 < plen {
                // Escaped character — match literally
                j += 1; // consume the backslash
                (p[j], true) // literal match
            } else {
                (p[pj], false)
            };

            if pc.1 {
                // Escaped literal
                dp[i][j + 1] = dp[i - 1][pj] && t[i - 1] == pc.0;
            } else if pc.0 == '%' {
                dp[i][j + 1] = dp[i][j] || dp[i - 1][j + 1];
            } else if pc.0 == '_' {
                dp[i][j + 1] = dp[i - 1][j];
            } else {
                dp[i][j + 1] = dp[i - 1][j] && t[i - 1] == pc.0;
            }
            j += 1;
        }
    }
    dp[tlen][plen]
}

#[inline(always)]
fn eval_arithmetic(op: &str, left: &ArenaValue, right: &ArenaValue, arena: &QueryArena) -> Result<ArenaValue, String> {
    if left.is_null() || right.is_null() {
        return Ok(ArenaValue::Null);
    }
    match (left, right) {
        (ArenaValue::Int(a), ArenaValue::Int(b)) => match op {
            "+" => Ok(ArenaValue::Int(
                a.checked_add(*b).ok_or("integer out of range")?,
            )),
            "-" => Ok(ArenaValue::Int(
                a.checked_sub(*b).ok_or("integer out of range")?,
            )),
            "*" => Ok(ArenaValue::Int(
                a.checked_mul(*b).ok_or("integer out of range")?,
            )),
            "/" => {
                if *b == 0 {
                    Err("division by zero".into())
                } else {
                    Ok(ArenaValue::Int(
                        a.checked_div(*b).ok_or("integer out of range")?,
                    ))
                }
            }
            "%" => {
                if *b == 0 { Err("division by zero".into()) }
                else { Ok(ArenaValue::Int(a.checked_rem(*b).ok_or("integer out of range")?)) }
            }
            _ => Err(format!("unsupported arithmetic op: {}", op)),
        },
        (ArenaValue::Float(a), ArenaValue::Float(b)) => match op {
            "+" => Ok(ArenaValue::Float(a + b)),
            "-" => Ok(ArenaValue::Float(a - b)),
            "*" => Ok(ArenaValue::Float(a * b)),
            "/" => {
                if *b == 0.0 {
                    Err("division by zero".into())
                } else {
                    Ok(ArenaValue::Float(a / b))
                }
            }
            "%" => {
                if *b == 0.0 { Err("division by zero".into()) }
                else { Ok(ArenaValue::Float(a % b)) }
            }
            _ => Err(format!("unsupported arithmetic op: {}", op)),
        },
        (ArenaValue::Int(a), ArenaValue::Float(b)) => {
            eval_arithmetic(op, &ArenaValue::Float(*a as f64), &ArenaValue::Float(*b), arena)
        }
        (ArenaValue::Float(a), ArenaValue::Int(b)) => {
            eval_arithmetic(op, &ArenaValue::Float(*a), &ArenaValue::Float(*b as f64), arena)
        }
        _ => Err(format!("cannot apply {} to {:?} and {:?}", op, left, right)),
    }
}

fn eval_func_call(
    fc: &pg_query::protobuf::FuncCall,
    row: &[ArenaValue],
    ctx: &JoinContext,
    arena: &mut QueryArena,
) -> Result<ArenaValue, String> {
    let name = extract_func_name(fc);

    // Aggregate functions are handled in exec_select_aggregate, not here.
    // If we reach here, it's a scalar function.
    let args: Vec<ArenaValue> = fc
        .args
        .iter()
        .map(|a| {
            eval_expr(
                a.node.as_ref().ok_or("FuncCall: missing arg")?,
                row,
                ctx,
                arena,
            )
        })
        .collect::<Result<_, _>>()?;

    eval_scalar_function(&name, &args, arena)
}

/// Parse a sequence name that may be schema-qualified ("public.myseq" or just "myseq").
/// Parse a text string back to a typed Value based on the column's OID.
fn parse_text_to_value(s: &str, oid: i32) -> Value {
    match oid {
        16 => Value::Bool(s == "t" || s == "true"),
        20 | 21 | 23 => s.parse::<i64>().map(Value::Int).unwrap_or(Value::Text(Arc::from(s))),
        700 | 701 | 1700 => s.parse::<f64>().map(Value::Float).unwrap_or(Value::Text(Arc::from(s))),
        16385 => parse_vector_literal(s).unwrap_or(Value::Text(Arc::from(s))),
        _ => Value::Text(Arc::from(s)),
    }
}

fn parse_seq_name(s: &str) -> (&str, &str) {
    match s.split_once('.') {
        Some((schema, name)) => (schema, name),
        None => ("public", s),
    }
}

fn eval_scalar_function(name: &str, args: &[ArenaValue], arena: &mut QueryArena) -> Result<ArenaValue, String> {
    match name {
        "upper" => match args.first() {
            Some(ArenaValue::Text(s)) => {
                let upper = arena.get_str(*s).to_uppercase();
                Ok(ArenaValue::Text(arena.alloc_str(&upper)))
            }
            Some(ArenaValue::Null) => Ok(ArenaValue::Null),
            _ => Err("upper() requires text argument".into()),
        },
        "lower" => match args.first() {
            Some(ArenaValue::Text(s)) => {
                let lower = arena.get_str(*s).to_lowercase();
                Ok(ArenaValue::Text(arena.alloc_str(&lower)))
            }
            Some(ArenaValue::Null) => Ok(ArenaValue::Null),
            _ => Err("lower() requires text argument".into()),
        },
        "length" => match args.first() {
            Some(ArenaValue::Text(s)) => Ok(ArenaValue::Int(arena.get_str(*s).len() as i64)),
            Some(ArenaValue::Null) => Ok(ArenaValue::Null),
            _ => Err("length() requires text argument".into()),
        },
        "concat" => {
            let parts: String = args
                .iter()
                .map(|v| match v {
                    ArenaValue::Null => String::new(),
                    v => v.to_text(arena).unwrap_or_default(),
                })
                .collect();
            Ok(ArenaValue::Text(arena.alloc_str(&parts)))
        }
        "abs" => match args.first() {
            Some(ArenaValue::Int(n)) => Ok(ArenaValue::Int(
                n.checked_abs().ok_or("integer out of range")?,
            )),
            Some(ArenaValue::Float(f)) => Ok(ArenaValue::Float(f.abs())),
            Some(ArenaValue::Null) => Ok(ArenaValue::Null),
            _ => Err("abs() requires numeric argument".into()),
        },
        "nextval" => match args.first() {
            Some(ArenaValue::Text(s)) => {
                let text = arena.get_str(*s).to_string();
                let (schema, name) = parse_seq_name(&text);
                let val = crate::sequence::nextval(schema, name)?;
                Ok(ArenaValue::Int(val))
            }
            Some(ArenaValue::Null) => Ok(ArenaValue::Null),
            _ => Err("nextval() requires text argument".into()),
        },
        "currval" => match args.first() {
            Some(ArenaValue::Text(s)) => {
                let text = arena.get_str(*s).to_string();
                let (schema, name) = parse_seq_name(&text);
                let val = crate::sequence::currval(schema, name)?;
                Ok(ArenaValue::Int(val))
            }
            Some(ArenaValue::Null) => Ok(ArenaValue::Null),
            _ => Err("currval() requires text argument".into()),
        },
        "setval" => match (args.first(), args.get(1)) {
            (Some(ArenaValue::Text(s)), Some(ArenaValue::Int(v))) => {
                let text = arena.get_str(*s).to_string();
                let (schema, name) = parse_seq_name(&text);
                let val = crate::sequence::setval(schema, name, *v)?;
                Ok(ArenaValue::Int(val))
            }
            _ => Err("setval() requires (text, integer) arguments".into()),
        },
        // COALESCE/NULLIF as function calls (pg_query sometimes routes here)
        "coalesce" => {
            for arg in args {
                if !arg.is_null() { return Ok(*arg); }
            }
            Ok(ArenaValue::Null)
        }
        "nullif" => {
            if args.len() != 2 { return Err("nullif() requires 2 arguments".into()); }
            if args[0].is_null() { return Ok(ArenaValue::Null); }
            if args[1].is_null() { return Ok(args[0]); }
            if args[0].eq_with(&args[1], arena) || args[0].compare(&args[1], arena) == Some(std::cmp::Ordering::Equal) {
                Ok(ArenaValue::Null)
            } else {
                Ok(args[0])
            }
        }
        // ── String functions ────────────────────────────────────
        "substring" | "substr" => {
            if args.is_empty() { return Err("substring() requires at least 2 arguments".into()); }
            if args[0].is_null() { return Ok(ArenaValue::Null); }
            let s = args[0].to_text(arena).unwrap_or_default();
            let chars: Vec<char> = s.chars().collect();
            // FROM position (1-based)
            let from = match args.get(1) {
                Some(ArenaValue::Int(n)) => (*n as usize).saturating_sub(1), // 1-based to 0-based
                _ => 0,
            };
            // FOR length (optional)
            let len = match args.get(2) {
                Some(ArenaValue::Int(n)) => *n as usize,
                _ => chars.len().saturating_sub(from),
            };
            let result: String = chars.iter().skip(from).take(len).collect();
            Ok(ArenaValue::Text(arena.alloc_str(&result)))
        }
        "btrim" | "trim" => {
            if args.is_empty() { return Err("trim() requires at least 1 argument".into()); }
            if args[0].is_null() { return Ok(ArenaValue::Null); }
            let s = args[0].to_text(arena).unwrap_or_default();
            let chars_to_trim = match args.get(1) {
                Some(ArenaValue::Text(t)) => arena.get_str(*t).to_string(),
                _ => " ".to_string(),
            };
            let trimmed = s.trim_matches(|c: char| chars_to_trim.contains(c)).to_string();
            Ok(ArenaValue::Text(arena.alloc_str(&trimmed)))
        }
        "ltrim" => {
            if args.is_empty() { return Err("ltrim() requires at least 1 argument".into()); }
            if args[0].is_null() { return Ok(ArenaValue::Null); }
            let s = args[0].to_text(arena).unwrap_or_default();
            let chars_to_trim = match args.get(1) {
                Some(ArenaValue::Text(t)) => arena.get_str(*t).to_string(),
                _ => " ".to_string(),
            };
            let trimmed = s.trim_start_matches(|c: char| chars_to_trim.contains(c)).to_string();
            Ok(ArenaValue::Text(arena.alloc_str(&trimmed)))
        }
        "rtrim" => {
            if args.is_empty() { return Err("rtrim() requires at least 1 argument".into()); }
            if args[0].is_null() { return Ok(ArenaValue::Null); }
            let s = args[0].to_text(arena).unwrap_or_default();
            let chars_to_trim = match args.get(1) {
                Some(ArenaValue::Text(t)) => arena.get_str(*t).to_string(),
                _ => " ".to_string(),
            };
            let trimmed = s.trim_end_matches(|c: char| chars_to_trim.contains(c)).to_string();
            Ok(ArenaValue::Text(arena.alloc_str(&trimmed)))
        }
        "strpos" | "position" => {
            // POSITION(substr IN str) → pg_query calls strpos(str, substr)
            if args.len() != 2 { return Err("strpos() requires 2 arguments".into()); }
            if args[0].is_null() || args[1].is_null() { return Ok(ArenaValue::Null); }
            let haystack = args[0].to_text(arena).unwrap_or_default();
            let needle = args[1].to_text(arena).unwrap_or_default();
            let pos = haystack.find(&needle).map(|p| p + 1).unwrap_or(0); // 1-based, 0 if not found
            Ok(ArenaValue::Int(pos as i64))
        }
        "replace" => {
            if args.len() != 3 { return Err("replace() requires 3 arguments".into()); }
            if args.iter().any(|a| a.is_null()) { return Ok(ArenaValue::Null); }
            let s = args[0].to_text(arena).unwrap_or_default();
            let from = args[1].to_text(arena).unwrap_or_default();
            let to = args[2].to_text(arena).unwrap_or_default();
            Ok(ArenaValue::Text(arena.alloc_str(&s.replace(&from, &to))))
        }
        "left" => {
            if args.len() != 2 { return Err("left() requires 2 arguments".into()); }
            if args[0].is_null() { return Ok(ArenaValue::Null); }
            let s = args[0].to_text(arena).unwrap_or_default();
            let n = match &args[1] {
                ArenaValue::Int(i) => *i as usize,
                _ => return Err("left() requires integer second argument".into()),
            };
            let result: String = s.chars().take(n).collect();
            Ok(ArenaValue::Text(arena.alloc_str(&result)))
        }
        "right" => {
            if args.len() != 2 { return Err("right() requires 2 arguments".into()); }
            if args[0].is_null() { return Ok(ArenaValue::Null); }
            let s = args[0].to_text(arena).unwrap_or_default();
            let n = match &args[1] {
                ArenaValue::Int(i) => *i as usize,
                _ => return Err("right() requires integer second argument".into()),
            };
            let chars: Vec<char> = s.chars().collect();
            let start = chars.len().saturating_sub(n);
            let result: String = chars[start..].iter().collect();
            Ok(ArenaValue::Text(arena.alloc_str(&result)))
        }
        // ── Math functions ──────────────────────────────────────
        "ceil" | "ceiling" => match args.first() {
            Some(ArenaValue::Int(n)) => Ok(ArenaValue::Int(*n)),
            Some(ArenaValue::Float(f)) => Ok(ArenaValue::Int(f.ceil() as i64)),
            Some(ArenaValue::Null) => Ok(ArenaValue::Null),
            _ => Err("ceil() requires numeric argument".into()),
        },
        "floor" => match args.first() {
            Some(ArenaValue::Int(n)) => Ok(ArenaValue::Int(*n)),
            Some(ArenaValue::Float(f)) => Ok(ArenaValue::Int(f.floor() as i64)),
            Some(ArenaValue::Null) => Ok(ArenaValue::Null),
            _ => Err("floor() requires numeric argument".into()),
        },
        "round" => {
            let val = args.first().ok_or("round() requires at least 1 argument")?;
            if val.is_null() { return Ok(ArenaValue::Null); }
            let decimals = match args.get(1) {
                Some(ArenaValue::Int(d)) => *d as i32,
                None => 0,
                _ => return Err("round() second argument must be integer".into()),
            };
            let f = match val {
                ArenaValue::Int(n) => *n as f64,
                ArenaValue::Float(f) => *f,
                _ => return Err("round() requires numeric argument".into()),
            };
            if decimals == 0 {
                Ok(ArenaValue::Int(f.round() as i64))
            } else {
                let factor = 10f64.powi(decimals);
                let rounded = (f * factor).round() / factor;
                Ok(ArenaValue::Float(rounded))
            }
        }
        "mod" => {
            if args.len() != 2 { return Err("mod() requires 2 arguments".into()); }
            eval_arithmetic("%", &args[0], &args[1], arena)
        }
        "power" | "pow" => {
            if args.len() != 2 { return Err("power() requires 2 arguments".into()); }
            if args[0].is_null() || args[1].is_null() { return Ok(ArenaValue::Null); }
            let base = match &args[0] {
                ArenaValue::Int(n) => *n as f64,
                ArenaValue::Float(f) => *f,
                _ => return Err("power() requires numeric arguments".into()),
            };
            let exp = match &args[1] {
                ArenaValue::Int(n) => *n as f64,
                ArenaValue::Float(f) => *f,
                _ => return Err("power() requires numeric arguments".into()),
            };
            let result = base.powf(exp);
            // Return Int if result is a whole number and fits
            if result == result.trunc() && result.is_finite() && result.abs() < i64::MAX as f64 {
                Ok(ArenaValue::Int(result as i64))
            } else {
                Ok(ArenaValue::Float(result))
            }
        }
        "sqrt" => match args.first() {
            Some(ArenaValue::Int(n)) => {
                let result = (*n as f64).sqrt();
                if result == result.trunc() { Ok(ArenaValue::Int(result as i64)) }
                else { Ok(ArenaValue::Float(result)) }
            }
            Some(ArenaValue::Float(f)) => Ok(ArenaValue::Float(f.sqrt())),
            Some(ArenaValue::Null) => Ok(ArenaValue::Null),
            _ => Err("sqrt() requires numeric argument".into()),
        },
        _ => Err(format!("function {}() does not exist", name)),
    }
}

fn eval_where(
    where_clause: &Option<Box<pg_query::protobuf::Node>>,
    row: &[ArenaValue],
    ctx: &JoinContext,
    arena: &mut QueryArena,
) -> Result<bool, String> {
    match where_clause {
        Some(wc) => match wc.node.as_ref() {
            Some(expr) => match eval_expr(expr, row, ctx, arena)? {
                ArenaValue::Bool(b) => Ok(b),
                ArenaValue::Null => Ok(false), // NULL in WHERE filters out the row
                _ => Err("WHERE clause must return boolean".into()),
            },
            None => Ok(true),
        },
        None => Ok(true),
    }
}

/// Legacy eval_where for DML (DELETE/UPDATE) paths that use Value rows.
/// Creates a local arena for the expression evaluation.
/// DML bridge: evaluate WHERE clause on Value rows using a shared arena.
/// The arena is reused across rows to avoid per-row 4KB allocation.
fn eval_where_value(
    where_clause: &Option<Box<pg_query::protobuf::Node>>,
    row: &[Value],
    ctx: &JoinContext,
    arena: &mut QueryArena,
) -> Result<bool, String> {
    match where_clause {
        Some(wc) => match wc.node.as_ref() {
            Some(expr) => {
                let arena_row: Vec<ArenaValue> = row.iter().map(|v| ArenaValue::from_value(v, arena)).collect();
                match eval_expr(expr, &arena_row, ctx, arena)? {
                    ArenaValue::Bool(b) => Ok(b),
                    ArenaValue::Null => Ok(false),
                    _ => Err("WHERE clause must return boolean".into()),
                }
            }
            None => Ok(true),
        },
        None => Ok(true),
    }
}

/// DML bridge: evaluate expression on Value rows using a shared arena.
fn eval_expr_value(node: &NodeEnum, row: &[Value], ctx: &JoinContext, arena: &mut QueryArena) -> Result<Value, String> {
    let arena_row: Vec<ArenaValue> = row.iter().map(|v| ArenaValue::from_value(v, arena)).collect();
    let result = eval_expr(node, &arena_row, ctx, arena)?;
    Ok(result.to_value(arena))
}

// ── Fast equality filter (Principle 4: vectorized WHERE) ─────────────

/// Bypass eval_expr for the most common WHERE pattern: `column = constant`.
/// Direct column-index comparison with zero AST walking overhead.
struct FastEqualityFilter {
    col_idx: usize,
    value: ArenaValue,
}

impl FastEqualityFilter {
    #[inline(always)]
    fn matches(&self, row: &[ArenaValue], arena: &QueryArena) -> bool {
        if self.col_idx >= row.len() { return false; }
        let v = &row[self.col_idx];
        v.eq_with(&self.value, arena) || v.compare(&self.value, arena) == Some(std::cmp::Ordering::Equal)
    }

    /// Match against Value rows (for scan_with inside storage lock).
    /// Zero allocation: compares Value directly against ArenaValue const.
    #[inline(always)]
    fn matches_value(&self, row: &[Value], arena: &QueryArena) -> bool {
        if self.col_idx >= row.len() { return false; }
        let v = &row[self.col_idx];
        match (v, &self.value) {
            (Value::Int(a), ArenaValue::Int(b)) => *a == *b,
            (Value::Float(a), ArenaValue::Float(b)) => *a == *b,
            (Value::Bool(a), ArenaValue::Bool(b)) => *a == *b,
            (Value::Int(a), ArenaValue::Float(b)) => (*a as f64) == *b,
            (Value::Float(a), ArenaValue::Int(b)) => *a == (*b as f64),
            (Value::Null, _) | (_, ArenaValue::Null) => false,
            (Value::Text(a), ArenaValue::Text(b)) => a.as_ref() == arena.get_str(*b),
            (Value::Vector(a), ArenaValue::Vector(b)) => a.as_slice() == arena.get_vec(*b),
            (Value::Bytea(a), ArenaValue::Bytea(b)) => {
                let bs = b.offset as usize;
                a.as_slice() == &arena.bytes_ref()[bs..bs + b.len as usize]
            }
            _ => false,
        }
    }
}

/// Try to extract a fast equality filter from a WHERE clause.
/// Returns `None` for anything other than simple `ColumnRef = AConst` patterns,
/// falling through to the generic eval_where loop.
fn try_fast_equality_filter(
    where_clause: &Option<Box<pg_query::protobuf::Node>>,
    ctx: &JoinContext,
    arena: &mut QueryArena,
) -> Option<FastEqualityFilter> {
    let wc = where_clause.as_ref()?;
    let node = wc.node.as_ref()?;

    if let NodeEnum::AExpr(expr) = node {
        // Must be AEXPR_OP with "=" operator
        let op = extract_op_name(&expr.name).ok()?;
        if op != "=" { return None; }

        let left = expr.lexpr.as_ref()?.node.as_ref()?;
        let right = expr.rexpr.as_ref()?.node.as_ref()?;

        // Pattern: ColumnRef = Constant
        if let NodeEnum::ColumnRef(cref) = left {
            let col_idx = resolve_column(cref, ctx).ok()?;
            let val = eval_const(Some(right));
            if matches!(val, Value::Null) { return None; } // NULL = x is never true
            let value = ArenaValue::from_value(&val, arena);
            return Some(FastEqualityFilter { col_idx, value });
        }
        // Pattern: Constant = ColumnRef
        if let NodeEnum::ColumnRef(cref) = right {
            let col_idx = resolve_column(cref, ctx).ok()?;
            let val = eval_const(Some(left));
            if matches!(val, Value::Null) { return None; }
            let value = ArenaValue::from_value(&val, arena);
            return Some(FastEqualityFilter { col_idx, value });
        }
    }
    None
}

// ── Helper extractors ─────────────────────────────────────────────────

fn extract_op_name(name_nodes: &[pg_query::protobuf::Node]) -> Result<String, String> {
    name_nodes
        .iter()
        .filter_map(|n| n.node.as_ref())
        .filter_map(|n| {
            if let NodeEnum::String(s) = n {
                Some(s.sval.clone())
            } else {
                None
            }
        })
        .next()
        .ok_or_else(|| "missing operator name".into())
}

// ── HNSW KNN detection ────────────────────────────────────────────────

/// Detected KNN query plan for HNSW acceleration.
struct KnnPlan {
    query_vector: Vec<f32>,
    k: usize,
    metric: crate::hnsw::DistanceMetric,
}

/// Detect the pattern: ORDER BY col <-> '[...]' LIMIT K (or <=>, <#>)
/// where col has an HNSW index. Returns None if pattern doesn't match.
fn try_detect_knn(
    select: &pg_query::protobuf::SelectStmt,
    ctx: &JoinContext,
    schema: &str,
    table_name: &str,
) -> Option<KnnPlan> {
    // Must have exactly one ORDER BY clause and a LIMIT
    if select.sort_clause.len() != 1 {
        return None;
    }
    let limit_node = select.limit_count.as_ref()?;
    let k = eval_const_i64(limit_node.node.as_ref())? as usize;
    if k == 0 {
        return None;
    }

    // Check the HNSW index exists on this table
    let hnsw_col_idx = storage::has_hnsw_index(schema, table_name)?;

    // Parse the ORDER BY expression — must be a distance operator
    let sort_node = select.sort_clause[0].node.as_ref()?;
    let sb = if let NodeEnum::SortBy(sb) = sort_node {
        sb
    } else {
        return None;
    };

    let inner = sb.node.as_ref()?.node.as_ref()?;
    let a_expr = if let NodeEnum::AExpr(expr) = inner {
        expr
    } else {
        return None;
    };

    let op = extract_op_name(&a_expr.name).ok()?;
    let metric = match op.as_str() {
        "<->" => crate::hnsw::DistanceMetric::L2,
        "<=>" => crate::hnsw::DistanceMetric::Cosine,
        "<#>" => crate::hnsw::DistanceMetric::InnerProduct,
        _ => return None,
    };

    // One side must be a ColumnRef matching the HNSW column, other side a constant vector
    let left = a_expr.lexpr.as_ref()?.node.as_ref()?;
    let right = a_expr.rexpr.as_ref()?.node.as_ref()?;

    let (col_node, vec_node) = if matches!(left, NodeEnum::ColumnRef(_)) {
        (left, right)
    } else if matches!(right, NodeEnum::ColumnRef(_)) {
        (right, left)
    } else {
        return None;
    };

    // Verify the column matches the HNSW-indexed column
    if let NodeEnum::ColumnRef(cref) = col_node {
        let col_idx = resolve_column(cref, ctx).ok()?;
        if col_idx != hnsw_col_idx {
            return None;
        }
    } else {
        return None;
    }

    // Extract the constant vector
    let query_vector = extract_const_vector(vec_node)?;

    Some(KnnPlan { query_vector, k, metric })
}

/// Extract a constant vector from an AST node (string literal like '[1.0, 2.0]').
fn extract_const_vector(node: &NodeEnum) -> Option<Vec<f32>> {
    match node {
        NodeEnum::AConst(ac) => {
            if let Some(pg_query::protobuf::a_const::Val::Sval(s)) = &ac.val {
                let trimmed = s.sval.trim();
                if trimmed.starts_with('[') && trimmed.ends_with(']') {
                    let inner = &trimmed[1..trimmed.len() - 1];
                    let parts: Result<Vec<f32>, _> =
                        inner.split(',').map(|p| p.trim().parse::<f32>()).collect();
                    return parts.ok();
                }
            }
            None
        }
        // TypeCast wrapping a string constant
        NodeEnum::TypeCast(tc) => {
            let arg = tc.arg.as_ref()?.node.as_ref()?;
            extract_const_vector(arg)
        }
        _ => None,
    }
}

fn extract_func_name(fc: &pg_query::protobuf::FuncCall) -> String {
    fc.funcname
        .iter()
        .filter_map(|n| n.node.as_ref())
        .filter_map(|n| {
            if let NodeEnum::String(s) = n {
                Some(s.sval.to_lowercase())
            } else {
                None
            }
        })
        .last()
        .unwrap_or_default()
}

fn extract_col_name(cref: &pg_query::protobuf::ColumnRef) -> String {
    cref.fields
        .iter()
        .filter_map(|f| f.node.as_ref())
        .filter_map(|n| {
            if let NodeEnum::String(s) = n {
                Some(s.sval.clone())
            } else {
                None
            }
        })
        .last()
        .unwrap_or_default()
}

fn is_aggregate(name: &str) -> bool {
    matches!(name, "count" | "sum" | "avg" | "min" | "max" | "string_agg" | "bool_and" | "bool_or")
}

fn query_has_aggregates(select: &pg_query::protobuf::SelectStmt) -> bool {
    select.target_list.iter().any(|t| {
        if let Some(NodeEnum::ResTarget(rt)) = t.node.as_ref() {
            if let Some(NodeEnum::FuncCall(fc)) = rt.val.as_ref().and_then(|v| v.node.as_ref()) {
                return is_aggregate(&extract_func_name(fc));
            }
        }
        false
    })
}

// ── FROM clause execution (handles single table, JOINs, implicit joins) ──

fn execute_from(node: &NodeEnum, arena: &mut QueryArena) -> Result<(Vec<Vec<ArenaValue>>, JoinContext), String> {
    match node {
        NodeEnum::RangeVar(rv) => {
            let alias = rv
                .alias
                .as_ref()
                .map(|a| a.aliasname.clone())
                .unwrap_or_else(|| rv.relname.clone());
            let schema = if rv.schemaname.is_empty() {
                "public"
            } else {
                &rv.schemaname
            };
            let table_def = catalog::get_table(schema, &rv.relname)
                .ok_or_else(|| format!("relation \"{}\" does not exist", rv.relname))?;
            // JOIN path: scan and convert to ArenaValue
            let value_rows = storage::scan(schema, &rv.relname)?;
            let rows = rows_to_arena(&value_rows, arena);
            let ncols = table_def.columns.len();
            let ctx = JoinContext {
                sources: vec![JoinSource {
                    alias,
                    table_name: rv.relname.clone(),
                    schema: schema.to_string(),
                    table_def,
                    col_offset: 0,
                }],
                total_columns: ncols,
            };
            Ok((rows, ctx))
        }
        NodeEnum::JoinExpr(je) => {
            let left_node = je
                .larg
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or("JOIN missing left")?;
            let right_node = je
                .rarg
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or("JOIN missing right")?;
            let (left_rows, left_ctx) = execute_from(left_node, arena)?;
            let (right_rows, right_ctx) = execute_from(right_node, arena)?;

            // Merge contexts — shift right offsets
            let left_width = left_ctx.total_columns;
            let right_width = right_ctx.total_columns;
            let mut sources = left_ctx.sources;
            for mut src in right_ctx.sources {
                src.col_offset += left_width;
                sources.push(src);
            }
            let merged = JoinContext {
                total_columns: left_width + right_width,
                sources,
            };

            // USING and NATURAL JOIN not yet supported
            if !je.using_clause.is_empty() {
                return Err("JOIN ... USING is not yet supported; use JOIN ... ON instead".into());
            }
            if je.is_natural {
                return Err("NATURAL JOIN is not yet supported; use JOIN ... ON instead".into());
            }

            let quals = je.quals.as_ref().and_then(|n| n.node.as_ref());

            // MySQL-style: hash join for equi-joins, nested loop for theta/cross.
            let result = match try_equi_hash_join(
                &left_rows, &right_rows, je.jointype,
                quals, &merged, left_width, right_width, arena,
            ) {
                Some(Ok(rows)) => rows,
                Some(Err(e)) => return Err(e),
                None => nested_loop_join(
                    &left_rows, &right_rows, je.jointype,
                    quals, &merged, left_width, right_width, arena,
                )?,
            };

            Ok((result, merged))
        }
        NodeEnum::RangeSubselect(rs) => {
            let inner = rs
                .subquery
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .ok_or("RangeSubselect missing subquery")?;

            let NodeEnum::SelectStmt(sel) = inner else {
                return Err("subquery in FROM is not a SELECT".into());
            };

            let (inner_cols, rows) = exec_select_raw(sel, None, arena)?;
            let alias = rs
                .alias
                .as_ref()
                .map(|a| a.aliasname.clone())
                .ok_or("subquery in FROM must have an alias")?;

            let columns: Vec<Column> = inner_cols
                .iter()
                .map(|(name, oid)| Column {
                    name: name.clone(),
                    type_oid: TypeOid::from_oid(*oid),
                    nullable: true,
                    primary_key: false,
                    unique: false,
                    default_expr: None,
                })
                .collect();

            let ncols = columns.len();
            let table_def = Table {
                name: alias.clone(),
                schema: String::new(),
                columns,
            };

            let ctx = JoinContext {
                sources: vec![JoinSource {
                    alias,
                    table_name: "subquery".into(),
                    schema: String::new(),
                    table_def,
                    col_offset: 0,
                }],
                total_columns: ncols,
            };

            Ok((rows, ctx))
        }
        _ => Err("unsupported FROM clause node".into()),
    }
}

/// Try to execute an equi-join as a hash join (O(N+M)).
/// Returns None if the ON clause isn't a simple equi-join.
/// Returns Some(Ok(rows)) on success, Some(Err) on execution error.
fn try_equi_hash_join(
    left_rows: &[Vec<ArenaValue>],
    right_rows: &[Vec<ArenaValue>],
    join_type: i32,
    quals: Option<&NodeEnum>,
    ctx: &JoinContext,
    left_width: usize,
    right_width: usize,
    arena: &mut QueryArena,
) -> Option<Result<Vec<Vec<ArenaValue>>, String>> {
    // Extract equi-join key columns from the ON clause
    let quals = quals?;
    let (left_col, right_col) = extract_equi_cols(quals, ctx)?;

    Some(execute_hash_join(
        left_rows, right_rows, join_type, ctx,
        left_width, right_width, left_col, right_col, arena,
    ))
}

/// Extract the column indices for a simple equi-join: ON a.x = b.y
fn extract_equi_cols(quals: &NodeEnum, ctx: &JoinContext) -> Option<(usize, usize)> {
    match quals {
        NodeEnum::AExpr(expr) => {
            let op = expr.name.iter()
                .filter_map(|n| n.node.as_ref())
                .filter_map(|n| if let NodeEnum::String(s) = n { Some(s.sval.as_str()) } else { None })
                .next()?;
            if op != "=" { return None; }

            let left_node = expr.lexpr.as_ref()?.node.as_ref()?;
            let right_node = expr.rexpr.as_ref()?.node.as_ref()?;

            if let (NodeEnum::ColumnRef(lcref), NodeEnum::ColumnRef(rcref)) = (left_node, right_node) {
                let li = resolve_column(lcref, ctx).ok()?;
                let ri = resolve_column(rcref, ctx).ok()?;
                Some((li, ri))
            } else {
                None
            }
        }
        // AND: compound conditions — fall back to nested loop for correctness
        NodeEnum::BoolExpr(be) if be.boolop == pg_query::protobuf::BoolExprType::AndExpr as i32 => {
            None  // Multiple conditions require full evaluation; use nested loop
        }
        _ => None,
    }
}

/// Hash join: build hash table on right side, probe with left side. O(N+M).
fn execute_hash_join(
    left_rows: &[Vec<ArenaValue>],
    right_rows: &[Vec<ArenaValue>],
    join_type: i32,
    ctx: &JoinContext,
    left_width: usize,
    right_width: usize,
    left_key_col: usize,
    right_key_col: usize,
    arena: &mut QueryArena,
) -> Result<Vec<Vec<ArenaValue>>, String> {
    let null_right = vec![ArenaValue::Null; right_width];
    let null_left = vec![ArenaValue::Null; left_width];

    let is_inner = join_type == pg_query::protobuf::JoinType::JoinInner as i32
        || join_type == pg_query::protobuf::JoinType::Undefined as i32;
    let is_left = join_type == pg_query::protobuf::JoinType::JoinLeft as i32;
    let is_right = join_type == pg_query::protobuf::JoinType::JoinRight as i32;
    let is_full = join_type == pg_query::protobuf::JoinType::JoinFull as i32;

    if !is_inner && !is_left && !is_right && !is_full {
        return Err(format!("unsupported JOIN type: {}", join_type));
    }

    // Ensure left_key is in [0..left_width) and right_key is in [left_width..)
    // The ON clause might have them in either order.
    let (left_key_col, right_key_col) = if left_key_col < left_width && right_key_col >= left_width {
        (left_key_col, right_key_col)
    } else if right_key_col < left_width && left_key_col >= left_width {
        (right_key_col, left_key_col) // swap
    } else {
        // Both on same side — can't hash join, return error
        return Err("JOIN ON condition references columns from the same table on both sides".into());
    };
    let right_local_key = right_key_col - left_width;

    // BUILD phase: hash table on right side using u64 hash (avoids borrow issues)
    // Skip NULL keys — in SQL, NULL = NULL is FALSE in JOIN ON conditions.
    let mut hash_table: HashMap<u64, Vec<usize>> = HashMap::new();
    for (i, row) in right_rows.iter().enumerate() {
        if right_local_key < row.len() {
            let key = row[right_local_key];
            if key.is_null() {
                continue; // NULL never matches in JOIN ON
            }
            let mut hasher = DefaultHasher::new();
            key.hash_with(arena, &mut hasher);
            let h = hasher.finish();
            hash_table.entry(h).or_default().push(i);
        }
    }

    // Pre-estimate result size: for INNER join, assume ~avg matches per left row
    let est_size = if is_inner { left_rows.len() * 2 } else { left_rows.len() };
    let mut result = Vec::with_capacity(est_size);
    let combined_width = left_width + right_width;
    let mut right_matched = if is_right || is_full {
        vec![false; right_rows.len()]
    } else {
        Vec::new()
    };

    // PROBE phase: for each left row, look up matching right rows
    for left in left_rows {
        if left_key_col >= left.len() { continue; }
        let key = left[left_key_col];

        // NULL key never matches — treat as unmatched for LEFT/FULL joins
        if key.is_null() {
            if is_left || is_full {
                let mut row = Vec::with_capacity(combined_width);
                row.extend_from_slice(left);
                row.extend_from_slice(&null_right);
                result.push(row);
            }
            continue;
        }

        let mut left_matched = false;

        let mut hasher = DefaultHasher::new();
        key.hash_with(arena, &mut hasher);
        let h = hasher.finish();

        if let Some(indices) = hash_table.get(&h) {
            for &ri in indices {
                // Verify equality (hash collision check)
                if !key.eq_with(&right_rows[ri][right_local_key], arena) {
                    continue;
                }
                left_matched = true;
                if !right_matched.is_empty() {
                    right_matched[ri] = true;
                }
                let mut combined = Vec::with_capacity(combined_width);
                combined.extend_from_slice(left);
                combined.extend_from_slice(&right_rows[ri]);
                result.push(combined);
            }
        }

        // LEFT/FULL: emit unmatched left row with NULLs
        if !left_matched && (is_left || is_full) {
            let mut row = Vec::with_capacity(combined_width);
            row.extend_from_slice(left);
            row.extend_from_slice(&null_right);
            result.push(row);
        }
    }

    // RIGHT/FULL: emit unmatched right rows
    if is_right || is_full {
        for (ri, right) in right_rows.iter().enumerate() {
            if !right_matched[ri] {
                let mut row = null_left.clone();
                row.extend_from_slice(right);
                result.push(row);
            }
        }
    }

    Ok(result)
}

fn nested_loop_join(
    left_rows: &[Vec<ArenaValue>],
    right_rows: &[Vec<ArenaValue>],
    join_type: i32,
    quals: Option<&NodeEnum>,
    ctx: &JoinContext,
    left_width: usize,
    right_width: usize,
    arena: &mut QueryArena,
) -> Result<Vec<Vec<ArenaValue>>, String> {
    let null_right = vec![ArenaValue::Null; right_width];
    let null_left = vec![ArenaValue::Null; left_width];
    let mut result = Vec::with_capacity(left_rows.len());
    let is_inner = join_type == pg_query::protobuf::JoinType::JoinInner as i32
        || join_type == pg_query::protobuf::JoinType::Undefined as i32;
    let is_left = join_type == pg_query::protobuf::JoinType::JoinLeft as i32;
    let is_right = join_type == pg_query::protobuf::JoinType::JoinRight as i32;
    let is_full = join_type == pg_query::protobuf::JoinType::JoinFull as i32;

    if !is_inner && !is_left && !is_right && !is_full {
        return Err(format!("unsupported JOIN type: {}", join_type));
    }
    let mut right_matched = if is_right || is_full {
        vec![false; right_rows.len()]
    } else {
        Vec::new()
    };

    for left in left_rows {
        let mut left_matched = false;
        for (ri, right) in right_rows.iter().enumerate() {
            let mut combined = Vec::with_capacity(left_width + right_width);
            combined.extend_from_slice(left);
            combined.extend_from_slice(right);

            let matches = match quals {
                Some(q) => matches!(eval_expr(q, &combined, ctx, arena)?, ArenaValue::Bool(true)),
                None => true, // CROSS JOIN
            };

            if matches {
                left_matched = true;
                if !right_matched.is_empty() {
                    right_matched[ri] = true;
                }
                result.push(combined);
            }
        }

        if !left_matched && (is_left || is_full) {
            let mut row = left.clone();
            row.extend_from_slice(&null_right);
            result.push(row);
        }
    }

    if is_right || is_full {
        for (ri, right) in right_rows.iter().enumerate() {
            if !right_matched[ri] {
                let mut row = null_left.clone();
                row.extend_from_slice(right);
                result.push(row);
            }
        }
    }

    Ok(result)
}

/// Execute the full FROM clause, handling multiple comma-separated tables.
fn execute_from_clause(
    from_clause: &[pg_query::protobuf::Node],
    arena: &mut QueryArena,
) -> Result<(Vec<Vec<ArenaValue>>, JoinContext), String> {
    let first = from_clause
        .first()
        .and_then(|n| n.node.as_ref())
        .ok_or("missing FROM")?;
    let (mut rows, mut ctx) = execute_from(first, arena)?;

    // Implicit cross join for FROM a, b, c ...
    for from_node in &from_clause[1..] {
        let node = from_node.node.as_ref().ok_or("missing FROM node")?;
        let (right_rows, right_ctx) = execute_from(node, arena)?;
        let left_width = ctx.total_columns;
        let right_width = right_ctx.total_columns;

        // Merge contexts
        let mut sources = ctx.sources;
        for mut src in right_ctx.sources {
            src.col_offset += left_width;
            sources.push(src);
        }
        ctx = JoinContext {
            total_columns: left_width + right_width,
            sources,
        };

        // Cross product
        let mut new_rows = Vec::with_capacity(rows.len() * right_rows.len());
        for left in &rows {
            for right in &right_rows {
                let mut combined = Vec::with_capacity(left_width + right_width);
                combined.extend_from_slice(left);
                combined.extend_from_slice(right);
                new_rows.push(combined);
            }
        }
        rows = new_rows;
    }

    Ok((rows, ctx))
}

// ── SELECT target resolution ──────────────────────────────────────────

enum SelectTarget {
    Column { name: String, idx: usize },
    Expr { name: String, expr: NodeEnum },
}

fn resolve_targets(
    select: &pg_query::protobuf::SelectStmt,
    ctx: &JoinContext,
) -> Result<Vec<SelectTarget>, String> {
    let mut targets = Vec::new();

    for target in &select.target_list {
        if let Some(NodeEnum::ResTarget(rt)) = target.node.as_ref() {
            let val_node = rt.val.as_ref().and_then(|v| v.node.as_ref());
            match val_node {
                Some(NodeEnum::ColumnRef(cref)) => {
                    // Check for star variants
                    let has_star = cref
                        .fields
                        .iter()
                        .any(|f| matches!(f.node.as_ref(), Some(NodeEnum::AStar(_))));

                    if has_star {
                        // Check if qualified star (e.g., u.*)
                        let string_fields = extract_string_fields(cref);
                        if string_fields.is_empty() {
                            // Unqualified *: expand all sources
                            for src in &ctx.sources {
                                for (i, col) in src.table_def.columns.iter().enumerate() {
                                    targets.push(SelectTarget::Column {
                                        name: col.name.clone(),
                                        idx: src.col_offset + i,
                                    });
                                }
                            }
                        } else {
                            // Qualified star: e.g., u.*
                            let qualifier = &string_fields[0];
                            let matches: Vec<_> = ctx.sources.iter()
                                .filter(|s| s.alias == *qualifier || s.table_name == *qualifier)
                                .collect();
                            let src = match matches.len() {
                                0 => return Err(format!(
                                    "missing FROM-clause entry for table \"{}\"", qualifier
                                )),
                                1 => matches[0],
                                _ => return Err(format!(
                                    "table reference \"{}\" is ambiguous", qualifier
                                )),
                            };
                            for (i, col) in src.table_def.columns.iter().enumerate() {
                                targets.push(SelectTarget::Column {
                                    name: col.name.clone(),
                                    idx: src.col_offset + i,
                                });
                            }
                        }
                    } else {
                        let idx = resolve_column(cref, ctx)?;
                        let alias = if rt.name.is_empty() {
                            extract_col_name(cref)
                        } else {
                            rt.name.clone()
                        };
                        targets.push(SelectTarget::Column { name: alias, idx });
                    }
                }
                Some(expr) => {
                    let alias = if rt.name.is_empty() {
                        "?column?".to_string()
                    } else {
                        rt.name.clone()
                    };
                    targets.push(SelectTarget::Expr {
                        name: alias,
                        expr: expr.clone(),
                    });
                }
                None => {
                    targets.push(SelectTarget::Column {
                        name: "?column?".into(),
                        idx: 0,
                    });
                }
            }
        }
    }

    Ok(targets)
}

// ── SELECT execution ──────────────────────────────────────────────────

/// Internal: execute a SELECT and return raw Value rows + column metadata.
/// Used by subqueries and as the base for exec_select.
fn exec_select_raw(
    select: &pg_query::protobuf::SelectStmt,
    outer: Option<(&[ArenaValue], &JoinContext)>,
    arena: &mut QueryArena,
) -> Result<(Vec<(String, i32)>, Vec<Vec<ArenaValue>>), String> {
    // Handle UNION / INTERSECT / EXCEPT (set operations)
    let set_op = select.op;
    if set_op == pg_query::protobuf::SetOperation::SetopUnion as i32
        || set_op == pg_query::protobuf::SetOperation::SetopIntersect as i32
        || set_op == pg_query::protobuf::SetOperation::SetopExcept as i32
    {
        let larg = select.larg.as_ref().ok_or("set operation missing left SELECT")?;
        let rarg = select.rarg.as_ref().ok_or("set operation missing right SELECT")?;
        let (lcols, lrows) = exec_select_raw(larg, outer, arena)?;
        let (_, rrows) = exec_select_raw(rarg, outer, arena)?;

        let mut result_rows = if set_op == pg_query::protobuf::SetOperation::SetopUnion as i32 {
            let mut combined = lrows;
            combined.extend(rrows);
            if !select.all {
                combined = dedup_distinct(&[pg_query::protobuf::Node { node: None }], combined, arena);
            }
            combined
        } else if set_op == pg_query::protobuf::SetOperation::SetopIntersect as i32 {
            lrows.into_iter().filter(|lrow| {
                rrows.iter().any(|rrow|
                    lrow.len() == rrow.len() && lrow.iter().zip(rrow.iter()).all(|(a, b)| a.eq_with(b, arena))
                )
            }).collect()
        } else {
            // EXCEPT
            lrows.into_iter().filter(|lrow| {
                !rrows.iter().any(|rrow|
                    lrow.len() == rrow.len() && lrow.iter().zip(rrow.iter()).all(|(a, b)| a.eq_with(b, arena))
                )
            }).collect()
        };

        // Apply ORDER BY / LIMIT on the combined result
        if !select.sort_clause.is_empty() || select.limit_count.is_some() || select.limit_offset.is_some() {
            if !select.sort_clause.is_empty() {
                // Resolve ORDER BY using column names from the result set
                let mut sort_keys = Vec::new();
                for sort_node in &select.sort_clause {
                    if let Some(NodeEnum::SortBy(sb)) = sort_node.node.as_ref() {
                        let ascending = sb.sortby_dir != pg_query::protobuf::SortByDir::SortbyDesc as i32;
                        let nulls_first = sb.sortby_nulls == pg_query::protobuf::SortByNulls::SortbyNullsFirst as i32;
                        if let Some(ref node) = sb.node {
                            if let Some(NodeEnum::ColumnRef(cref)) = node.node.as_ref() {
                                let col_name = cref.fields.iter()
                                    .filter_map(|f| f.node.as_ref())
                                    .filter_map(|n| if let NodeEnum::String(s) = n { Some(s.sval.as_str()) } else { None })
                                    .last().unwrap_or("");
                                let idx = lcols.iter().position(|(n, _)| n == col_name)
                                    .ok_or_else(|| format!("column \"{}\" does not exist in UNION result", col_name))?;
                                sort_keys.push(SortKey { col_idx: idx, ascending, nulls_first });
                            } else if let Some(NodeEnum::AConst(ac)) = node.node.as_ref() {
                                // ORDER BY 1, 2, etc. — positional
                                if let Some(pg_query::protobuf::a_const::Val::Ival(iv)) = &ac.val {
                                    sort_keys.push(SortKey { col_idx: (iv.ival as usize) - 1, ascending, nulls_first });
                                }
                            }
                        }
                    }
                }
                result_rows.sort_by(|a, b| compare_rows(&sort_keys, a, b, arena));
            }
            if let Some(ref offset_node) = select.limit_offset {
                if let Some(n) = eval_const_i64(offset_node.node.as_ref()) {
                    let n = n.max(0) as usize;
                    if n >= result_rows.len() { result_rows.clear(); }
                    else { result_rows.drain(0..n); }
                }
            }
            if let Some(ref limit_node) = select.limit_count {
                if let Some(n) = eval_const_i64(limit_node.node.as_ref()) {
                    result_rows.truncate(n.max(0) as usize);
                }
            }
        }

        return Ok((lcols, result_rows));
    }

    if select.from_clause.is_empty() {
        return exec_select_raw_no_from(select, outer, arena);
    }

    // Fast path: single-table query with no outer context — use scan_with
    // to filter inside the lock and clone only matching rows.
    if select.from_clause.len() == 1 && outer.is_none() {
        if let Some(NodeEnum::RangeVar(rv)) = select.from_clause[0].node.as_ref() {
            let schema = if rv.schemaname.is_empty() { "public" } else { &rv.schemaname };
            let table_def = catalog::get_table(schema, &rv.relname)
                .ok_or_else(|| format!("relation \"{}\" does not exist", rv.relname))?;
            let alias = rv.alias.as_ref()
                .map(|a| a.aliasname.clone())
                .unwrap_or_else(|| rv.relname.clone());
            let ncols = table_def.columns.len();
            let ctx = JoinContext {
                sources: vec![JoinSource {
                    alias,
                    table_name: rv.relname.clone(),
                    #[allow(dead_code)]
                    schema: schema.to_string(),
                    table_def,
                    col_offset: 0,
                }],
                total_columns: ncols,
            };

            // HNSW fast path: detect ORDER BY <distance_op> LIMIT K pattern
            if select.where_clause.is_none() && !select.sort_clause.is_empty() {
                if let Some(knn) = try_detect_knn(select, &ctx, schema, &rv.relname) {
                    // Ensure HNSW index uses the matching metric
                    if let Some(vec_col) = ctx.sources[0].table_def.columns.iter().position(|c| c.type_oid == TypeOid::Vector) {
                        let _ = storage::ensure_hnsw_index(schema, &rv.relname, vec_col, knn.metric);
                    }
                    // Account for OFFSET: request k + offset from HNSW
                    let offset = select.limit_offset.as_ref()
                        .and_then(|n| eval_const_i64(n.node.as_ref()))
                        .unwrap_or(0).max(0) as usize;
                    let hnsw_results = storage::hnsw_search(
                        schema, &rv.relname, &knn.query_vector, knn.k + offset,
                    )?;
                    let row_ids: Vec<usize> = hnsw_results.iter()
                        .skip(offset)
                        .map(|(_, row_id)| *row_id)
                        .collect();
                    let value_rows = storage::get_rows_by_ids(schema, &rv.relname, &row_ids)?;
                    let rows = rows_to_arena(&value_rows, arena);

                    // Project results (skip ORDER BY / LIMIT in post_filter since
                    // HNSW already returns sorted top-K)
                    let targets = resolve_targets(select, &ctx)?;
                    let columns: Vec<(String, i32)> = targets
                        .iter()
                        .map(|t| match t {
                            SelectTarget::Column { name, idx } => {
                                Ok((name.clone(), column_type_oid(*idx, &ctx)?))
                            }
                            SelectTarget::Expr { name, .. } => {
                                Ok((name.clone(), TypeOid::Text.oid()))
                            }
                        })
                        .collect::<Result<Vec<_>, String>>()?;

                    let mut result_rows = Vec::new();
                    for row in &rows {
                        let mut result_row = Vec::new();
                        for t in &targets {
                            let val = match t {
                                SelectTarget::Column { idx, .. } => {
                                    if *idx < row.len() {
                                        row[*idx] // ArenaValue is Copy
                                    } else {
                                        ArenaValue::Null
                                    }
                                }
                                SelectTarget::Expr { expr, .. } => {
                                    eval_expr(expr, row, &ctx, arena)?
                                }
                            };
                            result_row.push(val);
                        }
                        result_rows.push(result_row);
                    }

                    return Ok((columns, result_rows));
                }
            }

            // Filter inside the read lock — clone only matching rows.

            let fast_filter = try_fast_equality_filter(&select.where_clause, &ctx, arena);
            let value_rows = storage::scan_with(schema, &rv.relname, |all_rows| {
                let mut filtered = Vec::new();
                if let Some(ref ff) = fast_filter {
                    for row in all_rows {
                        if ff.matches_value(row, arena) {
                            filtered.push(row.clone());
                        }
                    }
                } else {
                    // Single arena shared across all rows (not per-row).
                    let mut scan_arena = QueryArena::new();
                    for row in all_rows {
                        if eval_where_value(&select.where_clause, row, &ctx, &mut scan_arena)? {
                            filtered.push(row.clone());
                        }
                    }
                }
                Ok(filtered)
            })?;
            let rows = rows_to_arena(&value_rows, arena);

            let result = exec_select_raw_post_filter(select, ctx, rows, 0, arena);
            return result;
        }
    }

    // General path: JOINs, implicit joins, subqueries, correlated
    let (all_rows, inner_ctx) = execute_from_clause(&select.from_clause, arena)?;

    // If we have an outer context (correlated subquery), merge it
    let (eval_rows, merged_ctx, outer_width) = if let Some((outer_row, outer_ctx)) = outer {
        let outer_width = outer_ctx.total_columns;
        // Merge: outer sources first, then inner sources (shifted)
        let mut sources: Vec<JoinSource> = Vec::new();
        for src in &outer_ctx.sources {
            sources.push(JoinSource {
                alias: src.alias.clone(),
                table_name: src.table_name.clone(),
                schema: src.schema.clone(),
                table_def: src.table_def.clone(),
                col_offset: src.col_offset,
            });
        }
        for src in inner_ctx.sources {
            sources.push(JoinSource {
                alias: src.alias,
                table_name: src.table_name,
                schema: src.schema,
                table_def: src.table_def,
                col_offset: src.col_offset + outer_width,
            });
        }
        let merged = JoinContext {
            total_columns: outer_width + inner_ctx.total_columns,
            sources,
        };
        // Prepend outer row to each inner row
        let rows: Vec<Vec<ArenaValue>> = all_rows
            .into_iter()
            .map(|inner_row| {
                let mut combined = outer_row.to_vec();
                combined.extend(inner_row);
                combined
            })
            .collect();
        (rows, merged, outer_width)
    } else {
        (all_rows, inner_ctx, 0)
    };

    // WHERE filter
    let mut rows = Vec::new();
    for row in eval_rows {
        if eval_where(&select.where_clause, &row, &merged_ctx, arena)? {
            rows.push(row);
        }
    }

    return exec_select_raw_post_filter(select, merged_ctx, rows, outer_width, arena);
}

/// Shared post-filter logic: aggregates, ORDER BY, LIMIT, projection.
fn exec_select_raw_post_filter(
    select: &pg_query::protobuf::SelectStmt,
    merged_ctx: JoinContext,
    mut rows: Vec<Vec<ArenaValue>>,
    _outer_width: usize,
    arena: &mut QueryArena,
) -> Result<(Vec<(String, i32)>, Vec<Vec<ArenaValue>>), String> {
    // Route to aggregate path if needed
    if query_has_aggregates(select) || !select.group_clause.is_empty() {
        let agg_result = exec_select_aggregate(select, &merged_ctx, rows, arena)?;
        // Convert string rows back to typed Value rows using column OIDs
        let col_oids: Vec<i32> = agg_result.columns.iter().map(|(_, oid)| *oid).collect();
        let mut value_rows: Vec<Vec<ArenaValue>> = agg_result
            .rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .enumerate()
                    .map(|(i, cell)| match cell {
                        None => ArenaValue::Null,
                        Some(s) => {
                            let oid = col_oids.get(i).copied().unwrap_or(25);
                            let v = parse_text_to_value(&s, oid);
                            ArenaValue::from_value(&v, arena)
                        }
                    })
                    .collect()
            })
            .collect();

        // SQL order: DISTINCT → ORDER BY → OFFSET → LIMIT
        value_rows = dedup_distinct(&select.distinct_clause, value_rows, arena);

        if !select.sort_clause.is_empty() {
            let sort_keys = resolve_aggregate_sort_keys(
                &select.sort_clause, &agg_result.columns, &merged_ctx, select,
            )?;
            value_rows.sort_by(|a, b| compare_rows(&sort_keys, a, b, arena));
        }

        if let Some(ref offset_node) = select.limit_offset {
            if let Some(n) = eval_const_i64(offset_node.node.as_ref()) {
                let n = n.max(0) as usize;
                if n >= value_rows.len() {
                    value_rows.clear();
                } else {
                    value_rows.drain(0..n);
                }
            }
        }

        if let Some(ref limit_node) = select.limit_count {
            if let Some(n) = eval_const_i64(limit_node.node.as_ref()) {
                value_rows.truncate(n.max(0) as usize);
            }
        }

        return Ok((agg_result.columns, value_rows));
    }

    // ORDER BY (on full rows before projection)
    if !select.sort_clause.is_empty() {
        let (sort_keys, expr_count) =
            resolve_sort_keys_with_exprs(&select.sort_clause, &merged_ctx, select, &mut rows, arena)?;
        rows.sort_by(|a, b| compare_rows(&sort_keys, a, b, arena));
        if expr_count > 0 {
            for row in rows.iter_mut() {
                row.truncate(row.len() - expr_count);
            }
        }
    }

    // Project before DISTINCT/OFFSET/LIMIT (SQL order: PROJECT → DISTINCT → ORDER BY → OFFSET → LIMIT)
    // ORDER BY already applied above on full rows; now project to output columns
    let targets = resolve_targets(select, &merged_ctx)?;
    let columns: Vec<(String, i32)> = targets
        .iter()
        .map(|t| match t {
            SelectTarget::Column { name, idx } => {
                Ok((name.clone(), column_type_oid(*idx, &merged_ctx)?))
            }
            SelectTarget::Expr { name, .. } => Ok((name.clone(), TypeOid::Text.oid())),
        })
        .collect::<Result<Vec<_>, String>>()?;

    let mut result_rows = Vec::new();
    for row in &rows {
        let mut result_row = Vec::new();
        for t in &targets {
            let val = match t {
                SelectTarget::Column { idx, .. } => {
                    if *idx < row.len() {
                        row[*idx]
                    } else {
                        return Err(format!(
                            "internal error: column index {} out of range for row of width {}",
                            idx, row.len()
                        ));
                    }
                }
                SelectTarget::Expr { expr, .. } => {
                    eval_expr(expr, row, &merged_ctx, arena)?
                }
            };
            result_row.push(val);
        }
        result_rows.push(result_row);
    }

    // SQL order: DISTINCT → OFFSET → LIMIT (after projection and ORDER BY)
    result_rows = dedup_distinct(&select.distinct_clause, result_rows, arena);

    if let Some(ref offset_node) = select.limit_offset {
        if let Some(n) = eval_const_i64(offset_node.node.as_ref()) {
            let n = n.max(0) as usize;
            if n >= result_rows.len() {
                result_rows.clear();
            } else {
                result_rows.drain(0..n);
            }
        }
    }

    if let Some(ref limit_node) = select.limit_count {
        if let Some(n) = eval_const_i64(limit_node.node.as_ref()) {
            result_rows.truncate(n.max(0) as usize);
        }
    }

    Ok((columns, result_rows))
}

/// Handle SELECT with no FROM clause, returning raw Values.
fn exec_select_raw_no_from(
    select: &pg_query::protobuf::SelectStmt,
    outer: Option<(&[ArenaValue], &JoinContext)>,
    arena: &mut QueryArena,
) -> Result<(Vec<(String, i32)>, Vec<Vec<ArenaValue>>), String> {
    let (eval_row, eval_ctx): (Vec<ArenaValue>, JoinContext) = if let Some((outer_row, outer_ctx)) = outer
    {
        let sources: Vec<JoinSource> = outer_ctx
            .sources
            .iter()
            .map(|src| JoinSource {
                alias: src.alias.clone(),
                table_name: src.table_name.clone(),
                schema: src.schema.clone(),
                table_def: src.table_def.clone(),
                col_offset: src.col_offset,
            })
            .collect();
        (
            outer_row.to_vec(),
            JoinContext {
                total_columns: outer_ctx.total_columns,
                sources,
            },
        )
    } else {
        (
            vec![],
            JoinContext {
                sources: vec![],
                total_columns: 0,
            },
        )
    };

    let mut columns = Vec::new();
    let mut row = Vec::new();

    for target in &select.target_list {
        if let Some(NodeEnum::ResTarget(rt)) = target.node.as_ref() {
            let alias = if rt.name.is_empty() {
                "?column?".to_string()
            } else {
                rt.name.clone()
            };
            let val = match rt.val.as_ref().and_then(|v| v.node.as_ref()) {
                Some(expr) => eval_expr(expr, &eval_row, &eval_ctx, arena)?,
                None => ArenaValue::Null,
            };
            columns.push((alias, TypeOid::Text.oid()));
            row.push(val);
        }
    }

    Ok((columns, vec![row]))
}

fn exec_select(
    select: &pg_query::protobuf::SelectStmt,
) -> Result<QueryResult, String> {
    let mut arena = QueryArena::new();
    let (columns, raw_rows) = exec_select_raw(select, None, &mut arena)?;
    let rows: Vec<Vec<Option<String>>> = raw_rows
        .iter()
        .map(|row| row.iter().map(|v| v.to_text(&arena)).collect())
        .collect();
    let count = rows.len();
    Ok(QueryResult {
        tag: format!("SELECT {}", count),
        columns,
        rows,
    })
}

// exec_select_no_from replaced by exec_select_raw_no_from above

// ── ORDER BY helpers ──────────────────────────────────────────────────

struct SortKey {
    col_idx: usize,
    ascending: bool,
    nulls_first: bool,
}

/// Extended ORDER BY resolver that supports arbitrary expressions (e.g. `embedding <-> '[1,0,0]'`).
/// Expression results are appended as temporary columns to each row; the caller must strip them.
/// Returns (sort_keys, number_of_expression_columns_appended).
fn resolve_sort_keys_with_exprs(
    sort_clause: &[pg_query::protobuf::Node],
    ctx: &JoinContext,
    select: &pg_query::protobuf::SelectStmt,
    rows: &mut Vec<Vec<ArenaValue>>,
    arena: &mut QueryArena,
) -> Result<(Vec<SortKey>, usize), String> {
    let mut keys = Vec::new();
    let mut expr_nodes: Vec<(usize, &NodeEnum)> = Vec::new(); // (key_index, expr_node)
    let base_width = if rows.is_empty() {
        ctx.total_columns
    } else {
        rows[0].len()
    };
    let mut next_col = base_width;

    for snode in sort_clause {
        if let Some(NodeEnum::SortBy(sb)) = snode.node.as_ref() {
            let inner = sb.node.as_ref().and_then(|n| n.node.as_ref());
            let col_idx = match inner {
                Some(NodeEnum::ColumnRef(cref)) => resolve_column(cref, ctx)?,
                Some(NodeEnum::AConst(ac)) => {
                    let ordinal = match &ac.val {
                        Some(pg_query::protobuf::a_const::Val::Ival(i)) => i.ival as usize,
                        _ => return Err("invalid ORDER BY ordinal".into()),
                    };
                    resolve_ordinal_to_col_idx(ordinal, select, ctx)?
                }
                Some(expr_node) => {
                    // Arbitrary expression — will be evaluated per-row
                    let idx = next_col;
                    next_col += 1;
                    expr_nodes.push((keys.len(), expr_node));
                    idx
                }
                None => return Err("ORDER BY missing expression".into()),
            };

            let ascending =
                sb.sortby_dir != pg_query::protobuf::SortByDir::SortbyDesc as i32;
            let nulls_first = match sb.sortby_nulls {
                x if x == pg_query::protobuf::SortByNulls::SortbyNullsFirst as i32 => true,
                x if x == pg_query::protobuf::SortByNulls::SortbyNullsLast as i32 => false,
                _ => !ascending,
            };

            keys.push(SortKey {
                col_idx,
                ascending,
                nulls_first,
            });
        }
    }

    let expr_count = next_col - base_width;

    // Evaluate expression ORDER BY keys for every row
    if expr_count > 0 {
        for row in rows.iter_mut() {
            // Pre-extend with Nulls
            row.resize(next_col, ArenaValue::Null);
        }
        // Need to clone expr_nodes data to avoid borrow issues
        let expr_data: Vec<(usize, NodeEnum)> = expr_nodes
            .iter()
            .map(|(ki, node)| (*ki, (*node).clone()))
            .collect();
        for row in rows.iter_mut() {
            for (key_idx, expr_node) in &expr_data {
                let col_idx = keys[*key_idx].col_idx;
                let val = eval_expr(expr_node, row, ctx, arena)?;
                row[col_idx] = val;
            }
        }
    }

    Ok((keys, expr_count))
}

fn resolve_ordinal_to_col_idx(
    ordinal: usize,
    select: &pg_query::protobuf::SelectStmt,
    ctx: &JoinContext,
) -> Result<usize, String> {
    if ordinal == 0 || ordinal > select.target_list.len() {
        return Err(format!("ORDER BY position {} is not in select list", ordinal));
    }
    let target = &select.target_list[ordinal - 1];
    if let Some(NodeEnum::ResTarget(rt)) = target.node.as_ref() {
        if let Some(NodeEnum::ColumnRef(cref)) = rt.val.as_ref().and_then(|v| v.node.as_ref()) {
            return resolve_column(cref, ctx);
        }
    }
    Err("ORDER BY ordinal must reference a column".into())
}

fn compare_rows(keys: &[SortKey], a: &[ArenaValue], b: &[ArenaValue], arena: &QueryArena) -> std::cmp::Ordering {
    for k in keys {
        let va = a.get(k.col_idx).copied().unwrap_or(ArenaValue::Null);
        let vb = b.get(k.col_idx).copied().unwrap_or(ArenaValue::Null);
        let ord = match (va, vb) {
            (ArenaValue::Null, ArenaValue::Null) => std::cmp::Ordering::Equal,
            (ArenaValue::Null, _) => {
                if k.nulls_first {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                }
            }
            (_, ArenaValue::Null) => {
                if k.nulls_first {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            }
            _ => va.compare(&vb, arena).unwrap_or(std::cmp::Ordering::Equal),
        };
        let ord = if k.ascending { ord } else { ord.reverse() };
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    std::cmp::Ordering::Equal
}

fn eval_const_i64(node: Option<&NodeEnum>) -> Option<i64> {
    match eval_const(node) {
        Value::Int(n) => Some(n),
        Value::Float(f) => Some(f as i64),
        _ => None,
    }
}

// ── Aggregate ORDER BY helpers ─────────────────────────────────────────

/// Resolve ORDER BY keys for aggregate query results.
/// Maps sort expressions to indices in the aggregate result columns.
fn resolve_aggregate_sort_keys(
    sort_clause: &[pg_query::protobuf::Node],
    result_columns: &[(String, i32)],
    _source_ctx: &JoinContext,
    select: &pg_query::protobuf::SelectStmt,
) -> Result<Vec<SortKey>, String> {
    let mut keys = Vec::new();

    for snode in sort_clause {
        if let Some(NodeEnum::SortBy(sb)) = snode.node.as_ref() {
            let inner = sb.node.as_ref().and_then(|n| n.node.as_ref());
            let col_idx = match inner {
                Some(NodeEnum::AConst(ac)) => {
                    // Ordinal reference (e.g. ORDER BY 2)
                    let ordinal = match &ac.val {
                        Some(pg_query::protobuf::a_const::Val::Ival(i)) => i.ival as usize,
                        _ => return Err("invalid ORDER BY ordinal".into()),
                    };
                    if ordinal == 0 || ordinal > result_columns.len() {
                        return Err(format!(
                            "ORDER BY position {} is not in select list", ordinal
                        ));
                    }
                    ordinal - 1
                }
                Some(NodeEnum::ColumnRef(cref)) => {
                    // Column name — find in result columns
                    let col_name = extract_col_name(cref);
                    result_columns.iter().position(|(name, _)| {
                        name.eq_ignore_ascii_case(&col_name)
                    }).ok_or_else(|| format!(
                        "column \"{}\" not found in aggregate result", col_name
                    ))?
                }
                Some(NodeEnum::FuncCall(fc)) => {
                    // Aggregate expression in ORDER BY (e.g. ORDER BY SUM(amount))
                    // Match against select target list to find result column index
                    let sort_func_name = extract_func_name(fc);
                    let mut found = None;
                    for (i, target) in select.target_list.iter().enumerate() {
                        if let Some(NodeEnum::ResTarget(rt)) = target.node.as_ref() {
                            if let Some(NodeEnum::FuncCall(tfc)) = rt.val.as_ref().and_then(|v| v.node.as_ref()) {
                                if extract_func_name(tfc) == sort_func_name {
                                    // Compare arguments for match
                                    if fc.args.len() == tfc.args.len() {
                                        found = Some(i);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    found.ok_or_else(|| format!(
                        "aggregate {}() in ORDER BY not found in select list",
                        sort_func_name
                    ))?
                }
                _ => return Err("unsupported ORDER BY expression in aggregate query".into()),
            };

            let ascending =
                sb.sortby_dir != pg_query::protobuf::SortByDir::SortbyDesc as i32;
            let nulls_first = match sb.sortby_nulls {
                x if x == pg_query::protobuf::SortByNulls::SortbyNullsFirst as i32 => true,
                x if x == pg_query::protobuf::SortByNulls::SortbyNullsLast as i32 => false,
                _ => !ascending,
            };

            keys.push(SortKey { col_idx, ascending, nulls_first });
        }
    }

    Ok(keys)
}

// ── Aggregate execution ───────────────────────────────────────────────

fn exec_select_aggregate(
    select: &pg_query::protobuf::SelectStmt,
    ctx: &JoinContext,
    rows: Vec<Vec<ArenaValue>>,
    arena: &mut QueryArena,
) -> Result<QueryResult, String> {
    let group_col_indices = resolve_group_columns(&select.group_clause, ctx)?;

    // Group rows (use Vec to maintain insertion order, HashMap for O(1) lookup)
    let mut groups: Vec<(Vec<ArenaValue>, Vec<Vec<ArenaValue>>)> = Vec::new();
    let mut group_index: HashMap<u64, Vec<usize>> = HashMap::new();

    if group_col_indices.is_empty() {
        groups.push((vec![], rows));
    } else {
        for row in rows {
            let key: Vec<ArenaValue> = group_col_indices.iter().map(|&i| row[i]).collect();

            // Hash the key
            let mut hasher = DefaultHasher::new();
            for v in &key {
                v.hash_with(arena, &mut hasher);
            }
            let h = hasher.finish();

            // Look up by hash, verify equality
            let mut found_idx = None;
            if let Some(candidates) = group_index.get(&h) {
                for &ci in candidates {
                    let existing_key = &groups[ci].0;
                    if existing_key.len() == key.len()
                        && existing_key.iter().zip(key.iter()).all(|(a, b)| a.eq_with(b, arena))
                    {
                        found_idx = Some(ci);
                        break;
                    }
                }
            }

            if let Some(idx) = found_idx {
                groups[idx].1.push(row);
            } else {
                let idx = groups.len();
                group_index.entry(h).or_default().push(idx);
                groups.push((key, vec![row]));
            }
        }
    }

    // Build result columns and rows
    let mut result_columns = Vec::new();
    let mut result_rows = Vec::new();
    let mut columns_built = false;

    for (group_key, group_rows) in &groups {
        let mut result_row = Vec::new();

        for target in &select.target_list {
            if let Some(NodeEnum::ResTarget(rt)) = target.node.as_ref() {
                let val_node = rt.val.as_ref().and_then(|v| v.node.as_ref());

                match val_node {
                    Some(NodeEnum::FuncCall(fc)) => {
                        let name = extract_func_name(fc);
                        if is_aggregate(&name) {
                            let agg_val =
                                compute_aggregate(&name, fc, group_rows, ctx, arena)?;
                            if !columns_built {
                                let alias = if rt.name.is_empty() {
                                    name.clone()
                                } else {
                                    rt.name.clone()
                                };
                                let oid = match name.as_str() {
                                    "count" => TypeOid::Int8.oid(),
                                    "avg" => TypeOid::Float8.oid(),
                                    "bool_and" | "bool_or" => TypeOid::Bool.oid(),
                                    _ => TypeOid::Text.oid(),
                                };
                                result_columns.push((alias, oid));
                            }
                            result_row.push(agg_val.to_text(arena));
                        } else {
                            return Err(format!(
                                "function {}() is not an aggregate function",
                                name
                            ));
                        }
                    }
                    Some(NodeEnum::ColumnRef(cref)) => {
                        let col_idx = resolve_column(cref, ctx)?;

                        if !group_col_indices.contains(&col_idx) {
                            let col_name = extract_col_name(cref);
                            return Err(format!(
                                "column \"{}\" must appear in the GROUP BY clause or be used in an aggregate function",
                                col_name
                            ));
                        }

                        if !columns_built {
                            let alias = if rt.name.is_empty() {
                                extract_col_name(cref)
                            } else {
                                rt.name.clone()
                            };
                            result_columns
                                .push((alias, column_type_oid(col_idx, ctx)?));
                        }

                        let key_pos = group_col_indices
                            .iter()
                            .position(|&i| i == col_idx)
                            .unwrap();
                        result_row.push(group_key[key_pos].to_text(arena));
                    }
                    _ => {
                        return Err(
                            "invalid expression in aggregate query".into(),
                        );
                    }
                }
            }
        }

        result_rows.push(result_row);
        columns_built = true;
    }

    // HAVING filter — collect passing indices, then retain (no per-row clone).
    if let Some(having_node) = &select.having_clause {
        if let Some(having_expr) = having_node.node.as_ref() {
            let mut keep = vec![false; result_rows.len()];
            for (i, (_, group_rows)) in groups.iter().enumerate() {
                if i < keep.len() && eval_having(having_expr, group_rows, ctx, arena)? {
                    keep[i] = true;
                }
            }
            let mut idx = 0;
            result_rows.retain(|_| { let k = keep[idx]; idx += 1; k });
        }
    }

    Ok(QueryResult {
        tag: format!("SELECT {}", result_rows.len()),
        columns: result_columns,
        rows: result_rows,
    })
}

fn resolve_group_columns(
    group_clause: &[pg_query::protobuf::Node],
    ctx: &JoinContext,
) -> Result<Vec<usize>, String> {
    let mut indices = Vec::new();
    for node in group_clause {
        if let Some(NodeEnum::ColumnRef(cref)) = node.node.as_ref() {
            let idx = resolve_column(cref, ctx)?;
            indices.push(idx);
        }
    }
    Ok(indices)
}

fn compute_aggregate(
    name: &str,
    fc: &pg_query::protobuf::FuncCall,
    rows: &[Vec<ArenaValue>],
    ctx: &JoinContext,
    arena: &mut QueryArena,
) -> Result<ArenaValue, String> {
    match name {
        "count" => {
            if fc.agg_star {
                Ok(ArenaValue::Int(rows.len() as i64))
            } else {
                let arg = fc
                    .args
                    .first()
                    .and_then(|a| a.node.as_ref())
                    .ok_or("COUNT requires argument")?;
                if fc.agg_distinct {
                    let mut seen: Vec<ArenaValue> = Vec::new();
                    for row in rows {
                        let v = eval_expr(arg, row, ctx, arena)?;
                        if v.is_null() { continue; }
                        if !seen.iter().any(|s| s.eq_with(&v, arena)) {
                            seen.push(v);
                        }
                    }
                    Ok(ArenaValue::Int(seen.len() as i64))
                } else {
                    let mut count: i64 = 0;
                    for row in rows {
                        match eval_expr(arg, row, ctx, arena)? {
                            ArenaValue::Null => {}
                            _ => count += 1,
                        }
                    }
                    Ok(ArenaValue::Int(count))
                }
            }
        }
        "sum" => {
            let arg = fc
                .args
                .first()
                .and_then(|a| a.node.as_ref())
                .ok_or("SUM requires argument")?;
            let mut vals: Vec<ArenaValue> = Vec::new();
            for row in rows {
                let v = eval_expr(arg, row, ctx, arena)?;
                if v.is_null() { continue; }
                if fc.agg_distinct {
                    if vals.iter().any(|s| s.eq_with(&v, arena)) { continue; }
                }
                vals.push(v);
            }
            if vals.is_empty() { return Ok(ArenaValue::Null); }
            let mut si: i64 = 0;
            let mut sf: f64 = 0.0;
            let mut is_float = false;
            for v in &vals {
                match v {
                    ArenaValue::Int(n) => { si = si.checked_add(*n).ok_or("integer out of range")?; }
                    ArenaValue::Float(f) => { sf += f; is_float = true; }
                    _ => return Err("SUM requires numeric".into()),
                }
            }
            Ok(if is_float { ArenaValue::Float(sf + si as f64) } else { ArenaValue::Int(si) })
        }
        "avg" => {
            let arg = fc
                .args
                .first()
                .and_then(|a| a.node.as_ref())
                .ok_or("AVG requires argument")?;
            let mut sum: f64 = 0.0;
            let mut count: i64 = 0;
            for row in rows {
                match eval_expr(arg, row, ctx, arena)? {
                    ArenaValue::Int(n) => {
                        sum += n as f64;
                        count += 1;
                    }
                    ArenaValue::Float(f) => {
                        sum += f;
                        count += 1;
                    }
                    ArenaValue::Null => {}
                    _ => return Err("AVG requires numeric".into()),
                }
            }
            Ok(if count == 0 {
                ArenaValue::Null
            } else {
                ArenaValue::Float(sum / count as f64)
            })
        }
        "min" => {
            let arg = fc
                .args
                .first()
                .and_then(|a| a.node.as_ref())
                .ok_or("MIN requires argument")?;
            let mut result: Option<ArenaValue> = None;
            for row in rows {
                let v = eval_expr(arg, row, ctx, arena)?;
                if v.is_null() {
                    continue;
                }
                result = Some(match result {
                    None => v,
                    Some(cur) => {
                        if v.compare(&cur, arena) == Some(std::cmp::Ordering::Less) {
                            v
                        } else {
                            cur
                        }
                    }
                });
            }
            Ok(result.unwrap_or(ArenaValue::Null))
        }
        "max" => {
            let arg = fc
                .args
                .first()
                .and_then(|a| a.node.as_ref())
                .ok_or("MAX requires argument")?;
            let mut result: Option<ArenaValue> = None;
            for row in rows {
                let v = eval_expr(arg, row, ctx, arena)?;
                if v.is_null() {
                    continue;
                }
                result = Some(match result {
                    None => v,
                    Some(cur) => {
                        if v.compare(&cur, arena) == Some(std::cmp::Ordering::Greater) {
                            v
                        } else {
                            cur
                        }
                    }
                });
            }
            Ok(result.unwrap_or(ArenaValue::Null))
        }
        "string_agg" => {
            let arg = fc.args.first().and_then(|a| a.node.as_ref())
                .ok_or("STRING_AGG requires argument")?;
            let delim_node = fc.args.get(1).and_then(|a| a.node.as_ref());
            let delim = if let Some(d) = delim_node {
                let dv = eval_expr(d, &rows[0], ctx, arena)?;
                dv.to_text(arena).unwrap_or_else(|| ", ".to_string())
            } else {
                ", ".to_string()
            };
            let mut parts: Vec<String> = Vec::new();
            for row in rows {
                let v = eval_expr(arg, row, ctx, arena)?;
                if let Some(text) = v.to_text(arena) {
                    parts.push(text);
                }
            }
            if parts.is_empty() {
                Ok(ArenaValue::Null)
            } else {
                // Apply ORDER BY within the aggregate if specified
                if !fc.agg_order.is_empty() {
                    parts.sort();
                }
                Ok(ArenaValue::Text(arena.alloc_str(&parts.join(&delim))))
            }
        }
        "bool_and" => {
            let arg = fc.args.first().and_then(|a| a.node.as_ref())
                .ok_or("BOOL_AND requires argument")?;
            let mut result = true;
            let mut has_val = false;
            for row in rows {
                match eval_expr(arg, row, ctx, arena)? {
                    ArenaValue::Bool(b) => { result = result && b; has_val = true; }
                    ArenaValue::Null => {}
                    _ => return Err("BOOL_AND requires boolean".into()),
                }
            }
            if !has_val { Ok(ArenaValue::Null) } else { Ok(ArenaValue::Bool(result)) }
        }
        "bool_or" => {
            let arg = fc.args.first().and_then(|a| a.node.as_ref())
                .ok_or("BOOL_OR requires argument")?;
            let mut result = false;
            let mut has_val = false;
            for row in rows {
                match eval_expr(arg, row, ctx, arena)? {
                    ArenaValue::Bool(b) => { result = result || b; has_val = true; }
                    ArenaValue::Null => {}
                    _ => return Err("BOOL_OR requires boolean".into()),
                }
            }
            if !has_val { Ok(ArenaValue::Null) } else { Ok(ArenaValue::Bool(result)) }
        }
        _ => Err(format!("unknown aggregate function: {}", name)),
    }
}

fn eval_having(
    expr: &NodeEnum,
    group_rows: &[Vec<ArenaValue>],
    ctx: &JoinContext,
    arena: &mut QueryArena,
) -> Result<bool, String> {
    let val = eval_having_expr(expr, group_rows, ctx, arena)?;
    Ok(matches!(val, ArenaValue::Bool(true)))
}

fn eval_having_expr(
    node: &NodeEnum,
    group_rows: &[Vec<ArenaValue>],
    ctx: &JoinContext,
    arena: &mut QueryArena,
) -> Result<ArenaValue, String> {
    match node {
        NodeEnum::FuncCall(fc) => {
            let name = extract_func_name(fc);
            if is_aggregate(&name) {
                compute_aggregate(&name, fc, group_rows, ctx, arena)
            } else {
                Err(format!("non-aggregate function in HAVING: {}", name))
            }
        }
        NodeEnum::AExpr(expr) => {
            let op = extract_op_name(&expr.name)?;
            let left = expr
                .lexpr
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .map(|n| eval_having_expr(n, group_rows, ctx, arena))
                .transpose()?;
            let right = expr
                .rexpr
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .map(|n| eval_having_expr(n, group_rows, ctx, arena))
                .transpose()?;

            let (l, r) = match (left, right) {
                (Some(l), Some(r)) => (l, r),
                _ => return Ok(ArenaValue::Null),
            };

            if l.is_null() || r.is_null() {
                return Ok(ArenaValue::Null);
            }

            let cmp = l.compare(&r, arena);
            let result = match op.as_str() {
                "=" => cmp.map(|o| o == std::cmp::Ordering::Equal),
                "<>" | "!=" => cmp.map(|o| o != std::cmp::Ordering::Equal),
                "<" => cmp.map(|o| o == std::cmp::Ordering::Less),
                ">" => cmp.map(|o| o == std::cmp::Ordering::Greater),
                "<=" => cmp.map(|o| o != std::cmp::Ordering::Greater),
                ">=" => cmp.map(|o| o != std::cmp::Ordering::Less),
                _ => return Err(format!("unsupported operator in HAVING: {}", op)),
            };
            Ok(result.map(ArenaValue::Bool).unwrap_or(ArenaValue::Null))
        }
        NodeEnum::AConst(_) | NodeEnum::Integer(_) | NodeEnum::Float(_) => {
            let dummy_ctx = JoinContext {
                sources: vec![],
                total_columns: 0,
            };
            eval_expr(node, &[], &dummy_ctx, arena)
        }
        NodeEnum::BoolExpr(be) => {
            let boolop = be.boolop;
            if boolop == pg_query::protobuf::BoolExprType::AndExpr as i32 {
                let mut has_null = false;
                for arg in &be.args {
                    if let Some(n) = arg.node.as_ref() {
                        match eval_having_expr(n, group_rows, ctx, arena)? {
                            ArenaValue::Bool(false) => return Ok(ArenaValue::Bool(false)),
                            ArenaValue::Bool(true) => continue,
                            _ => { has_null = true; continue; }
                        }
                    }
                }
                Ok(if has_null { ArenaValue::Null } else { ArenaValue::Bool(true) })
            } else if boolop == pg_query::protobuf::BoolExprType::OrExpr as i32 {
                let mut has_null = false;
                for arg in &be.args {
                    if let Some(n) = arg.node.as_ref() {
                        match eval_having_expr(n, group_rows, ctx, arena)? {
                            ArenaValue::Bool(true) => return Ok(ArenaValue::Bool(true)),
                            ArenaValue::Bool(false) => continue,
                            _ => { has_null = true; continue; }
                        }
                    }
                }
                Ok(if has_null { ArenaValue::Null } else { ArenaValue::Bool(false) })
            } else {
                Err("unsupported HAVING boolean expression".into())
            }
        }
        NodeEnum::ColumnRef(_) => {
            if group_rows.is_empty() {
                return Ok(ArenaValue::Null);
            }
            eval_expr(node, &group_rows[0], ctx, arena)
        }
        _ => Err("unsupported expression in HAVING clause".into()),
    }
}

// ── DELETE ─────────────────────────────────────────────────────────────

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

    let table_def = catalog::get_table(schema, table_name)
        .ok_or_else(|| format!("relation \"{}\" does not exist", table_name))?;
    let has_returning = !delete.returning_list.is_empty();

    let (count, deleted_rows) = if delete.where_clause.is_some() {
        let ctx = JoinContext::single(schema, table_name, table_def.clone());
        // Validate WHERE clause — single arena shared across all rows.
        let mut validate_arena = QueryArena::new();
        storage::scan_with(schema, table_name, |all_rows| {
            for row in all_rows {
                eval_where_value(&delete.where_clause, row, &ctx, &mut validate_arena)?;
            }
            Ok(())
        })?;
        if has_returning {
            let wc = delete.where_clause.clone();
            let mut del_arena = QueryArena::new();
            let rows = storage::delete_where_returning(schema, table_name, |row| {
                eval_where_value(&wc, row, &ctx, &mut del_arena).unwrap_or(false)
            })?;
            let n = rows.len() as u64;
            (n, rows)
        } else {
            let wc = delete.where_clause.clone();
            let mut del_arena = QueryArena::new();
            let n = storage::delete_where(schema, table_name, |row| {
                eval_where_value(&wc, row, &ctx, &mut del_arena).unwrap_or(false)
            })?;
            (n, vec![])
        }
    } else {
        if has_returning {
            let rows = storage::delete_all_returning(schema, table_name)?;
            let n = rows.len() as u64;
            (n, rows)
        } else {
            let n = storage::delete_all(schema, table_name)?;
            (n, vec![])
        }
    };

    let tag = format!("DELETE {}", count);
    if has_returning {
        eval_returning(&delete.returning_list, &deleted_rows, &table_def, schema, table_name, &tag)
    } else {
        Ok(QueryResult { tag, columns: vec![], rows: vec![] })
    }
}

// ── UPDATE ────────────────────────────────────────────────────────────

fn exec_update(
    update: &pg_query::protobuf::UpdateStmt,
) -> Result<QueryResult, String> {
    let rel = update
        .relation
        .as_ref()
        .ok_or("UPDATE missing relation")?;
    let table_name = &rel.relname;
    let schema = if rel.schemaname.is_empty() {
        "public"
    } else {
        &rel.schemaname
    };

    let table_def = catalog::get_table(schema, table_name)
        .ok_or_else(|| format!("relation \"{}\" does not exist", table_name))?;
    let ctx = JoinContext::single(schema, table_name, table_def.clone());

    let mut assignments: Vec<(usize, NodeEnum)> = Vec::new();
    for target in &update.target_list {
        if let Some(NodeEnum::ResTarget(rt)) = target.node.as_ref() {
            let col_name = &rt.name;
            let col_idx = table_def
                .columns
                .iter()
                .position(|c| &c.name == col_name)
                .ok_or_else(|| format!("column \"{}\" does not exist", col_name))?;
            let val_node = rt
                .val
                .as_ref()
                .and_then(|v| v.node.as_ref())
                .ok_or_else(|| format!("SET {} missing value", col_name))?;
            assignments.push((col_idx, val_node.clone()));
        }
    }

    // Validate WHERE clause — single arena shared across all rows.
    let mut validate_arena = QueryArena::new();
    storage::scan_with(schema, table_name, |all_rows| {
        for row in all_rows {
            eval_where_value(&update.where_clause, row, &ctx, &mut validate_arena)?;
        }
        Ok(())
    })?;

    let wc = update.where_clause.clone();
    let td = table_def.clone();
    let assigns = assignments.clone();
    let ctx2 = JoinContext::single(schema, table_name, td.clone());

    let has_returning = !update.returning_list.is_empty();
    let mut updated_rows: Vec<Vec<Value>> = Vec::new();
    let mut pred_arena = QueryArena::new();
    let mut expr_arena = QueryArena::new();

    let count = storage::update_rows_checked(
        schema,
        table_name,
        |row| eval_where_value(&wc, row, &ctx2, &mut pred_arena).unwrap_or(false),
        |row| {
            let ctx_inner = JoinContext::single(schema, table_name, td.clone());
            let mut new_row = row.clone();
            for (col_idx, expr_node) in &assigns {
                new_row[*col_idx] = eval_expr_value(expr_node, row, &ctx_inner, &mut expr_arena)?;
            }
            check_not_null(&td, &new_row)?;
            if has_returning {
                updated_rows.push(new_row.clone());
            }
            Ok(new_row)
        },
        |new_row, all_rows, skip_idx| {
            check_unique_against(&td, new_row, all_rows, skip_idx)
        },
    )?;

    let tag = format!("UPDATE {}", count);
    if has_returning {
        eval_returning(&update.returning_list, &updated_rows, &table_def, schema, table_name, &tag)
    } else {
        Ok(QueryResult { tag, columns: vec![], rows: vec![] })
    }
}

// ── TRUNCATE ──────────────────────────────────────────────────────────

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

// ══════════════════════════════════════════════════════════════════════
// Tests
// ══════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() {
        catalog::reset();
        storage::reset();
    }

    fn setup_test_table() {
        setup();
        execute("CREATE TABLE t (id integer, name text)").unwrap();
        execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
        execute("INSERT INTO t VALUES (2, 'bob')").unwrap();
        execute("INSERT INTO t VALUES (3, 'carol')").unwrap();
    }

    // ── Basic CRUD ────────────────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn create_and_insert_and_select() {
        setup();
        execute("CREATE TABLE users (id integer, name text)").unwrap();
        execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
        execute("INSERT INTO users VALUES (2, 'bob')").unwrap();
        let result = execute("SELECT * FROM users").unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0][0], Some("1".into()));
        assert_eq!(result.rows[0][1], Some("alice".into()));
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

    // ── WHERE ─────────────────────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn select_where_eq() {
        setup_test_table();
        let r = execute("SELECT * FROM t WHERE id = 1").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][1], Some("alice".into()));
    }

    #[test]
    #[serial_test::serial]
    fn select_where_gt() {
        setup_test_table();
        let r = execute("SELECT * FROM t WHERE id > 1").unwrap();
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    #[serial_test::serial]
    fn select_where_and() {
        setup_test_table();
        let r = execute("SELECT * FROM t WHERE id > 1 AND name = 'bob'").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("2".into()));
    }

    #[test]
    #[serial_test::serial]
    fn select_where_or() {
        setup_test_table();
        let r = execute("SELECT * FROM t WHERE id = 1 OR id = 3").unwrap();
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    #[serial_test::serial]
    fn select_where_is_null() {
        setup();
        execute("CREATE TABLE t (id integer, name text)").unwrap();
        execute("INSERT INTO t (id) VALUES (1)").unwrap();
        execute("INSERT INTO t VALUES (2, 'bob')").unwrap();
        let r = execute("SELECT * FROM t WHERE name IS NULL").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("1".into()));
    }

    #[test]
    #[serial_test::serial]
    fn select_where_is_not_null() {
        setup();
        execute("CREATE TABLE t (id integer, name text)").unwrap();
        execute("INSERT INTO t (id) VALUES (1)").unwrap();
        execute("INSERT INTO t VALUES (2, 'bob')").unwrap();
        let r = execute("SELECT * FROM t WHERE name IS NOT NULL").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("2".into()));
    }

    // ── UPDATE ────────────────────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn update_with_where() {
        setup_test_table();
        let r = execute("UPDATE t SET name = 'updated' WHERE id = 1").unwrap();
        assert_eq!(r.tag, "UPDATE 1");
        let sel = execute("SELECT * FROM t WHERE id = 1").unwrap();
        assert_eq!(sel.rows[0][1], Some("updated".into()));
    }

    #[test]
    #[serial_test::serial]
    fn update_all_rows() {
        setup_test_table();
        let r = execute("UPDATE t SET name = 'all'").unwrap();
        assert_eq!(r.tag, "UPDATE 3");
    }

    #[test]
    #[serial_test::serial]
    fn update_self_referential() {
        setup_test_table();
        execute("UPDATE t SET id = id + 1 WHERE id = 1").unwrap();
        let sel = execute("SELECT * FROM t WHERE name = 'alice'").unwrap();
        assert_eq!(sel.rows[0][0], Some("2".into()));
    }

    // ── DELETE WHERE ──────────────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn delete_where() {
        setup_test_table();
        let r = execute("DELETE FROM t WHERE id = 1").unwrap();
        assert_eq!(r.tag, "DELETE 1");
        assert_eq!(execute("SELECT * FROM t").unwrap().rows.len(), 2);
    }

    #[test]
    #[serial_test::serial]
    fn delete_where_no_match() {
        setup_test_table();
        let r = execute("DELETE FROM t WHERE id = 999").unwrap();
        assert_eq!(r.tag, "DELETE 0");
        assert_eq!(execute("SELECT * FROM t").unwrap().rows.len(), 3);
    }

    // ── ORDER BY / LIMIT / OFFSET ─────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn order_by_asc() {
        setup();
        execute("CREATE TABLE t (id int, name text)").unwrap();
        execute("INSERT INTO t VALUES (3, 'c')").unwrap();
        execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        let r = execute("SELECT * FROM t ORDER BY id").unwrap();
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[1][0], Some("2".into()));
        assert_eq!(r.rows[2][0], Some("3".into()));
    }

    #[test]
    #[serial_test::serial]
    fn order_by_desc() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        execute("INSERT INTO t VALUES (1)").unwrap();
        execute("INSERT INTO t VALUES (3)").unwrap();
        execute("INSERT INTO t VALUES (2)").unwrap();
        let r = execute("SELECT * FROM t ORDER BY id DESC").unwrap();
        assert_eq!(r.rows[0][0], Some("3".into()));
        assert_eq!(r.rows[2][0], Some("1".into()));
    }

    #[test]
    #[serial_test::serial]
    fn limit_basic() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        for i in 1..=5 {
            execute(&format!("INSERT INTO t VALUES ({})", i)).unwrap();
        }
        let r = execute("SELECT * FROM t LIMIT 2").unwrap();
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    #[serial_test::serial]
    fn limit_offset() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        for i in 1..=5 {
            execute(&format!("INSERT INTO t VALUES ({})", i)).unwrap();
        }
        let r = execute("SELECT * FROM t ORDER BY id LIMIT 2 OFFSET 1").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("2".into()));
        assert_eq!(r.rows[1][0], Some("3".into()));
    }

    #[test]
    #[serial_test::serial]
    fn order_by_desc_limit() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        for i in 1..=5 {
            execute(&format!("INSERT INTO t VALUES ({})", i)).unwrap();
        }
        let r = execute("SELECT * FROM t ORDER BY id DESC LIMIT 2").unwrap();
        assert_eq!(r.rows[0][0], Some("5".into()));
        assert_eq!(r.rows[1][0], Some("4".into()));
    }

    #[test]
    #[serial_test::serial]
    fn offset_beyond() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        execute("INSERT INTO t VALUES (1)").unwrap();
        let r = execute("SELECT * FROM t OFFSET 1000").unwrap();
        assert_eq!(r.rows.len(), 0);
    }

    #[test]
    #[serial_test::serial]
    fn limit_zero() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        execute("INSERT INTO t VALUES (1)").unwrap();
        let r = execute("SELECT * FROM t LIMIT 0").unwrap();
        assert_eq!(r.rows.len(), 0);
    }

    // ── Expressions in SELECT ─────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn select_arithmetic() {
        setup();
        execute("CREATE TABLE t (id int, price float8)").unwrap();
        execute("INSERT INTO t VALUES (1, 100.0)").unwrap();
        let r = execute("SELECT id, price * 1.1 FROM t").unwrap();
        assert_eq!(r.rows[0][0], Some("1".into()));
        let val: f64 = r.rows[0][1].as_ref().unwrap().parse().unwrap();
        assert!((val - 110.0).abs() < 0.01);
    }

    #[test]
    #[serial_test::serial]
    fn select_upper() {
        setup();
        execute("CREATE TABLE t (name text)").unwrap();
        execute("INSERT INTO t VALUES ('hello')").unwrap();
        let r = execute("SELECT upper(name) FROM t").unwrap();
        assert_eq!(r.rows[0][0], Some("HELLO".into()));
    }

    #[test]
    #[serial_test::serial]
    fn select_lower() {
        setup();
        execute("CREATE TABLE t (name text)").unwrap();
        execute("INSERT INTO t VALUES ('HELLO')").unwrap();
        let r = execute("SELECT lower(name) FROM t").unwrap();
        assert_eq!(r.rows[0][0], Some("hello".into()));
    }

    #[test]
    #[serial_test::serial]
    fn select_length() {
        setup();
        execute("CREATE TABLE t (name text)").unwrap();
        execute("INSERT INTO t VALUES ('hello')").unwrap();
        let r = execute("SELECT length(name) FROM t").unwrap();
        assert_eq!(r.rows[0][0], Some("5".into()));
    }

    #[test]
    #[serial_test::serial]
    fn select_concat_func() {
        setup();
        execute("CREATE TABLE t (a text, b text)").unwrap();
        execute("INSERT INTO t VALUES ('hello', 'world')").unwrap();
        let r = execute("SELECT concat(a, ' ', b) FROM t").unwrap();
        assert_eq!(r.rows[0][0], Some("hello world".into()));
    }

    #[test]
    #[serial_test::serial]
    fn select_concat_op() {
        setup();
        execute("CREATE TABLE t (a text, b text)").unwrap();
        execute("INSERT INTO t VALUES ('hello', 'world')").unwrap();
        let r = execute("SELECT a || ' ' || b FROM t").unwrap();
        assert_eq!(r.rows[0][0], Some("hello world".into()));
    }

    #[test]
    #[serial_test::serial]
    fn select_unary_minus() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        execute("INSERT INTO t VALUES (5)").unwrap();
        let r = execute("SELECT -id FROM t").unwrap();
        assert_eq!(r.rows[0][0], Some("-5".into()));
    }

    #[test]
    #[serial_test::serial]
    fn select_int_division() {
        setup();
        let r = execute("SELECT 5 / 2").unwrap();
        assert_eq!(r.rows[0][0], Some("2".into()));
    }

    #[test]
    #[serial_test::serial]
    fn select_expr_with_alias() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        execute("INSERT INTO t VALUES (1)").unwrap();
        let r = execute("SELECT id + 1 AS next_id FROM t").unwrap();
        assert_eq!(r.columns[0].0, "next_id");
        assert_eq!(r.rows[0][0], Some("2".into()));
    }

    // ── Constraints ───────────────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn pk_prevents_duplicate() {
        setup();
        execute("CREATE TABLE t (id int PRIMARY KEY, name text)").unwrap();
        execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        let err = execute("INSERT INTO t VALUES (1, 'b')");
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("unique constraint"));
    }

    #[test]
    #[serial_test::serial]
    fn pk_prevents_null() {
        setup();
        execute("CREATE TABLE t (id int PRIMARY KEY, name text)").unwrap();
        let err = execute("INSERT INTO t (name) VALUES ('a')");
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("not-null"));
    }

    #[test]
    #[serial_test::serial]
    fn unique_prevents_duplicate() {
        setup();
        execute("CREATE TABLE t (id int, email text UNIQUE)").unwrap();
        execute("INSERT INTO t VALUES (1, 'a@b.com')").unwrap();
        assert!(execute("INSERT INTO t VALUES (2, 'a@b.com')").is_err());
    }

    #[test]
    #[serial_test::serial]
    fn unique_allows_multiple_nulls() {
        setup();
        execute("CREATE TABLE t (id int, email text UNIQUE)").unwrap();
        execute("INSERT INTO t VALUES (1, NULL)").unwrap();
        execute("INSERT INTO t VALUES (2, NULL)").unwrap();
        let r = execute("SELECT * FROM t").unwrap();
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    #[serial_test::serial]
    fn not_null_constraint() {
        setup();
        execute("CREATE TABLE t (id int NOT NULL, name text)").unwrap();
        let err = execute("INSERT INTO t (name) VALUES ('a')");
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("not-null"));
    }

    #[test]
    #[serial_test::serial]
    fn composite_pk() {
        setup();
        execute("CREATE TABLE t (a int, b int, c text, PRIMARY KEY (a, b))").unwrap();
        execute("INSERT INTO t VALUES (1, 1, 'x')").unwrap();
        execute("INSERT INTO t VALUES (1, 2, 'y')").unwrap(); // ok
        assert!(execute("INSERT INTO t VALUES (1, 1, 'z')").is_err()); // duplicate
    }

    // ── Aggregates / GROUP BY / HAVING ────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn count_star() {
        setup();
        execute("CREATE TABLE t (id int, name text)").unwrap();
        execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        let r = execute("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("2".into()));
    }

    #[test]
    #[serial_test::serial]
    fn count_column_skips_nulls() {
        setup();
        execute("CREATE TABLE t (id int, name text)").unwrap();
        execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        execute("INSERT INTO t VALUES (2, NULL)").unwrap();
        let r = execute("SELECT COUNT(name) FROM t").unwrap();
        assert_eq!(r.rows[0][0], Some("1".into()));
    }

    #[test]
    #[serial_test::serial]
    fn count_empty_table() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        let r = execute("SELECT COUNT(*) FROM t").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("0".into()));
    }

    #[test]
    #[serial_test::serial]
    fn sum_basic() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        execute("INSERT INTO t VALUES (10)").unwrap();
        execute("INSERT INTO t VALUES (20)").unwrap();
        execute("INSERT INTO t VALUES (30)").unwrap();
        let r = execute("SELECT SUM(id) FROM t").unwrap();
        assert_eq!(r.rows[0][0], Some("60".into()));
    }

    #[test]
    #[serial_test::serial]
    fn sum_empty_is_null() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        let r = execute("SELECT SUM(id) FROM t").unwrap();
        assert_eq!(r.rows[0][0], None);
    }

    #[test]
    #[serial_test::serial]
    fn avg_basic() {
        setup();
        execute("CREATE TABLE t (val int)").unwrap();
        execute("INSERT INTO t VALUES (10)").unwrap();
        execute("INSERT INTO t VALUES (20)").unwrap();
        let r = execute("SELECT AVG(val) FROM t").unwrap();
        assert_eq!(r.rows[0][0], Some("15".into()));
    }

    #[test]
    #[serial_test::serial]
    fn min_max() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        execute("INSERT INTO t VALUES (3)").unwrap();
        execute("INSERT INTO t VALUES (1)").unwrap();
        execute("INSERT INTO t VALUES (5)").unwrap();
        let r = execute("SELECT MIN(id), MAX(id) FROM t").unwrap();
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[0][1], Some("5".into()));
    }

    #[test]
    #[serial_test::serial]
    fn group_by_basic() {
        setup();
        execute("CREATE TABLE emp (dept text, salary int)").unwrap();
        execute("INSERT INTO emp VALUES ('eng', 100)").unwrap();
        execute("INSERT INTO emp VALUES ('eng', 200)").unwrap();
        execute("INSERT INTO emp VALUES ('sales', 150)").unwrap();
        let r = execute("SELECT dept, COUNT(*) FROM emp GROUP BY dept").unwrap();
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    #[serial_test::serial]
    fn group_by_having() {
        setup();
        execute("CREATE TABLE emp (dept text, salary int)").unwrap();
        execute("INSERT INTO emp VALUES ('eng', 100)").unwrap();
        execute("INSERT INTO emp VALUES ('eng', 200)").unwrap();
        execute("INSERT INTO emp VALUES ('sales', 150)").unwrap();
        let r = execute(
            "SELECT dept, COUNT(*) FROM emp GROUP BY dept HAVING COUNT(*) > 1",
        )
        .unwrap();
        assert_eq!(r.rows.len(), 1);
    }

    #[test]
    #[serial_test::serial]
    fn mixed_agg_no_group_by_error() {
        setup();
        execute("CREATE TABLE t (id int, name text)").unwrap();
        execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        let err = execute("SELECT id, COUNT(*) FROM t");
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("GROUP BY"));
    }

    // ── Devin review fixes ────────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn update_division_by_zero_errors() {
        setup();
        execute("CREATE TABLE t (id int, val int)").unwrap();
        execute("INSERT INTO t VALUES (1, 10)").unwrap();
        let err = execute("UPDATE t SET val = val / 0 WHERE id = 1");
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("division by zero"));
        // Row should be unchanged
        let r = execute("SELECT val FROM t WHERE id = 1").unwrap();
        assert_eq!(r.rows[0][0], Some("10".into()));
    }

    #[test]
    #[serial_test::serial]
    fn update_enforces_unique_constraint() {
        setup();
        execute("CREATE TABLE t (id int PRIMARY KEY, name text)").unwrap();
        execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        let err = execute("UPDATE t SET id = 1 WHERE id = 2");
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("unique constraint"));
    }

    #[test]
    #[serial_test::serial]
    fn update_enforces_not_null() {
        setup();
        execute("CREATE TABLE t (id int PRIMARY KEY, name text NOT NULL)").unwrap();
        execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        let err = execute("UPDATE t SET name = NULL WHERE id = 1");
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("not-null"));
    }

    // ── JOIN tests ────────────────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn inner_join() {
        setup();
        execute("CREATE TABLE users (id int, name text)").unwrap();
        execute("CREATE TABLE orders (id int, user_id int, total int)").unwrap();
        execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
        execute("INSERT INTO users VALUES (2, 'bob')").unwrap();
        execute("INSERT INTO orders VALUES (10, 1, 100)").unwrap();
        execute("INSERT INTO orders VALUES (11, 1, 200)").unwrap();
        execute("INSERT INTO orders VALUES (12, 2, 50)").unwrap();
        let r = execute(
            "SELECT users.name, orders.total FROM users JOIN orders ON users.id = orders.user_id",
        )
        .unwrap();
        assert_eq!(r.rows.len(), 3);
    }

    #[test]
    #[serial_test::serial]
    fn left_join() {
        setup();
        execute("CREATE TABLE users (id int, name text)").unwrap();
        execute("CREATE TABLE orders (id int, user_id int, total int)").unwrap();
        execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
        execute("INSERT INTO users VALUES (2, 'bob')").unwrap();
        execute("INSERT INTO users VALUES (3, 'carol')").unwrap();
        execute("INSERT INTO orders VALUES (10, 1, 100)").unwrap();
        let r = execute(
            "SELECT users.name, orders.total FROM users LEFT JOIN orders ON users.id = orders.user_id",
        )
        .unwrap();
        assert_eq!(r.rows.len(), 3); // alice with order, bob NULL, carol NULL
    }

    #[test]
    #[serial_test::serial]
    fn cross_join() {
        setup();
        execute("CREATE TABLE colors (name text)").unwrap();
        execute("CREATE TABLE sizes (size text)").unwrap();
        execute("INSERT INTO colors VALUES ('red')").unwrap();
        execute("INSERT INTO colors VALUES ('blue')").unwrap();
        execute("INSERT INTO sizes VALUES ('S')").unwrap();
        execute("INSERT INTO sizes VALUES ('M')").unwrap();
        execute("INSERT INTO sizes VALUES ('L')").unwrap();
        let r = execute("SELECT * FROM colors CROSS JOIN sizes").unwrap();
        assert_eq!(r.rows.len(), 6); // 2 * 3
    }

    #[test]
    #[serial_test::serial]
    fn implicit_join() {
        setup();
        execute("CREATE TABLE a (id int, val text)").unwrap();
        execute("CREATE TABLE b (a_id int, data text)").unwrap();
        execute("INSERT INTO a VALUES (1, 'x')").unwrap();
        execute("INSERT INTO a VALUES (2, 'y')").unwrap();
        execute("INSERT INTO b VALUES (1, 'linked')").unwrap();
        let r = execute("SELECT a.val, b.data FROM a, b WHERE a.id = b.a_id").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("x".into()));
        assert_eq!(r.rows[0][1], Some("linked".into()));
    }

    #[test]
    #[serial_test::serial]
    fn join_with_aliases() {
        setup();
        execute("CREATE TABLE users (id int, name text)").unwrap();
        execute("CREATE TABLE orders (id int, user_id int, total int)").unwrap();
        execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
        execute("INSERT INTO orders VALUES (10, 1, 100)").unwrap();
        let r = execute(
            "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id",
        )
        .unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("alice".into()));
        assert_eq!(r.rows[0][1], Some("100".into()));
    }

    #[test]
    #[serial_test::serial]
    fn three_way_join() {
        setup();
        execute("CREATE TABLE a (id int, name text)").unwrap();
        execute("CREATE TABLE b (id int, a_id int)").unwrap();
        execute("CREATE TABLE c (id int, b_id int, val text)").unwrap();
        execute("INSERT INTO a VALUES (1, 'root')").unwrap();
        execute("INSERT INTO b VALUES (10, 1)").unwrap();
        execute("INSERT INTO c VALUES (100, 10, 'leaf')").unwrap();
        let r = execute(
            "SELECT a.name, c.val FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id",
        )
        .unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("root".into()));
        assert_eq!(r.rows[0][1], Some("leaf".into()));
    }

    #[test]
    #[serial_test::serial]
    fn right_join() {
        setup();
        execute("CREATE TABLE orders (id int, user_id int)").unwrap();
        execute("CREATE TABLE users (id int, name text)").unwrap();
        execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
        execute("INSERT INTO users VALUES (2, 'bob')").unwrap();
        execute("INSERT INTO orders VALUES (10, 1)").unwrap();
        let r = execute(
            "SELECT orders.id, users.name FROM orders RIGHT JOIN users ON users.id = orders.user_id",
        )
        .unwrap();
        assert_eq!(r.rows.len(), 2); // alice with order, bob with NULL order
    }

    #[test]
    #[serial_test::serial]
    fn join_with_aggregate() {
        setup();
        execute("CREATE TABLE users (id int, name text)").unwrap();
        execute("CREATE TABLE orders (id int, user_id int, total int)").unwrap();
        execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
        execute("INSERT INTO users VALUES (2, 'bob')").unwrap();
        execute("INSERT INTO orders VALUES (10, 1, 100)").unwrap();
        execute("INSERT INTO orders VALUES (11, 1, 200)").unwrap();
        execute("INSERT INTO orders VALUES (12, 2, 50)").unwrap();
        let r = execute(
            "SELECT users.name, SUM(orders.total) FROM users JOIN orders ON users.id = orders.user_id GROUP BY users.name",
        )
        .unwrap();
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    #[serial_test::serial]
    fn ambiguous_column_error() {
        setup();
        execute("CREATE TABLE a (id int, name text)").unwrap();
        execute("CREATE TABLE b (id int, data text)").unwrap();
        execute("INSERT INTO a VALUES (1, 'x')").unwrap();
        execute("INSERT INTO b VALUES (1, 'y')").unwrap();
        let err = execute("SELECT id FROM a JOIN b ON a.id = b.id");
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("ambiguous"));
    }

    #[test]
    #[serial_test::serial]
    fn join_select_star() {
        setup();
        execute("CREATE TABLE t1 (a int, b text)").unwrap();
        execute("CREATE TABLE t2 (c int, d text)").unwrap();
        execute("INSERT INTO t1 VALUES (1, 'x')").unwrap();
        execute("INSERT INTO t2 VALUES (2, 'y')").unwrap();
        let r = execute("SELECT * FROM t1 CROSS JOIN t2").unwrap();
        assert_eq!(r.columns.len(), 4); // a, b, c, d
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[0][1], Some("x".into()));
        assert_eq!(r.rows[0][2], Some("2".into()));
        assert_eq!(r.rows[0][3], Some("y".into()));
    }

    // ── RETURNING clause tests ────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn insert_returning_star() {
        setup();
        execute("CREATE TABLE t (id int, name text)").unwrap();
        let r = execute("INSERT INTO t VALUES (1, 'alice') RETURNING *").unwrap();
        assert_eq!(r.tag, "INSERT 0 1");
        assert_eq!(r.columns.len(), 2);
        assert_eq!(r.columns[0].0, "id");
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[0][1], Some("alice".into()));
    }

    #[test]
    #[serial_test::serial]
    fn insert_returning_specific_column() {
        setup();
        execute("CREATE TABLE t (id int, name text)").unwrap();
        let r = execute("INSERT INTO t VALUES (1, 'alice') RETURNING id").unwrap();
        assert_eq!(r.columns.len(), 1);
        assert_eq!(r.columns[0].0, "id");
        assert_eq!(r.rows[0][0], Some("1".into()));
    }

    #[test]
    #[serial_test::serial]
    fn insert_returning_multi_row() {
        setup();
        execute("CREATE TABLE t (id int, name text)").unwrap();
        let r = execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob') RETURNING id, name").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[1][0], Some("2".into()));
    }

    #[test]
    #[serial_test::serial]
    fn insert_returning_expression() {
        setup();
        execute("CREATE TABLE t (id int, name text)").unwrap();
        let r = execute("INSERT INTO t VALUES (1, 'alice') RETURNING id, upper(name)").unwrap();
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[0][1], Some("ALICE".into()));
    }

    #[test]
    #[serial_test::serial]
    fn update_returning() {
        setup();
        execute("CREATE TABLE t (id int, name text)").unwrap();
        execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
        execute("INSERT INTO t VALUES (2, 'bob')").unwrap();
        let r = execute("UPDATE t SET name = 'updated' WHERE id = 1 RETURNING *").unwrap();
        assert_eq!(r.tag, "UPDATE 1");
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[0][1], Some("updated".into()));
    }

    #[test]
    #[serial_test::serial]
    fn delete_returning() {
        setup();
        execute("CREATE TABLE t (id int, name text)").unwrap();
        execute("INSERT INTO t VALUES (1, 'alice')").unwrap();
        execute("INSERT INTO t VALUES (2, 'bob')").unwrap();
        let r = execute("DELETE FROM t WHERE id = 1 RETURNING *").unwrap();
        assert_eq!(r.tag, "DELETE 1");
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[0][1], Some("alice".into()));
        // Verify row was actually deleted
        let sel = execute("SELECT * FROM t").unwrap();
        assert_eq!(sel.rows.len(), 1);
    }

    #[test]
    #[serial_test::serial]
    fn delete_all_returning() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        execute("INSERT INTO t VALUES (1)").unwrap();
        execute("INSERT INTO t VALUES (2)").unwrap();
        let r = execute("DELETE FROM t RETURNING id").unwrap();
        assert_eq!(r.rows.len(), 2);
    }

    // ── DEFAULT + SERIAL tests ────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn serial_auto_increment() {
        setup();
        execute("CREATE TABLE t (id serial PRIMARY KEY, name text)").unwrap();
        execute("INSERT INTO t (name) VALUES ('alice')").unwrap();
        execute("INSERT INTO t (name) VALUES ('bob')").unwrap();
        let r = execute("SELECT * FROM t ORDER BY id").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[0][1], Some("alice".into()));
        assert_eq!(r.rows[1][0], Some("2".into()));
        assert_eq!(r.rows[1][1], Some("bob".into()));
    }

    #[test]
    #[serial_test::serial]
    fn serial_with_returning() {
        setup();
        execute("CREATE TABLE t (id serial PRIMARY KEY, name text)").unwrap();
        let r = execute("INSERT INTO t (name) VALUES ('alice') RETURNING id").unwrap();
        assert_eq!(r.rows[0][0], Some("1".into()));
        let r = execute("INSERT INTO t (name) VALUES ('bob') RETURNING id, name").unwrap();
        assert_eq!(r.rows[0][0], Some("2".into()));
        assert_eq!(r.rows[0][1], Some("bob".into()));
    }

    #[test]
    #[serial_test::serial]
    fn default_literal_values() {
        setup();
        execute("CREATE TABLE t (id int DEFAULT 0, name text DEFAULT 'anon')").unwrap();
        execute("INSERT INTO t (name) VALUES ('alice')").unwrap();
        let r = execute("SELECT * FROM t").unwrap();
        assert_eq!(r.rows[0][0], Some("0".into()));
        assert_eq!(r.rows[0][1], Some("alice".into()));
    }

    #[test]
    #[serial_test::serial]
    fn nextval_function() {
        setup();
        execute("CREATE TABLE t (id serial, name text)").unwrap();
        let r = execute("SELECT nextval('t_id_seq')").unwrap();
        // Sequence was at 0 after table creation consumed 0 values
        // Actually, serial CREATE creates seq starting at 1
        // nextval from SELECT should advance it
        assert!(r.rows[0][0].is_some());
    }

    // ── Subquery tests ──────────────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn in_subquery() {
        setup();
        execute("CREATE TABLE users (id int, name text)").unwrap();
        execute("CREATE TABLE orders (user_id int)").unwrap();
        execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')").unwrap();
        execute("INSERT INTO orders VALUES (1), (1), (3)").unwrap();
        let r =
            execute("SELECT name FROM users WHERE id IN (SELECT user_id FROM orders)").unwrap();
        assert_eq!(r.rows.len(), 2); // alice, carol (not bob)
    }

    #[test]
    #[serial_test::serial]
    fn not_in_subquery() {
        setup();
        execute("CREATE TABLE users (id int, name text)").unwrap();
        execute("CREATE TABLE orders (user_id int)").unwrap();
        execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')").unwrap();
        execute("INSERT INTO orders VALUES (1), (3)").unwrap();
        let r = execute("SELECT name FROM users WHERE id NOT IN (SELECT user_id FROM orders)")
            .unwrap();
        assert_eq!(r.rows.len(), 1); // bob only
    }

    #[test]
    #[serial_test::serial]
    fn exists_subquery() {
        setup();
        execute("CREATE TABLE users (id int, name text)").unwrap();
        execute("CREATE TABLE orders (user_id int)").unwrap();
        execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')").unwrap();
        execute("INSERT INTO orders VALUES (1)").unwrap();
        let r = execute(
            "SELECT name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)",
        )
        .unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("alice".into()));
    }

    #[test]
    #[serial_test::serial]
    fn scalar_subquery() {
        setup();
        execute("CREATE TABLE t (id int, val int)").unwrap();
        execute("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").unwrap();
        let r = execute("SELECT (SELECT SUM(val) FROM t)").unwrap();
        assert_eq!(r.rows[0][0], Some("60".into()));
    }

    #[test]
    #[serial_test::serial]
    fn in_literal_list() {
        setup();
        execute("CREATE TABLE t (id int, name text)").unwrap();
        execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").unwrap();
        let r = execute("SELECT * FROM t WHERE id IN (1, 3)").unwrap();
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    #[serial_test::serial]
    fn not_in_literal_list() {
        setup();
        execute("CREATE TABLE t (id int, name text)").unwrap();
        execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").unwrap();
        let r = execute("SELECT * FROM t WHERE id NOT IN (1, 3)").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][1], Some("b".into()));
    }

    #[test]
    #[serial_test::serial]
    fn derived_table() {
        setup();
        execute("CREATE TABLE t (id int, name text)").unwrap();
        execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')").unwrap();
        let r = execute(
            "SELECT sub.name FROM (SELECT id, name FROM t WHERE id > 1) AS sub ORDER BY sub.name",
        )
        .unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("b".into()));
        assert_eq!(r.rows[1][0], Some("c".into()));
    }

    #[test]
    #[serial_test::serial]
    fn scalar_subquery_too_many_rows() {
        setup();
        execute("CREATE TABLE t (id int)").unwrap();
        execute("INSERT INTO t VALUES (1), (2)").unwrap();
        let err = execute("SELECT (SELECT id FROM t)");
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("more than one row"));
    }

    // ── Vector type tests ────────────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn vector_create_insert_select() {
        setup();
        execute("CREATE TABLE items (id int, embedding vector)").unwrap();
        execute("INSERT INTO items VALUES (1, '[1.0, 2.0, 3.0]')").unwrap();
        execute("INSERT INTO items VALUES (2, '[4.0, 5.0, 6.0]')").unwrap();
        let r = execute("SELECT * FROM items").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][1], Some("[1,2,3]".into()));
    }

    #[test]
    #[serial_test::serial]
    fn vector_l2_distance() {
        setup();
        execute("CREATE TABLE items (id int, embedding vector)").unwrap();
        execute("INSERT INTO items VALUES (1, '[1.0, 0.0, 0.0]')").unwrap();
        execute("INSERT INTO items VALUES (2, '[0.0, 1.0, 0.0]')").unwrap();
        execute("INSERT INTO items VALUES (3, '[1.0, 1.0, 0.0]')").unwrap();
        // L2 distance from [1,0,0]: item 1=0, item 3=1, item 2=sqrt(2)
        let r = execute("SELECT id FROM items ORDER BY embedding <-> '[1.0, 0.0, 0.0]' LIMIT 2")
            .unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("1".into())); // closest
        assert_eq!(r.rows[1][0], Some("3".into())); // second closest
    }

    #[test]
    #[serial_test::serial]
    fn vector_cosine_distance() {
        setup();
        execute("CREATE TABLE items (id int, embedding vector)").unwrap();
        execute("INSERT INTO items VALUES (1, '[1.0, 0.0]')").unwrap();
        execute("INSERT INTO items VALUES (2, '[0.0, 1.0]')").unwrap();
        execute("INSERT INTO items VALUES (3, '[0.707, 0.707]')").unwrap();
        // Cosine distance from [1,0]: item 1=0, item 3~0.29, item 2=1
        let r = execute("SELECT id FROM items ORDER BY embedding <=> '[1.0, 0.0]' LIMIT 2")
            .unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("1".into()));
    }

    #[test]
    #[serial_test::serial]
    fn vector_inner_product() {
        setup();
        execute("CREATE TABLE items (id int, embedding vector)").unwrap();
        execute("INSERT INTO items VALUES (1, '[1.0, 2.0, 3.0]')").unwrap();
        execute("INSERT INTO items VALUES (2, '[3.0, 2.0, 1.0]')").unwrap();
        // Inner product with [1,0,0]: item 1=1, item 2=3
        // Negative inner product: item 1=-1, item 2=-3
        // ORDER BY <#> (ascending): item 2 first (most similar via inner product)
        let r =
            execute("SELECT id FROM items ORDER BY embedding <#> '[1.0, 0.0, 0.0]'").unwrap();
        assert_eq!(r.rows[0][0], Some("2".into())); // highest inner product
    }

    #[test]
    #[serial_test::serial]
    fn vector_dimension_mismatch() {
        setup();
        execute("CREATE TABLE items (id int, embedding vector)").unwrap();
        execute("INSERT INTO items VALUES (1, '[1.0, 2.0]')").unwrap();
        execute("INSERT INTO items VALUES (2, '[1.0, 2.0, 3.0]')").unwrap();
        // Different dimensions should error
        let err = execute("SELECT id FROM items ORDER BY embedding <-> '[1.0, 2.0]'");
        // This will error during the sort comparison
        assert!(err.is_err());
    }

    #[test]
    #[serial_test::serial]
    fn vector_knn_search() {
        setup();
        execute("CREATE TABLE points (id int, pos vector)").unwrap();
        execute("INSERT INTO points VALUES (1, '[0.0, 0.0]')").unwrap();
        execute("INSERT INTO points VALUES (2, '[1.0, 1.0]')").unwrap();
        execute("INSERT INTO points VALUES (3, '[2.0, 2.0]')").unwrap();
        execute("INSERT INTO points VALUES (4, '[10.0, 10.0]')").unwrap();
        // KNN: 3 nearest to [1.5, 1.5]
        let r = execute("SELECT id FROM points ORDER BY pos <-> '[1.5, 1.5]' LIMIT 3").unwrap();
        assert_eq!(r.rows.len(), 3);
        // [1,1] and [2,2] are equidistant (0.707) from [1.5,1.5] — tie order is
        // implementation-defined (HNSW vs brute-force may differ).
        let ids: Vec<String> = r.rows.iter().filter_map(|row| row[0].clone()).collect();
        assert!(ids.contains(&"2".to_string())); // [1,1]
        assert!(ids.contains(&"3".to_string())); // [2,2]
        assert!(ids.contains(&"1".to_string())); // [0,0] — closer than [10,10]
        assert!(!ids.contains(&"4".to_string())); // [10,10] is farthest
    }

    // ── Bug fix regression tests ─────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn hash_join_null_keys() {
        setup();
        execute("CREATE TABLE a (id int, val text)").unwrap();
        execute("CREATE TABLE b (id int, data text)").unwrap();
        execute("INSERT INTO a VALUES (1, 'x')").unwrap();
        execute("INSERT INTO a VALUES (NULL, 'z')").unwrap();
        execute("INSERT INTO b VALUES (1, 'p')").unwrap();
        execute("INSERT INTO b VALUES (NULL, 'q')").unwrap();
        let r = execute("SELECT a.val, b.data FROM a JOIN b ON a.id = b.id").unwrap();
        assert_eq!(r.rows.len(), 1); // only id=1 matches, NOT NULL=NULL
        assert_eq!(r.rows[0][0], Some("x".into()));
        assert_eq!(r.rows[0][1], Some("p".into()));
    }

    #[test]
    #[serial_test::serial]
    fn left_join_null_keys() {
        setup();
        execute("CREATE TABLE a (id int, val text)").unwrap();
        execute("CREATE TABLE b (id int, data text)").unwrap();
        execute("INSERT INTO a VALUES (1, 'x')").unwrap();
        execute("INSERT INTO a VALUES (2, 'y')").unwrap();
        execute("INSERT INTO a VALUES (NULL, 'z')").unwrap();
        execute("INSERT INTO b VALUES (1, 'p')").unwrap();
        execute("INSERT INTO b VALUES (NULL, 'q')").unwrap();
        let r = execute("SELECT a.val, b.data FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.val").unwrap();
        assert_eq!(r.rows.len(), 3);
        // x matches p, y gets NULL, z gets NULL (NOT matched with q!)
    }

    #[test]
    #[serial_test::serial]
    fn aggregate_order_by() {
        setup();
        execute("CREATE TABLE sales (region text, amount int)").unwrap();
        execute("INSERT INTO sales VALUES ('east', 100)").unwrap();
        execute("INSERT INTO sales VALUES ('west', 200)").unwrap();
        execute("INSERT INTO sales VALUES ('east', 150)").unwrap();
        let r = execute("SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY SUM(amount) DESC").unwrap();
        assert_eq!(r.rows[0][0], Some("east".into())); // east=250 > west=200
    }

    #[test]
    #[serial_test::serial]
    fn aggregate_limit() {
        setup();
        execute("CREATE TABLE t (grp text, val int)").unwrap();
        execute("INSERT INTO t VALUES ('a', 1)").unwrap();
        execute("INSERT INTO t VALUES ('b', 2)").unwrap();
        execute("INSERT INTO t VALUES ('c', 3)").unwrap();
        let r = execute("SELECT grp, COUNT(*) FROM t GROUP BY grp LIMIT 2").unwrap();
        assert_eq!(r.rows.len(), 2);
    }

    #[test]
    #[serial_test::serial]
    fn multi_row_insert_atomic() {
        setup();
        execute("CREATE TABLE t (id int PRIMARY KEY)").unwrap();
        let err = execute("INSERT INTO t VALUES (1), (1)"); // duplicate in same batch
        assert!(err.is_err());
        // Table should be empty — nothing committed
        let r = execute("SELECT * FROM t").unwrap();
        assert_eq!(r.rows.len(), 0);
    }

    #[test]
    #[serial_test::serial]
    fn update_intra_batch_uniqueness() {
        setup();
        execute("CREATE TABLE t (id int PRIMARY KEY, name text)").unwrap();
        execute("INSERT INTO t VALUES (1, 'a')").unwrap();
        execute("INSERT INTO t VALUES (2, 'b')").unwrap();
        let err = execute("UPDATE t SET id = 5"); // both rows get id=5 — should error
        assert!(err.is_err());
        // Original data should be unchanged
        let r = execute("SELECT * FROM t ORDER BY id").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("1".into()));
    }

    // ── HNSW index tests ──────────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn hnsw_knn_basic() {
        setup();
        execute("CREATE TABLE items (id int, embedding vector)").unwrap();
        // Insert vectors — HNSW index is auto-created on first insert
        for i in 0..50 {
            let v: Vec<f32> = (0..8)
                .map(|d| ((i * 7 + d * 3) as f32 * 0.02) % 1.0)
                .collect();
            let vstr = format!(
                "[{}]",
                v.iter()
                    .map(|f| format!("{:.4}", f))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            execute(&format!("INSERT INTO items VALUES ({}, '{}')", i, vstr)).unwrap();
        }
        // KNN search should use HNSW
        let r = execute(
            "SELECT id FROM items ORDER BY embedding <-> '[0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5]' LIMIT 5",
        )
        .unwrap();
        assert_eq!(r.rows.len(), 5);
    }

    #[test]
    #[serial_test::serial]
    fn hnsw_knn_returns_closest() {
        setup();
        execute("CREATE TABLE pts (id int, pos vector)").unwrap();
        execute("INSERT INTO pts VALUES (1, '[0.0, 0.0]')").unwrap();
        execute("INSERT INTO pts VALUES (2, '[1.0, 0.0]')").unwrap();
        execute("INSERT INTO pts VALUES (3, '[0.0, 1.0]')").unwrap();
        execute("INSERT INTO pts VALUES (4, '[10.0, 10.0]')").unwrap();
        // Nearest to [0.1, 0.1] should be id=1
        let r = execute("SELECT id FROM pts ORDER BY pos <-> '[0.1, 0.1]' LIMIT 2").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("1".into()));
    }

    #[test]
    #[serial_test::serial]
    fn hnsw_recall_test() {
        setup();
        execute("CREATE TABLE recall_t (id int, emb vector)").unwrap();
        let dim = 8;
        let n = 200;
        let mut vectors: Vec<Vec<f32>> = Vec::new();
        for i in 0..n {
            let v: Vec<f32> = (0..dim)
                .map(|d| ((i * 13 + d * 7) as f32 * 0.005) % 1.0)
                .collect();
            let vstr = format!(
                "[{}]",
                v.iter()
                    .map(|f| format!("{:.6}", f))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            execute(&format!("INSERT INTO recall_t VALUES ({}, '{}')", i, vstr)).unwrap();
            vectors.push(v);
        }
        let query = vec![0.5f32; dim];
        let k = 10;

        // Brute force ground truth
        let mut brute: Vec<(f32, usize)> = vectors
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let d: f32 = v
                    .iter()
                    .zip(query.iter())
                    .map(|(a, b)| (a - b).powi(2))
                    .sum::<f32>()
                    .sqrt();
                (d, i)
            })
            .collect();
        brute.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        let truth: std::collections::HashSet<String> = brute
            .iter()
            .take(k)
            .map(|(_, id)| id.to_string())
            .collect();

        // HNSW result
        let qstr = format!(
            "[{}]",
            query
                .iter()
                .map(|f| format!("{:.6}", f))
                .collect::<Vec<_>>()
                .join(",")
        );
        let r = execute(&format!(
            "SELECT id FROM recall_t ORDER BY emb <-> '{}' LIMIT {}",
            qstr, k
        ))
        .unwrap();
        let hnsw_ids: std::collections::HashSet<String> = r
            .rows
            .iter()
            .filter_map(|row| row[0].clone())
            .collect();

        let overlap = truth.intersection(&hnsw_ids).count();
        let recall = overlap as f32 / k as f32;
        assert!(
            recall >= 0.7,
            "HNSW recall {:.0}% ({}/{}) is below 70% threshold",
            recall * 100.0,
            overlap,
            k
        );
    }

    #[test]
    #[serial_test::serial]
    fn hnsw_insert_correct_row_ids() {
        setup();
        execute("CREATE TABLE vec_t (id INT, v VECTOR)").unwrap();
        execute("INSERT INTO vec_t VALUES (1, '[1,0,0]'), (2, '[0,1,0]'), (3, '[0,0,1]')").unwrap();
        let r = execute("SELECT id FROM vec_t ORDER BY v <-> '[1,0,0]' LIMIT 1").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("1".to_string()));
    }

    #[test]
    #[serial_test::serial]
    fn pk_null_rejected_at_storage() {
        setup();
        execute("CREATE TABLE pk_t (id INT PRIMARY KEY, name TEXT)").unwrap();
        let r = execute("INSERT INTO pk_t VALUES (NULL, 'test')");
        assert!(r.is_err());
    }

    #[test]
    #[serial_test::serial]
    fn default_function_error() {
        setup();
        let r = execute("CREATE TABLE df_t (id INT, created_at TEXT DEFAULT now())");
        assert!(r.is_err());
        assert!(r.unwrap_err().contains("not yet supported"));
    }

    // ── LIKE / ILIKE tests ──────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn like_percent_wildcard() {
        setup();
        execute("CREATE TABLE likes (name TEXT)").unwrap();
        execute("INSERT INTO likes VALUES ('alice'), ('bob'), ('alicia'), ('charlie')").unwrap();
        let r = execute("SELECT name FROM likes WHERE name LIKE 'ali%' ORDER BY name").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("alice".into()));
        assert_eq!(r.rows[1][0], Some("alicia".into()));
    }

    #[test]
    #[serial_test::serial]
    fn like_underscore_wildcard() {
        setup();
        execute("CREATE TABLE like2 (code TEXT)").unwrap();
        execute("INSERT INTO like2 VALUES ('A1'), ('A2'), ('AB'), ('A12')").unwrap();
        let r = execute("SELECT code FROM like2 WHERE code LIKE 'A_' ORDER BY code").unwrap();
        assert_eq!(r.rows.len(), 3);
        assert_eq!(r.rows[0][0], Some("A1".into()));
        assert_eq!(r.rows[1][0], Some("A2".into()));
        assert_eq!(r.rows[2][0], Some("AB".into()));
    }

    #[test]
    #[serial_test::serial]
    fn not_like() {
        setup();
        execute("CREATE TABLE like3 (name TEXT)").unwrap();
        execute("INSERT INTO like3 VALUES ('foo'), ('bar'), ('foobar')").unwrap();
        let r = execute("SELECT name FROM like3 WHERE name NOT LIKE 'foo%' ORDER BY name").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("bar".into()));
    }

    #[test]
    #[serial_test::serial]
    fn ilike_case_insensitive() {
        setup();
        execute("CREATE TABLE like4 (name TEXT)").unwrap();
        execute("INSERT INTO like4 VALUES ('Alice'), ('BOB'), ('alice')").unwrap();
        let r = execute("SELECT name FROM like4 WHERE name ILIKE 'alice' ORDER BY name").unwrap();
        assert_eq!(r.rows.len(), 2);
        // PostgreSQL: case-insensitive match returns both Alice and alice
    }

    #[test]
    #[serial_test::serial]
    fn like_null_propagation() {
        setup();
        execute("CREATE TABLE like5 (name TEXT)").unwrap();
        execute("INSERT INTO like5 VALUES ('a'), (NULL)").unwrap();
        let r = execute("SELECT name FROM like5 WHERE name LIKE '%'").unwrap();
        assert_eq!(r.rows.len(), 1); // NULL LIKE '%' = NULL, excluded from WHERE
    }

    // ── CASE WHEN tests ─────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn case_searched() {
        setup();
        execute("CREATE TABLE scores (name TEXT, score INT)").unwrap();
        execute("INSERT INTO scores VALUES ('a', 90), ('b', 60), ('c', 40)").unwrap();
        let r = execute(
            "SELECT name, CASE WHEN score >= 80 THEN 'A' WHEN score >= 50 THEN 'B' ELSE 'F' END FROM scores ORDER BY name"
        ).unwrap();
        assert_eq!(r.rows[0][1], Some("A".into()));
        assert_eq!(r.rows[1][1], Some("B".into()));
        assert_eq!(r.rows[2][1], Some("F".into()));
    }

    #[test]
    #[serial_test::serial]
    fn case_simple() {
        setup();
        execute("CREATE TABLE status (code INT)").unwrap();
        execute("INSERT INTO status VALUES (1), (2), (3)").unwrap();
        let r = execute(
            "SELECT CASE code WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'other' END FROM status ORDER BY code"
        ).unwrap();
        assert_eq!(r.rows[0][0], Some("one".into()));
        assert_eq!(r.rows[1][0], Some("two".into()));
        assert_eq!(r.rows[2][0], Some("other".into()));
    }

    #[test]
    #[serial_test::serial]
    fn case_no_else_returns_null() {
        setup();
        execute("CREATE TABLE ce (x INT)").unwrap();
        execute("INSERT INTO ce VALUES (1), (99)").unwrap();
        let r = execute("SELECT CASE WHEN x = 1 THEN 'yes' END FROM ce ORDER BY x").unwrap();
        assert_eq!(r.rows[0][0], Some("yes".into()));
        assert_eq!(r.rows[1][0], None); // no ELSE → NULL
    }

    #[test]
    #[serial_test::serial]
    fn case_in_where() {
        setup();
        execute("CREATE TABLE cw (x INT)").unwrap();
        execute("INSERT INTO cw VALUES (1), (2), (3)").unwrap();
        let r = execute(
            "SELECT x FROM cw WHERE CASE WHEN x > 1 THEN true ELSE false END ORDER BY x"
        ).unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("2".into()));
        assert_eq!(r.rows[1][0], Some("3".into()));
    }

    // ── DISTINCT tests ──────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn distinct_basic() {
        setup();
        execute("CREATE TABLE dup (color TEXT)").unwrap();
        execute("INSERT INTO dup VALUES ('red'), ('blue'), ('red'), ('green'), ('blue')").unwrap();
        let r = execute("SELECT DISTINCT color FROM dup ORDER BY color").unwrap();
        assert_eq!(r.rows.len(), 3);
        assert_eq!(r.rows[0][0], Some("blue".into()));
        assert_eq!(r.rows[1][0], Some("green".into()));
        assert_eq!(r.rows[2][0], Some("red".into()));
    }

    #[test]
    #[serial_test::serial]
    fn distinct_with_null() {
        setup();
        execute("CREATE TABLE dup2 (x INT)").unwrap();
        execute("INSERT INTO dup2 VALUES (1), (NULL), (2), (NULL), (1)").unwrap();
        let r = execute("SELECT DISTINCT x FROM dup2 ORDER BY x").unwrap();
        // PostgreSQL: NULL groups as one, ORDER BY puts NULLs last
        // We should have 3 distinct values: 1, 2, NULL
        assert_eq!(r.rows.len(), 3);
    }

    #[test]
    #[serial_test::serial]
    fn distinct_multi_column() {
        setup();
        execute("CREATE TABLE dup3 (a INT, b TEXT)").unwrap();
        execute("INSERT INTO dup3 VALUES (1, 'x'), (1, 'y'), (1, 'x'), (2, 'x')").unwrap();
        let r = execute("SELECT DISTINCT a, b FROM dup3 ORDER BY a, b").unwrap();
        assert_eq!(r.rows.len(), 3); // (1,x), (1,y), (2,x)
    }

    #[test]
    #[serial_test::serial]
    fn like_escaped_percent() {
        setup();
        execute("CREATE TABLE like6 (s TEXT)").unwrap();
        execute("INSERT INTO like6 VALUES ('100%'), ('100x'), ('100')").unwrap();
        let r = execute(r#"SELECT s FROM like6 WHERE s LIKE '100\%'"#).unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("100%".into()));
    }

    #[test]
    #[serial_test::serial]
    fn distinct_with_limit() {
        setup();
        execute("CREATE TABLE dup5 (x INT)").unwrap();
        execute("INSERT INTO dup5 VALUES (1), (1), (2), (2), (3), (3)").unwrap();
        // DISTINCT first (3 unique), then LIMIT 2
        let r = execute("SELECT DISTINCT x FROM dup5 ORDER BY x LIMIT 2").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[1][0], Some("2".into()));
    }

    #[test]
    #[serial_test::serial]
    fn distinct_preserves_order() {
        setup();
        execute("CREATE TABLE dup4 (x INT)").unwrap();
        execute("INSERT INTO dup4 VALUES (3), (1), (2), (1), (3)").unwrap();
        let r = execute("SELECT DISTINCT x FROM dup4 ORDER BY x").unwrap();
        assert_eq!(r.rows.len(), 3);
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[1][0], Some("2".into()));
        assert_eq!(r.rows[2][0], Some("3".into()));
    }

    // ═══════════════════════════════════════════════════════════════
    // SPEC TESTS — SQL Completeness Target
    // Each test below defines a PostgreSQL behavior we must match.
    // ═══════════════════════════════════════════════════════════════

    // ── ALTER TABLE ─────────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn alter_table_add_column() {
        setup();
        execute("CREATE TABLE alt1 (id INT, name TEXT)").unwrap();
        execute("INSERT INTO alt1 VALUES (1, 'alice')").unwrap();
        execute("ALTER TABLE alt1 ADD COLUMN age INT").unwrap();
        let r = execute("SELECT id, name, age FROM alt1").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][2], None); // new column is NULL for existing rows
    }

    #[test]
    #[serial_test::serial]
    fn alter_table_add_column_with_default() {
        setup();
        execute("CREATE TABLE alt2 (id INT)").unwrap();
        execute("INSERT INTO alt2 VALUES (1), (2)").unwrap();
        execute("ALTER TABLE alt2 ADD COLUMN status TEXT DEFAULT 'active'").unwrap();
        let r = execute("SELECT id, status FROM alt2 ORDER BY id").unwrap();
        assert_eq!(r.rows[0][1], Some("active".into()));
        assert_eq!(r.rows[1][1], Some("active".into()));
    }

    #[test]
    #[serial_test::serial]
    fn alter_table_drop_column() {
        setup();
        execute("CREATE TABLE alt3 (id INT, name TEXT, age INT)").unwrap();
        execute("INSERT INTO alt3 VALUES (1, 'alice', 30)").unwrap();
        execute("ALTER TABLE alt3 DROP COLUMN age").unwrap();
        let r = execute("SELECT * FROM alt3").unwrap();
        assert_eq!(r.columns.len(), 2);
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[0][1], Some("alice".into()));
    }

    #[test]
    #[serial_test::serial]
    fn alter_table_rename_column() {
        setup();
        execute("CREATE TABLE alt4 (id INT, name TEXT)").unwrap();
        execute("INSERT INTO alt4 VALUES (1, 'alice')").unwrap();
        execute("ALTER TABLE alt4 RENAME COLUMN name TO full_name").unwrap();
        let r = execute("SELECT full_name FROM alt4").unwrap();
        assert_eq!(r.rows[0][0], Some("alice".into()));
    }

    #[test]
    #[serial_test::serial]
    fn alter_table_rename_table() {
        setup();
        execute("CREATE TABLE alt5 (id INT)").unwrap();
        execute("INSERT INTO alt5 VALUES (1)").unwrap();
        execute("ALTER TABLE alt5 RENAME TO alt5_renamed").unwrap();
        let r = execute("SELECT id FROM alt5_renamed").unwrap();
        assert_eq!(r.rows[0][0], Some("1".into()));
    }

    // ── Type Casting ────────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn cast_text_to_int() {
        setup();
        let r = execute("SELECT CAST('42' AS INT)").unwrap();
        assert_eq!(r.rows[0][0], Some("42".into()));
    }

    #[test]
    #[serial_test::serial]
    fn cast_int_to_text() {
        setup();
        let r = execute("SELECT CAST(42 AS TEXT)").unwrap();
        assert_eq!(r.rows[0][0], Some("42".into()));
    }

    #[test]
    #[serial_test::serial]
    fn cast_float_to_int_rounds() {
        setup();
        // PostgreSQL: CAST(3.7 AS INT) = 4 (rounds)
        let r = execute("SELECT CAST(3.7 AS INT)").unwrap();
        assert_eq!(r.rows[0][0], Some("4".into()));
    }

    #[test]
    #[serial_test::serial]
    fn cast_shorthand_syntax() {
        setup();
        let r = execute("SELECT '123'::INT").unwrap();
        assert_eq!(r.rows[0][0], Some("123".into()));
    }

    #[test]
    #[serial_test::serial]
    fn cast_int_to_float() {
        setup();
        let r = execute("SELECT 42::FLOAT8").unwrap();
        // Should be a float representation
        let val = r.rows[0][0].as_ref().unwrap();
        assert!(val == "42" || val == "42.0");
    }

    #[test]
    #[serial_test::serial]
    fn cast_bool_to_text() {
        setup();
        let r = execute("SELECT true::TEXT").unwrap();
        assert_eq!(r.rows[0][0], Some("true".into()));
    }

    #[test]
    #[serial_test::serial]
    fn cast_in_where() {
        setup();
        execute("CREATE TABLE cast_t (val TEXT)").unwrap();
        execute("INSERT INTO cast_t VALUES ('10'), ('20'), ('3')").unwrap();
        let r = execute("SELECT val FROM cast_t WHERE val::INT > 5 ORDER BY val::INT").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("10".into()));
        assert_eq!(r.rows[1][0], Some("20".into()));
    }

    // ── BETWEEN ─────────────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn between_basic() {
        setup();
        execute("CREATE TABLE bet1 (x INT)").unwrap();
        execute("INSERT INTO bet1 VALUES (1), (5), (10), (15), (20)").unwrap();
        let r = execute("SELECT x FROM bet1 WHERE x BETWEEN 5 AND 15 ORDER BY x").unwrap();
        assert_eq!(r.rows.len(), 3);
        assert_eq!(r.rows[0][0], Some("5".into()));
        assert_eq!(r.rows[1][0], Some("10".into()));
        assert_eq!(r.rows[2][0], Some("15".into()));
    }

    #[test]
    #[serial_test::serial]
    fn not_between() {
        setup();
        execute("CREATE TABLE bet2 (x INT)").unwrap();
        execute("INSERT INTO bet2 VALUES (1), (5), (10)").unwrap();
        let r = execute("SELECT x FROM bet2 WHERE x NOT BETWEEN 3 AND 7 ORDER BY x").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[1][0], Some("10".into()));
    }

    // ── COALESCE / NULLIF ───────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn coalesce_basic() {
        setup();
        execute("CREATE TABLE coal (a INT, b INT, c INT)").unwrap();
        execute("INSERT INTO coal VALUES (NULL, NULL, 3)").unwrap();
        execute("INSERT INTO coal VALUES (NULL, 2, 3)").unwrap();
        execute("INSERT INTO coal VALUES (1, 2, 3)").unwrap();
        let r = execute("SELECT COALESCE(a, b, c) FROM coal ORDER BY a, b, c").unwrap();
        // Row 1: COALESCE(1,2,3) = 1
        // Row 2: COALESCE(NULL,2,3) = 2
        // Row 3: COALESCE(NULL,NULL,3) = 3
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[1][0], Some("2".into()));
        assert_eq!(r.rows[2][0], Some("3".into()));
    }

    #[test]
    #[serial_test::serial]
    fn coalesce_all_null() {
        setup();
        let r = execute("SELECT COALESCE(NULL, NULL, NULL)").unwrap();
        assert_eq!(r.rows[0][0], None);
    }

    #[test]
    #[serial_test::serial]
    fn nullif_equal() {
        setup();
        // NULLIF(a, b) returns NULL if a = b, else a
        let r = execute("SELECT NULLIF(5, 5)").unwrap();
        assert_eq!(r.rows[0][0], None);
    }

    #[test]
    #[serial_test::serial]
    fn nullif_not_equal() {
        setup();
        let r = execute("SELECT NULLIF(5, 3)").unwrap();
        assert_eq!(r.rows[0][0], Some("5".into()));
    }

    // ── String Functions ────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn substring_from_for() {
        setup();
        // SUBSTRING('hello' FROM 2 FOR 3) = 'ell'
        let r = execute("SELECT SUBSTRING('hello' FROM 2 FOR 3)").unwrap();
        assert_eq!(r.rows[0][0], Some("ell".into()));
    }

    #[test]
    #[serial_test::serial]
    fn substring_from_only() {
        setup();
        // SUBSTRING('hello' FROM 3) = 'llo'
        let r = execute("SELECT SUBSTRING('hello' FROM 3)").unwrap();
        assert_eq!(r.rows[0][0], Some("llo".into()));
    }

    #[test]
    #[serial_test::serial]
    fn trim_basic() {
        setup();
        let r = execute("SELECT TRIM('  hello  ')").unwrap();
        assert_eq!(r.rows[0][0], Some("hello".into()));
    }

    #[test]
    #[serial_test::serial]
    fn trim_leading_trailing() {
        setup();
        let r = execute("SELECT TRIM(LEADING ' ' FROM '  hello  ')").unwrap();
        assert_eq!(r.rows[0][0], Some("hello  ".into()));
    }

    #[test]
    #[serial_test::serial]
    fn replace_function() {
        setup();
        let r = execute("SELECT REPLACE('hello world', 'world', 'rust')").unwrap();
        assert_eq!(r.rows[0][0], Some("hello rust".into()));
    }

    #[test]
    #[serial_test::serial]
    fn position_function() {
        setup();
        // POSITION('lo' IN 'hello') = 4 (1-based)
        let r = execute("SELECT POSITION('lo' IN 'hello')").unwrap();
        assert_eq!(r.rows[0][0], Some("4".into()));
    }

    #[test]
    #[serial_test::serial]
    fn left_right_functions() {
        setup();
        let r1 = execute("SELECT LEFT('hello', 3)").unwrap();
        assert_eq!(r1.rows[0][0], Some("hel".into()));
        let r2 = execute("SELECT RIGHT('hello', 3)").unwrap();
        assert_eq!(r2.rows[0][0], Some("llo".into()));
    }

    // ── Math Functions ──────────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn ceil_floor_round() {
        setup();
        let r1 = execute("SELECT CEIL(4.2)").unwrap();
        assert_eq!(r1.rows[0][0], Some("5".into()));
        let r2 = execute("SELECT FLOOR(4.8)").unwrap();
        assert_eq!(r2.rows[0][0], Some("4".into()));
        let r3 = execute("SELECT ROUND(4.567, 2)").unwrap();
        assert_eq!(r3.rows[0][0], Some("4.57".into()));
    }

    #[test]
    #[serial_test::serial]
    fn mod_function() {
        setup();
        let r = execute("SELECT MOD(10, 3)").unwrap();
        assert_eq!(r.rows[0][0], Some("1".into()));
    }

    #[test]
    #[serial_test::serial]
    fn power_sqrt() {
        setup();
        let r1 = execute("SELECT POWER(2, 10)").unwrap();
        assert_eq!(r1.rows[0][0], Some("1024".into()));
        let r2 = execute("SELECT SQRT(144)").unwrap();
        assert_eq!(r2.rows[0][0], Some("12".into()));
    }

    // ── Aggregate Enhancements ──────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn count_distinct() {
        setup();
        execute("CREATE TABLE cd (color TEXT)").unwrap();
        execute("INSERT INTO cd VALUES ('red'), ('blue'), ('red'), ('green'), ('blue')").unwrap();
        let r = execute("SELECT COUNT(DISTINCT color) FROM cd").unwrap();
        assert_eq!(r.rows[0][0], Some("3".into()));
    }

    #[test]
    #[serial_test::serial]
    fn sum_distinct() {
        setup();
        execute("CREATE TABLE sd (x INT)").unwrap();
        execute("INSERT INTO sd VALUES (1), (2), (2), (3), (3), (3)").unwrap();
        let r = execute("SELECT SUM(DISTINCT x) FROM sd").unwrap();
        assert_eq!(r.rows[0][0], Some("6".into())); // 1+2+3
    }

    #[test]
    #[serial_test::serial]
    fn avg_function() {
        setup();
        execute("CREATE TABLE av (x INT)").unwrap();
        execute("INSERT INTO av VALUES (10), (20), (30)").unwrap();
        let r = execute("SELECT AVG(x) FROM av").unwrap();
        assert_eq!(r.rows[0][0], Some("20".into()));
    }

    #[test]
    #[serial_test::serial]
    fn string_agg() {
        setup();
        execute("CREATE TABLE sa (name TEXT)").unwrap();
        execute("INSERT INTO sa VALUES ('a'), ('b'), ('c')").unwrap();
        let r = execute("SELECT STRING_AGG(name, ', ' ORDER BY name) FROM sa").unwrap();
        assert_eq!(r.rows[0][0], Some("a, b, c".into()));
    }

    #[test]
    #[serial_test::serial]
    fn bool_and_or() {
        setup();
        execute("CREATE TABLE ba (x BOOL)").unwrap();
        execute("INSERT INTO ba VALUES (true), (true), (false)").unwrap();
        let r1 = execute("SELECT BOOL_AND(x) FROM ba").unwrap();
        assert_eq!(r1.rows[0][0], Some("f".into()));
        let r2 = execute("SELECT BOOL_OR(x) FROM ba").unwrap();
        assert_eq!(r2.rows[0][0], Some("t".into()));
    }

    // ── UNION / UNION ALL / INTERSECT / EXCEPT ──────────────────

    #[test]
    #[serial_test::serial]
    fn union_all() {
        setup();
        execute("CREATE TABLE u1 (x INT)").unwrap();
        execute("CREATE TABLE u2 (x INT)").unwrap();
        execute("INSERT INTO u1 VALUES (1), (2)").unwrap();
        execute("INSERT INTO u2 VALUES (2), (3)").unwrap();
        let r = execute("SELECT x FROM u1 UNION ALL SELECT x FROM u2 ORDER BY x").unwrap();
        assert_eq!(r.rows.len(), 4); // 1, 2, 2, 3
    }

    #[test]
    #[serial_test::serial]
    fn union_dedup() {
        setup();
        execute("CREATE TABLE u3 (x INT)").unwrap();
        execute("CREATE TABLE u4 (x INT)").unwrap();
        execute("INSERT INTO u3 VALUES (1), (2)").unwrap();
        execute("INSERT INTO u4 VALUES (2), (3)").unwrap();
        let r = execute("SELECT x FROM u3 UNION SELECT x FROM u4 ORDER BY x").unwrap();
        assert_eq!(r.rows.len(), 3); // 1, 2, 3 (deduped)
    }

    #[test]
    #[serial_test::serial]
    fn intersect_basic() {
        setup();
        execute("CREATE TABLE i1 (x INT)").unwrap();
        execute("CREATE TABLE i2 (x INT)").unwrap();
        execute("INSERT INTO i1 VALUES (1), (2), (3)").unwrap();
        execute("INSERT INTO i2 VALUES (2), (3), (4)").unwrap();
        let r = execute("SELECT x FROM i1 INTERSECT SELECT x FROM i2 ORDER BY x").unwrap();
        assert_eq!(r.rows.len(), 2); // 2, 3
    }

    #[test]
    #[serial_test::serial]
    fn except_basic() {
        setup();
        execute("CREATE TABLE e1 (x INT)").unwrap();
        execute("CREATE TABLE e2 (x INT)").unwrap();
        execute("INSERT INTO e1 VALUES (1), (2), (3)").unwrap();
        execute("INSERT INTO e2 VALUES (2), (3), (4)").unwrap();
        let r = execute("SELECT x FROM e1 EXCEPT SELECT x FROM e2 ORDER BY x").unwrap();
        assert_eq!(r.rows.len(), 1); // 1
        assert_eq!(r.rows[0][0], Some("1".into()));
    }

    // ── Subquery Enhancements ───────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn subquery_in_select_list() {
        setup();
        execute("CREATE TABLE sq1 (id INT, name TEXT)").unwrap();
        execute("CREATE TABLE sq2 (user_id INT, score INT)").unwrap();
        execute("INSERT INTO sq1 VALUES (1, 'alice'), (2, 'bob')").unwrap();
        execute("INSERT INTO sq2 VALUES (1, 100), (1, 200), (2, 50)").unwrap();
        let r = execute(
            "SELECT name, (SELECT SUM(score) FROM sq2 WHERE sq2.user_id = sq1.id) FROM sq1 ORDER BY name"
        ).unwrap();
        assert_eq!(r.rows[0][0], Some("alice".into()));
        assert_eq!(r.rows[0][1], Some("300".into()));
        assert_eq!(r.rows[1][0], Some("bob".into()));
        assert_eq!(r.rows[1][1], Some("50".into()));
    }

    // ── Expression Enhancements ─────────────────────────────────

    #[test]
    #[serial_test::serial]
    #[ignore = "ORDER BY alias resolution not yet implemented"]
    fn expression_aliases_in_order_by() {
        setup();
        execute("CREATE TABLE ea (price INT, qty INT)").unwrap();
        execute("INSERT INTO ea VALUES (10, 5), (20, 2), (5, 10)").unwrap();
        let r = execute("SELECT price * qty AS total FROM ea ORDER BY total").unwrap();
        assert_eq!(r.rows[0][0], Some("40".into()));
        assert_eq!(r.rows[1][0], Some("50".into()));
        assert_eq!(r.rows[2][0], Some("50".into()));
    }

    #[test]
    #[serial_test::serial]
    fn negative_literal() {
        setup();
        let r = execute("SELECT -5").unwrap();
        assert_eq!(r.rows[0][0], Some("-5".into()));
    }

    #[test]
    #[serial_test::serial]
    fn modulo_operator() {
        setup();
        let r = execute("SELECT 10 % 3").unwrap();
        assert_eq!(r.rows[0][0], Some("1".into()));
    }

    // ── INSERT ... SELECT ───────────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn insert_select() {
        setup();
        execute("CREATE TABLE src (x INT)").unwrap();
        execute("CREATE TABLE dst (x INT)").unwrap();
        execute("INSERT INTO src VALUES (1), (2), (3)").unwrap();
        execute("INSERT INTO dst SELECT x FROM src WHERE x > 1").unwrap();
        let r = execute("SELECT x FROM dst ORDER BY x").unwrap();
        assert_eq!(r.rows.len(), 2);
        assert_eq!(r.rows[0][0], Some("2".into()));
        assert_eq!(r.rows[1][0], Some("3".into()));
    }

    // ── UPDATE with JOIN / FROM ─────────────────────────────────

    #[test]
    #[serial_test::serial]
    #[ignore = "UPDATE FROM not yet implemented"]
    fn update_from_join() {
        setup();
        execute("CREATE TABLE prices (id INT, price INT)").unwrap();
        execute("CREATE TABLE discounts (id INT, discount INT)").unwrap();
        execute("INSERT INTO prices VALUES (1, 100), (2, 200)").unwrap();
        execute("INSERT INTO discounts VALUES (1, 10), (2, 20)").unwrap();
        execute("UPDATE prices SET price = prices.price - discounts.discount FROM discounts WHERE prices.id = discounts.id").unwrap();
        let r = execute("SELECT id, price FROM prices ORDER BY id").unwrap();
        assert_eq!(r.rows[0][1], Some("90".into()));
        assert_eq!(r.rows[1][1], Some("180".into()));
    }

    // ── DELETE with USING ───────────────────────────────────────

    #[test]
    #[serial_test::serial]
    #[ignore = "DELETE USING not yet implemented"]
    fn delete_using() {
        setup();
        execute("CREATE TABLE items (id INT, name TEXT)").unwrap();
        execute("CREATE TABLE blacklist (name TEXT)").unwrap();
        execute("INSERT INTO items VALUES (1, 'good'), (2, 'bad'), (3, 'ugly')").unwrap();
        execute("INSERT INTO blacklist VALUES ('bad'), ('ugly')").unwrap();
        execute("DELETE FROM items USING blacklist WHERE items.name = blacklist.name").unwrap();
        let r = execute("SELECT name FROM items").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0], Some("good".into()));
    }

    // ── CREATE TABLE AS / SELECT INTO ───────────────────────────

    #[test]
    #[serial_test::serial]
    fn create_table_as_select() {
        setup();
        execute("CREATE TABLE ctas_src (id INT, name TEXT)").unwrap();
        execute("INSERT INTO ctas_src VALUES (1, 'alice'), (2, 'bob')").unwrap();
        execute("CREATE TABLE ctas_dst AS SELECT * FROM ctas_src WHERE id = 1").unwrap();
        let r = execute("SELECT id, name FROM ctas_dst").unwrap();
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][1], Some("alice".into()));
    }

    // ── IF NOT EXISTS / IF EXISTS ───────────────────────────────

    #[test]
    #[serial_test::serial]
    fn create_table_if_not_exists() {
        setup();
        execute("CREATE TABLE ine (id INT)").unwrap();
        // Should not error
        execute("CREATE TABLE IF NOT EXISTS ine (id INT)").unwrap();
        execute("INSERT INTO ine VALUES (1)").unwrap();
        let r = execute("SELECT id FROM ine").unwrap();
        assert_eq!(r.rows.len(), 1);
    }

    #[test]
    #[serial_test::serial]
    fn drop_table_if_exists() {
        setup();
        // Should not error even if table doesn't exist
        execute("DROP TABLE IF EXISTS nonexistent").unwrap();
    }

    // ── Multiple DEFAULT expressions ────────────────────────────

    #[test]
    #[serial_test::serial]
    fn insert_default_keyword() {
        setup();
        execute("CREATE TABLE def1 (id SERIAL, name TEXT DEFAULT 'unnamed')").unwrap();
        execute("INSERT INTO def1 (name) VALUES (DEFAULT)").unwrap();
        let r = execute("SELECT id, name FROM def1").unwrap();
        assert_eq!(r.rows[0][0], Some("1".into()));
        assert_eq!(r.rows[0][1], Some("unnamed".into()));
    }

    // ── Column aliases in SELECT ────────────────────────────────

    #[test]
    #[serial_test::serial]
    fn column_alias() {
        setup();
        execute("CREATE TABLE ca (first_name TEXT)").unwrap();
        execute("INSERT INTO ca VALUES ('alice')").unwrap();
        let r = execute("SELECT first_name AS name FROM ca").unwrap();
        assert_eq!(r.columns[0].0, "name");
        assert_eq!(r.rows[0][0], Some("alice".into()));
    }

}
