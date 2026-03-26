// TODO(#5): Describe Portal sends NoData — requires parsing SQL to determine
//   column types without execution. Needs a type-inference pass over the AST
//   to return RowDescription for prepared statements. Phase 2 work.
//
// TODO(#8): O(N) uniqueness check on INSERT/UPDATE — needs hash indexes on
//   unique columns for O(1) constraint validation. Phase 2 work (index engine).

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use parking_lot::RwLock;
use pg_query::NodeEnum;
use serde::Serialize;

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
    for elt in &create.table_elts {
        let node = elt.node.as_ref().ok_or("missing table element")?;
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
                crate::sequence::create_sequence(schema, &seq_name, 1, 1)?;
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
                    eval_expr(expr, row, &ctx)?
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
        for values_list in &sel.values_lists {
            if let Some(NodeEnum::List(list)) = values_list.node.as_ref() {
                // Only apply defaults for columns NOT explicitly specified.
                // This avoids wasting sequence values on SERIAL columns
                // when the user provides an explicit value.
                let mut row: Vec<Value> = table_def
                    .columns
                    .iter()
                    .map(|col| {
                        if target_cols.contains(&col.name) {
                            Ok(Value::Null) // will be overwritten by explicit value below
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

                    row[col_idx] = eval_const(val_node.node.as_ref());
                }

                // Coerce text values to vector type where the column expects it
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
    }

    // Atomic batch insert: validate ALL rows against existing data AND each other,
    // then insert all at once. If any row fails, nothing is committed. (#4)
    let (unique_checks, pk_cols) = build_unique_checks(&table_def);
    let row_count = all_rows.len() as u64;
    let inserted_rows = if has_returning { all_rows.clone() } else { Vec::new() };

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

    // Collect vectors for HNSW insertion (before batch moves the rows)
    let hnsw_vectors: Vec<(usize, Vec<f32>)> = if let Some(col_idx) = vector_col_idx {
        if storage::has_hnsw_index(schema, table_name).is_some() {
            // Get current row count to compute row_ids for the new rows
            let base_row_id = storage::row_count(schema, table_name).unwrap_or(0) as usize;
            all_rows
                .iter()
                .enumerate()
                .filter_map(|(i, row)| {
                    if let Some(crate::types::Value::Vector(v)) = row.get(col_idx) {
                        Some((base_row_id + i, v.clone()))
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };

    storage::insert_batch_checked(
        schema, table_name, all_rows, &unique_checks, &pk_cols,
    )?;

    // Insert vectors into HNSW index after successful row insertion
    for (row_id, vector) in hnsw_vectors {
        let _ = storage::hnsw_insert(schema, table_name, row_id, vector);
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

fn eval_expr(node: &NodeEnum, row: &[Value], ctx: &JoinContext) -> Result<Value, String> {
    match node {
        NodeEnum::ColumnRef(cref) => {
            let idx = resolve_column(cref, ctx)?;
            if idx < row.len() {
                Ok(row[idx].clone())
            } else if ctx.total_columns == 0 {
                Ok(Value::Null) // no-FROM context (SELECT 1)
            } else {
                Err(format!(
                    "internal error: column index {} out of range for row of width {}",
                    idx, row.len()
                ))
            }
        }
        NodeEnum::AConst(ac) => {
            if ac.isnull {
                return Ok(Value::Null);
            }
            if let Some(val) = &ac.val {
                match val {
                    pg_query::protobuf::a_const::Val::Ival(i) => Ok(Value::Int(i.ival as i64)),
                    pg_query::protobuf::a_const::Val::Fval(f) => f
                        .fval
                        .parse::<f64>()
                        .map(Value::Float)
                        .map_err(|e| e.to_string()),
                    pg_query::protobuf::a_const::Val::Sval(s) => {
                        let trimmed = s.sval.trim();
                        if trimmed.starts_with('[') && trimmed.ends_with(']') {
                            parse_vector_literal(trimmed)
                        } else {
                            Ok(Value::Text(Arc::from(s.sval.as_str())))
                        }
                    }
                    pg_query::protobuf::a_const::Val::Bsval(s) => Ok(Value::Text(Arc::from(s.bsval.as_str()))),
                    pg_query::protobuf::a_const::Val::Boolval(b) => Ok(Value::Bool(b.boolval)),
                }
            } else {
                Ok(Value::Null)
            }
        }
        NodeEnum::Integer(i) => Ok(Value::Int(i.ival as i64)),
        NodeEnum::Float(f) => f
            .fval
            .parse::<f64>()
            .map(Value::Float)
            .map_err(|e| e.to_string()),
        NodeEnum::String(s) => {
            let trimmed = s.sval.trim();
            if trimmed.starts_with('[') && trimmed.ends_with(']') {
                parse_vector_literal(trimmed)
            } else {
                Ok(Value::Text(Arc::from(s.sval.as_str())))
            }
        }
        NodeEnum::TypeCast(tc) => {
            let inner = tc
                .arg
                .as_ref()
                .and_then(|a| a.node.as_ref())
                .ok_or("TypeCast missing arg")?;
            let val = eval_expr(inner, row, ctx)?;
            // Handle cast to vector type
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
                if type_name == "vector" {
                    if let Value::Text(s) = &val {
                        let trimmed = s.trim();
                        if trimmed.starts_with('[') && trimmed.ends_with(']') {
                            return parse_vector_literal(trimmed);
                        }
                    }
                }
            }
            Ok(val)
        }
        NodeEnum::AExpr(expr) => eval_a_expr(expr, row, ctx),
        NodeEnum::BoolExpr(bexpr) => eval_bool_expr(bexpr, row, ctx),
        NodeEnum::NullTest(nt) => {
            let inner = nt
                .arg
                .as_ref()
                .and_then(|a| a.node.as_ref())
                .ok_or("NullTest missing arg")?;
            let val = eval_expr(inner, row, ctx)?;
            let is_null = matches!(val, Value::Null);
            if nt.nulltesttype == pg_query::protobuf::NullTestType::IsNull as i32 {
                Ok(Value::Bool(is_null))
            } else {
                Ok(Value::Bool(!is_null))
            }
        }
        NodeEnum::FuncCall(fc) => eval_func_call(fc, row, ctx),
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
                            exec_select_raw(sel, Some((row, ctx)))?;
                        return Ok(Value::Bool(!inner_rows.is_empty()));
                    }

                    // ANY / IN (sub_link_type = AnySublink)
                    if sub_type == pg_query::protobuf::SubLinkType::AnySublink as i32 {
                        let test_node = sl
                            .testexpr
                            .as_ref()
                            .and_then(|n| n.node.as_ref())
                            .ok_or("IN subquery missing test expression")?;
                        let test_val = eval_expr(test_node, row, ctx)?;

                        let (cols, inner_rows) =
                            exec_select_raw(sel, Some((row, ctx)))?;

                        // Validate: subquery must return exactly one column
                        if !cols.is_empty() && cols.len() != 1 {
                            return Err("subquery must return only one column".into());
                        }

                        if matches!(test_val, Value::Null) {
                            return Ok(Value::Null);
                        }

                        let mut has_null = false;
                        for inner_row in &inner_rows {
                            let inner_val =
                                inner_row.first().cloned().unwrap_or(Value::Null);
                            if matches!(inner_val, Value::Null) {
                                has_null = true;
                                continue;
                            }
                            // Use compare() for type coercion (Int vs Float)
                            // and == for same-type equality
                            let is_eq = test_val == inner_val
                                || test_val.compare(&inner_val)
                                    == Some(std::cmp::Ordering::Equal);
                            if is_eq {
                                return Ok(Value::Bool(true));
                            }
                        }
                        return Ok(if has_null {
                            Value::Null
                        } else {
                            Value::Bool(false)
                        });
                    }

                    // ALL (sub_link_type = AllSublink)
                    if sub_type == pg_query::protobuf::SubLinkType::AllSublink as i32 {
                        let test_node = sl
                            .testexpr
                            .as_ref()
                            .and_then(|n| n.node.as_ref())
                            .ok_or("ALL subquery missing test expression")?;
                        let test_val = eval_expr(test_node, row, ctx)?;

                        let (_cols, inner_rows) =
                            exec_select_raw(sel, Some((row, ctx)))?;

                        // NULL op ALL(empty) = TRUE, NULL op ALL(non-empty) = NULL
                        if matches!(test_val, Value::Null) {
                            return Ok(if inner_rows.is_empty() {
                                Value::Bool(true)
                            } else {
                                Value::Null
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
                                inner_row.first().cloned().unwrap_or(Value::Null);
                            let cmp_result =
                                eval_comparison_op(&op, &test_val, &inner_val)?;
                            match cmp_result {
                                Value::Bool(true) => continue,
                                Value::Bool(false) => return Ok(Value::Bool(false)),
                                _ => has_null = true,
                            }
                        }
                        return Ok(if has_null { Value::Null } else { Value::Bool(true) });
                    }

                    // Scalar subquery (ExprSublink)
                    if sub_type == pg_query::protobuf::SubLinkType::ExprSublink as i32 {
                        let (_cols, inner_rows) =
                            exec_select_raw(sel, Some((row, ctx)))?;
                        if inner_rows.len() > 1 {
                            return Err("more than one row returned by a subquery used as an expression".into());
                        }
                        return Ok(inner_rows
                            .first()
                            .and_then(|r| r.first())
                            .cloned()
                            .unwrap_or(Value::Null));
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

fn eval_comparison_op(op: &str, left: &Value, right: &Value) -> Result<Value, String> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    let cmp = left.compare(right);
    let result = match op {
        "=" => cmp.map(|o| o == std::cmp::Ordering::Equal),
        "<>" | "!=" => cmp.map(|o| o != std::cmp::Ordering::Equal),
        "<" => cmp.map(|o| o == std::cmp::Ordering::Less),
        ">" => cmp.map(|o| o == std::cmp::Ordering::Greater),
        "<=" => cmp.map(|o| o != std::cmp::Ordering::Greater),
        ">=" => cmp.map(|o| o != std::cmp::Ordering::Less),
        _ => return Err(format!("unsupported operator in ALL: {}", op)),
    };
    Ok(result.map(Value::Bool).unwrap_or(Value::Null))
}

fn eval_a_expr(
    expr: &pg_query::protobuf::AExpr,
    row: &[Value],
    ctx: &JoinContext,
) -> Result<Value, String> {
    // Handle IN / NOT IN literal lists (AexprIn)
    if expr.kind == pg_query::protobuf::AExprKind::AexprIn as i32 {
        let in_op = extract_op_name(&expr.name).unwrap_or_default();
        let negated = in_op == "<>";

        let left_node = expr
            .lexpr
            .as_ref()
            .and_then(|n| n.node.as_ref())
            .ok_or("IN missing left operand")?;
        let left_val = eval_expr(left_node, row, ctx)?;

        if matches!(left_val, Value::Null) {
            return Ok(Value::Null);
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
                    let item_val = eval_expr(item_node, row, ctx)?;
                    if matches!(item_val, Value::Null) {
                        has_null = true;
                        continue;
                    }
                    let is_eq = left_val == item_val
                        || left_val.compare(&item_val) == Some(std::cmp::Ordering::Equal);
                    if is_eq {
                        return Ok(Value::Bool(!negated));
                    }
                }
            }
            return Ok(if has_null {
                Value::Null
            } else {
                Value::Bool(negated)
            });
        }

        return Err("IN requires a list on the right".into());
    }

    let op = extract_op_name(&expr.name)?;

    // Unary minus
    if expr.lexpr.is_none() && op == "-" {
        let right = expr
            .rexpr
            .as_ref()
            .and_then(|n| n.node.as_ref())
            .ok_or("unary - missing operand")?;
        return match eval_expr(right, row, ctx)? {
            Value::Int(n) => Ok(Value::Int(
                n.checked_neg().ok_or("integer out of range")?,
            )),
            Value::Float(f) => Ok(Value::Float(-f)),
            Value::Null => Ok(Value::Null),
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

    let left = eval_expr(left_node, row, ctx)?;
    let right = eval_expr(right_node, row, ctx)?;

    // String concatenation
    if op == "||" {
        return match (&left, &right) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            _ => {
                let l = left.to_text().unwrap_or_default();
                let r = right.to_text().unwrap_or_default();
                Ok(Value::Text(Arc::from(format!("{}{}", l, r).as_str())))
            }
        };
    }

    // Arithmetic
    if matches!(op.as_str(), "+" | "-" | "*" | "/") {
        return eval_arithmetic(&op, &left, &right);
    }

    // Vector distance operators (return Float, not Bool — no NULL propagation needed here)
    match op.as_str() {
        "<->" => {
            return match (&left, &right) {
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                (Value::Vector(a), Value::Vector(b)) if a.len() == b.len() => {
                    // Single-pass sum-of-squared-differences — no intermediate
                    // allocation, no branches in inner loop → SIMD-friendly.
                    let dist_sq: f32 = a.iter().zip(b.iter())
                        .map(|(x, y)| { let d = x - y; d * d })
                        .sum();
                    Ok(Value::Float(dist_sq.sqrt() as f64))
                }
                (Value::Vector(a), Value::Vector(b)) => Err(format!(
                    "different vector dimensions {} and {}",
                    a.len(),
                    b.len()
                )),
                _ => Err("operator <-> requires vector operands".into()),
            };
        }
        "<=>" => {
            return match (&left, &right) {
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                (Value::Vector(a), Value::Vector(b)) if a.len() == b.len() => {
                    // Single-pass cosine: dot product + both norms in one
                    // iteration — 3x fewer passes, no branches → SIMD-friendly.
                    let (dot, norm_a_sq, norm_b_sq) = a.iter().zip(b.iter())
                        .fold((0.0f32, 0.0f32, 0.0f32), |(d, na, nb), (x, y)| {
                            (d + x * y, na + x * x, nb + y * y)
                        });
                    let denom = norm_a_sq.sqrt() * norm_b_sq.sqrt();
                    if denom == 0.0 {
                        Ok(Value::Float(1.0))
                    } else {
                        Ok(Value::Float((1.0 - dot / denom) as f64))
                    }
                }
                (Value::Vector(a), Value::Vector(b)) => Err(format!(
                    "different vector dimensions {} and {}",
                    a.len(),
                    b.len()
                )),
                _ => Err("operator <=> requires vector operands".into()),
            };
        }
        "<#>" => {
            return match (&left, &right) {
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                (Value::Vector(a), Value::Vector(b)) if a.len() == b.len() => {
                    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
                    Ok(Value::Float((-dot) as f64))
                }
                (Value::Vector(a), Value::Vector(b)) => Err(format!(
                    "different vector dimensions {} and {}",
                    a.len(),
                    b.len()
                )),
                _ => Err("operator <#> requires vector operands".into()),
            };
        }
        _ => {}
    }

    // NULL propagation for comparisons
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }

    let cmp = left.compare(&right);
    let result = match op.as_str() {
        "=" => cmp.map(|o| o == std::cmp::Ordering::Equal),
        "<>" | "!=" => cmp.map(|o| o != std::cmp::Ordering::Equal),
        "<" => cmp.map(|o| o == std::cmp::Ordering::Less),
        ">" => cmp.map(|o| o == std::cmp::Ordering::Greater),
        "<=" => cmp.map(|o| o != std::cmp::Ordering::Greater),
        ">=" => cmp.map(|o| o != std::cmp::Ordering::Less),
        _ => return Err(format!("unsupported operator: {}", op)),
    };
    Ok(result.map(Value::Bool).unwrap_or(Value::Null))
}

fn eval_bool_expr(
    bexpr: &pg_query::protobuf::BoolExpr,
    row: &[Value],
    ctx: &JoinContext,
) -> Result<Value, String> {
    let boolop = bexpr.boolop;
    if boolop == pg_query::protobuf::BoolExprType::AndExpr as i32 {
        let mut has_null = false;
        for arg in &bexpr.args {
            let inner = arg.node.as_ref().ok_or("BoolExpr: missing arg")?;
            match eval_expr(inner, row, ctx)? {
                Value::Bool(false) => return Ok(Value::Bool(false)),
                Value::Null => has_null = true,
                Value::Bool(true) => {}
                other => return Err(format!("AND expects bool, got {:?}", other)),
            }
        }
        Ok(if has_null { Value::Null } else { Value::Bool(true) })
    } else if boolop == pg_query::protobuf::BoolExprType::OrExpr as i32 {
        let mut has_null = false;
        for arg in &bexpr.args {
            let inner = arg.node.as_ref().ok_or("BoolExpr: missing arg")?;
            match eval_expr(inner, row, ctx)? {
                Value::Bool(true) => return Ok(Value::Bool(true)),
                Value::Null => has_null = true,
                Value::Bool(false) => {}
                other => return Err(format!("OR expects bool, got {:?}", other)),
            }
        }
        Ok(if has_null { Value::Null } else { Value::Bool(false) })
    } else if boolop == pg_query::protobuf::BoolExprType::NotExpr as i32 {
        let inner = bexpr
            .args
            .first()
            .and_then(|a| a.node.as_ref())
            .ok_or("NOT missing arg")?;
        match eval_expr(inner, row, ctx)? {
            Value::Bool(b) => Ok(Value::Bool(!b)),
            Value::Null => Ok(Value::Null),
            other => Err(format!("NOT expects bool, got {:?}", other)),
        }
    } else {
        Err(format!("unsupported BoolExpr op: {}", boolop))
    }
}

fn eval_arithmetic(op: &str, left: &Value, right: &Value) -> Result<Value, String> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }
    match (left, right) {
        (Value::Int(a), Value::Int(b)) => match op {
            "+" => Ok(Value::Int(
                a.checked_add(*b).ok_or("integer out of range")?,
            )),
            "-" => Ok(Value::Int(
                a.checked_sub(*b).ok_or("integer out of range")?,
            )),
            "*" => Ok(Value::Int(
                a.checked_mul(*b).ok_or("integer out of range")?,
            )),
            "/" => {
                if *b == 0 {
                    Err("division by zero".into())
                } else {
                    Ok(Value::Int(
                        a.checked_div(*b).ok_or("integer out of range")?,
                    ))
                }
            }
            _ => Err(format!("unsupported arithmetic op: {}", op)),
        },
        (Value::Float(a), Value::Float(b)) => match op {
            "+" => Ok(Value::Float(a + b)),
            "-" => Ok(Value::Float(a - b)),
            "*" => Ok(Value::Float(a * b)),
            "/" => {
                if *b == 0.0 {
                    Err("division by zero".into())
                } else {
                    Ok(Value::Float(a / b))
                }
            }
            _ => Err(format!("unsupported arithmetic op: {}", op)),
        },
        (Value::Int(a), Value::Float(b)) => {
            eval_arithmetic(op, &Value::Float(*a as f64), &Value::Float(*b))
        }
        (Value::Float(a), Value::Int(b)) => {
            eval_arithmetic(op, &Value::Float(*a), &Value::Float(*b as f64))
        }
        _ => Err(format!("cannot apply {} to {:?} and {:?}", op, left, right)),
    }
}

fn eval_func_call(
    fc: &pg_query::protobuf::FuncCall,
    row: &[Value],
    ctx: &JoinContext,
) -> Result<Value, String> {
    let name = extract_func_name(fc);

    // Aggregate functions are handled in exec_select_aggregate, not here.
    // If we reach here, it's a scalar function.
    let args: Vec<Value> = fc
        .args
        .iter()
        .map(|a| {
            eval_expr(
                a.node.as_ref().ok_or("FuncCall: missing arg")?,
                row,
                ctx,
            )
        })
        .collect::<Result<_, _>>()?;

    eval_scalar_function(&name, &args)
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

fn eval_scalar_function(name: &str, args: &[Value]) -> Result<Value, String> {
    match name {
        "upper" => match args.first() {
            Some(Value::Text(s)) => Ok(Value::Text(Arc::from(s.to_uppercase().as_str()))),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err("upper() requires text argument".into()),
        },
        "lower" => match args.first() {
            Some(Value::Text(s)) => Ok(Value::Text(Arc::from(s.to_lowercase().as_str()))),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err("lower() requires text argument".into()),
        },
        "length" => match args.first() {
            Some(Value::Text(s)) => Ok(Value::Int(s.len() as i64)),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err("length() requires text argument".into()),
        },
        "concat" => {
            let parts: String = args
                .iter()
                .map(|v| match v {
                    Value::Null => String::new(),
                    v => v.to_text().unwrap_or_default(),
                })
                .collect();
            Ok(Value::Text(Arc::from(parts.as_str())))
        }
        "abs" => match args.first() {
            Some(Value::Int(n)) => Ok(Value::Int(
                n.checked_abs().ok_or("integer out of range")?,
            )),
            Some(Value::Float(f)) => Ok(Value::Float(f.abs())),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err("abs() requires numeric argument".into()),
        },
        "nextval" => match args.first() {
            Some(Value::Text(s)) => {
                let (schema, name) = parse_seq_name(s);
                let val = crate::sequence::nextval(schema, name)?;
                Ok(Value::Int(val))
            }
            Some(Value::Null) => Ok(Value::Null),
            _ => Err("nextval() requires text argument".into()),
        },
        "currval" => match args.first() {
            Some(Value::Text(s)) => {
                let (schema, name) = parse_seq_name(s);
                let val = crate::sequence::currval(schema, name)?;
                Ok(Value::Int(val))
            }
            Some(Value::Null) => Ok(Value::Null),
            _ => Err("currval() requires text argument".into()),
        },
        "setval" => match (args.first(), args.get(1)) {
            (Some(Value::Text(s)), Some(Value::Int(v))) => {
                let (schema, name) = parse_seq_name(s);
                let val = crate::sequence::setval(schema, name, *v)?;
                Ok(Value::Int(val))
            }
            _ => Err("setval() requires (text, integer) arguments".into()),
        },
        _ => Err(format!("function {}() does not exist", name)),
    }
}

fn eval_where(
    where_clause: &Option<Box<pg_query::protobuf::Node>>,
    row: &[Value],
    ctx: &JoinContext,
) -> Result<bool, String> {
    match where_clause {
        Some(wc) => match wc.node.as_ref() {
            Some(expr) => match eval_expr(expr, row, ctx)? {
                Value::Bool(b) => Ok(b),
                Value::Null => Ok(false), // NULL in WHERE filters out the row
                _ => Err("WHERE clause must return boolean".into()),
            },
            None => Ok(true),
        },
        None => Ok(true),
    }
}

// ── Fast equality filter (Principle 4: vectorized WHERE) ─────────────

/// Bypass eval_expr for the most common WHERE pattern: `column = constant`.
/// Direct column-index comparison with zero AST walking overhead.
struct FastEqualityFilter {
    col_idx: usize,
    value: Value,
}

impl FastEqualityFilter {
    #[inline(always)]
    fn matches(&self, row: &[Value]) -> bool {
        if self.col_idx >= row.len() { return false; }
        let v = &row[self.col_idx];
        // Use both == (same-type fast path) and compare() (cross-type: Int vs Float)
        *v == self.value || v.compare(&self.value) == Some(std::cmp::Ordering::Equal)
    }
}

/// Try to extract a fast equality filter from a WHERE clause.
/// Returns `None` for anything other than simple `ColumnRef = AConst` patterns,
/// falling through to the generic eval_where loop.
fn try_fast_equality_filter(
    where_clause: &Option<Box<pg_query::protobuf::Node>>,
    ctx: &JoinContext,
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
            let value = eval_const(Some(right));
            if matches!(value, Value::Null) { return None; } // NULL = x is never true
            return Some(FastEqualityFilter { col_idx, value });
        }
        // Pattern: Constant = ColumnRef
        if let NodeEnum::ColumnRef(cref) = right {
            let col_idx = resolve_column(cref, ctx).ok()?;
            let value = eval_const(Some(left));
            if matches!(value, Value::Null) { return None; }
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
    matches!(name, "count" | "sum" | "avg" | "min" | "max")
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

fn execute_from(node: &NodeEnum) -> Result<(Vec<Vec<Value>>, JoinContext), String> {
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
            // JOIN path: scan() (clone) is required here because rows must be owned
            // to combine with rows from other tables in the JOIN. scan_with() would
            // hold the read lock, preventing concurrent access to other tables and
            // making multi-table lock acquisition deadlock-prone.
            let rows = storage::scan(schema, &rv.relname)?;
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
            let (left_rows, left_ctx) = execute_from(left_node)?;
            let (right_rows, right_ctx) = execute_from(right_node)?;

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
                quals, &merged, left_width, right_width,
            ) {
                Some(Ok(rows)) => rows,
                Some(Err(e)) => return Err(e),
                None => nested_loop_join(
                    &left_rows, &right_rows, je.jointype,
                    quals, &merged, left_width, right_width,
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

            let (inner_cols, rows) = exec_select_raw(sel, None)?;
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
    left_rows: &[Vec<Value>],
    right_rows: &[Vec<Value>],
    join_type: i32,
    quals: Option<&NodeEnum>,
    ctx: &JoinContext,
    left_width: usize,
    right_width: usize,
) -> Option<Result<Vec<Vec<Value>>, String>> {
    // Extract equi-join key columns from the ON clause
    let quals = quals?;
    let (left_col, right_col) = extract_equi_cols(quals, ctx)?;

    Some(execute_hash_join(
        left_rows, right_rows, join_type, ctx,
        left_width, right_width, left_col, right_col,
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
        // AND: take the first equality condition
        NodeEnum::BoolExpr(be) if be.boolop == pg_query::protobuf::BoolExprType::AndExpr as i32 => {
            for arg in &be.args {
                if let Some(result) = arg.node.as_ref().and_then(|n| extract_equi_cols(n, ctx)) {
                    return Some(result);
                }
            }
            None
        }
        _ => None,
    }
}

/// Hash join: build hash table on right side, probe with left side. O(N+M).
fn execute_hash_join(
    left_rows: &[Vec<Value>],
    right_rows: &[Vec<Value>],
    join_type: i32,
    ctx: &JoinContext,
    left_width: usize,
    right_width: usize,
    left_key_col: usize,
    right_key_col: usize,
) -> Result<Vec<Vec<Value>>, String> {
    let null_right = vec![Value::Null; right_width];
    let null_left = vec![Value::Null; left_width];

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
        // Both on same side — can't hash join this, fall back
        return nested_loop_join(left_rows, right_rows, join_type, None, ctx, left_width, right_width);
    };
    let right_local_key = right_key_col - left_width;

    // BUILD phase: hash table on right side, keyed by join column value
    // Skip NULL keys — in SQL, NULL = NULL is FALSE in JOIN ON conditions.
    let mut hash_table: HashMap<Value, Vec<usize>> = HashMap::new();
    for (i, row) in right_rows.iter().enumerate() {
        if right_local_key < row.len() {
            if matches!(row[right_local_key], Value::Null) {
                continue; // NULL never matches in JOIN ON
            }
            let key = row[right_local_key].clone();
            hash_table.entry(key).or_default().push(i);
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

    // PROBE phase: for each left row, look up matching right rows in O(1)
    for left in left_rows {
        if left_key_col >= left.len() { continue; }
        let key = &left[left_key_col];

        // NULL key never matches — treat as unmatched for LEFT/FULL joins
        if matches!(key, Value::Null) {
            if is_left || is_full {
                let mut row = Vec::with_capacity(combined_width);
                row.extend_from_slice(left);
                row.extend_from_slice(&null_right);
                result.push(row);
            }
            continue;
        }

        let mut left_matched = false;

        if let Some(indices) = hash_table.get(key) {
            for &ri in indices {
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
    left_rows: &[Vec<Value>],
    right_rows: &[Vec<Value>],
    join_type: i32,
    quals: Option<&NodeEnum>,
    ctx: &JoinContext,
    left_width: usize,
    right_width: usize,
) -> Result<Vec<Vec<Value>>, String> {
    let null_right = vec![Value::Null; right_width];
    let null_left = vec![Value::Null; left_width];
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
                Some(q) => matches!(eval_expr(q, &combined, ctx)?, Value::Bool(true)),
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
) -> Result<(Vec<Vec<Value>>, JoinContext), String> {
    let first = from_clause
        .first()
        .and_then(|n| n.node.as_ref())
        .ok_or("missing FROM")?;
    let (mut rows, mut ctx) = execute_from(first)?;

    // Implicit cross join for FROM a, b, c ...
    for from_node in &from_clause[1..] {
        let node = from_node.node.as_ref().ok_or("missing FROM node")?;
        let (right_rows, right_ctx) = execute_from(node)?;
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
    outer: Option<(&[Value], &JoinContext)>,
) -> Result<(Vec<(String, i32)>, Vec<Vec<Value>>), String> {
    if select.from_clause.is_empty() {
        return exec_select_raw_no_from(select, outer);
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
                    let rows = storage::get_rows_by_ids(schema, &rv.relname, &row_ids)?;

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
                                        row[*idx].clone()
                                    } else {
                                        Value::Null
                                    }
                                }
                                SelectTarget::Expr { expr, .. } => {
                                    eval_expr(expr, row, &ctx)?
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

            let fast_filter = try_fast_equality_filter(&select.where_clause, &ctx);
            let rows = storage::scan_with(schema, &rv.relname, |all_rows| {
                let mut filtered = Vec::new();
                if let Some(ref ff) = fast_filter {
                    // Fast path: direct column comparison — no eval_expr overhead.
                    // Principle 4: vectorized WHERE for simple equality.
                    for row in all_rows {
                        if ff.matches(row) {
                            filtered.push(row.clone());
                        }
                    }
                } else {
                    // General path: full eval_where with AST walking.
                    for row in all_rows {
                        if eval_where(&select.where_clause, row, &ctx)? {
                            filtered.push(row.clone());
                        }
                    }
                }
                Ok(filtered)
            })?;

            let result = exec_select_raw_post_filter(select, ctx, rows, 0);
            return result;
        }
    }

    // General path: JOINs, implicit joins, subqueries, correlated
    let (all_rows, inner_ctx) = execute_from_clause(&select.from_clause)?;

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
        let rows: Vec<Vec<Value>> = all_rows
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
        if eval_where(&select.where_clause, &row, &merged_ctx)? {
            rows.push(row);
        }
    }

    return exec_select_raw_post_filter(select, merged_ctx, rows, outer_width);
}

/// Shared post-filter logic: aggregates, ORDER BY, LIMIT, projection.
fn exec_select_raw_post_filter(
    select: &pg_query::protobuf::SelectStmt,
    merged_ctx: JoinContext,
    mut rows: Vec<Vec<Value>>,
    _outer_width: usize,
) -> Result<(Vec<(String, i32)>, Vec<Vec<Value>>), String> {
    // Route to aggregate path if needed
    if query_has_aggregates(select) || !select.group_clause.is_empty() {
        let agg_result = exec_select_aggregate(select, &merged_ctx, rows)?;
        // Convert string rows back to typed Value rows using column OIDs
        let col_oids: Vec<i32> = agg_result.columns.iter().map(|(_, oid)| *oid).collect();
        let mut value_rows: Vec<Vec<Value>> = agg_result
            .rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .enumerate()
                    .map(|(i, cell)| match cell {
                        None => Value::Null,
                        Some(s) => {
                            let oid = col_oids.get(i).copied().unwrap_or(25);
                            parse_text_to_value(&s, oid)
                        }
                    })
                    .collect()
            })
            .collect();

        // Apply ORDER BY to aggregate results (Bug #2 / #7 fix)
        if !select.sort_clause.is_empty() {
            let sort_keys = resolve_aggregate_sort_keys(
                &select.sort_clause, &agg_result.columns, &merged_ctx, select,
            )?;
            value_rows.sort_by(|a, b| compare_rows(&sort_keys, a, b));
        }

        // Apply OFFSET
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

        // Apply LIMIT
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
            resolve_sort_keys_with_exprs(&select.sort_clause, &merged_ctx, select, &mut rows)?;
        rows.sort_by(|a, b| compare_rows(&sort_keys, a, b));
        // Strip temporary expression columns appended for sorting
        if expr_count > 0 {
            for row in rows.iter_mut() {
                row.truncate(row.len() - expr_count);
            }
        }
    }

    // OFFSET
    if let Some(ref offset_node) = select.limit_offset {
        if let Some(n) = eval_const_i64(offset_node.node.as_ref()) {
            let n = n.max(0) as usize;
            if n >= rows.len() {
                rows.clear();
            } else {
                rows.drain(0..n);
            }
        }
    }

    // LIMIT
    if let Some(ref limit_node) = select.limit_count {
        if let Some(n) = eval_const_i64(limit_node.node.as_ref()) {
            rows.truncate(n.max(0) as usize);
        }
    }

    // Resolve targets and project
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
                        row[*idx].clone()
                    } else {
                        return Err(format!(
                            "internal error: column index {} out of range for row of width {}",
                            idx, row.len()
                        ));
                    }
                }
                SelectTarget::Expr { expr, .. } => {
                    eval_expr(expr, row, &merged_ctx)?
                }
            };
            result_row.push(val);
        }
        result_rows.push(result_row);
    }

    

    Ok((columns, result_rows))
}

/// Handle SELECT with no FROM clause, returning raw Values.
fn exec_select_raw_no_from(
    select: &pg_query::protobuf::SelectStmt,
    outer: Option<(&[Value], &JoinContext)>,
) -> Result<(Vec<(String, i32)>, Vec<Vec<Value>>), String> {
    let (eval_row, eval_ctx): (Vec<Value>, JoinContext) = if let Some((outer_row, outer_ctx)) = outer
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
                Some(expr) => eval_expr(expr, &eval_row, &eval_ctx)?,
                None => Value::Null,
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
    let (columns, raw_rows) = exec_select_raw(select, None)?;
    let rows: Vec<Vec<Option<String>>> = raw_rows
        .iter()
        .map(|row| row.iter().map(|v| v.to_text()).collect())
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
    rows: &mut Vec<Vec<Value>>,
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
            row.resize(next_col, Value::Null);
        }
        // Need to clone expr_nodes data to avoid borrow issues
        let expr_data: Vec<(usize, NodeEnum)> = expr_nodes
            .iter()
            .map(|(ki, node)| (*ki, (*node).clone()))
            .collect();
        for row in rows.iter_mut() {
            for (key_idx, expr_node) in &expr_data {
                let col_idx = keys[*key_idx].col_idx;
                let val = eval_expr(expr_node, row, ctx)?;
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

fn compare_rows(keys: &[SortKey], a: &[Value], b: &[Value]) -> std::cmp::Ordering {
    for k in keys {
        let va = a.get(k.col_idx).unwrap_or(&Value::Null);
        let vb = b.get(k.col_idx).unwrap_or(&Value::Null);
        let ord = match (va, vb) {
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            (Value::Null, _) => {
                if k.nulls_first {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                }
            }
            (_, Value::Null) => {
                if k.nulls_first {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            }
            _ => va.compare(vb).unwrap_or(std::cmp::Ordering::Equal),
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
    rows: Vec<Vec<Value>>,
) -> Result<QueryResult, String> {
    let group_col_indices = resolve_group_columns(&select.group_clause, ctx)?;

    // Group rows (use Vec to maintain insertion order, HashMap for O(1) lookup)
    let mut groups: Vec<(Vec<Value>, Vec<Vec<Value>>)> = Vec::new();
    let mut group_index: HashMap<Vec<Value>, usize> = HashMap::new();

    if group_col_indices.is_empty() {
        groups.push((vec![], rows));
    } else {
        for row in rows {
            let key: Vec<Value> = group_col_indices.iter().map(|&i| row[i].clone()).collect();

            if let Some(&idx) = group_index.get(&key) {
                groups[idx].1.push(row);
            } else {
                let idx = groups.len();
                group_index.insert(key.clone(), idx);
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
                                compute_aggregate(&name, fc, group_rows, ctx)?;
                            if !columns_built {
                                let alias = if rt.name.is_empty() {
                                    name.clone()
                                } else {
                                    rt.name.clone()
                                };
                                let oid = match name.as_str() {
                                    "count" => TypeOid::Int8.oid(),
                                    "avg" => TypeOid::Float8.oid(),
                                    _ => TypeOid::Text.oid(),
                                };
                                result_columns.push((alias, oid));
                            }
                            result_row.push(agg_val.to_text());
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
                        result_row.push(group_key[key_pos].to_text());
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

    // HAVING filter
    if let Some(having_node) = &select.having_clause {
        if let Some(having_expr) = having_node.node.as_ref() {
            let mut filtered = Vec::new();
            for (i, (_, group_rows)) in groups.iter().enumerate() {
                if eval_having(having_expr, group_rows, ctx)? {
                    filtered.push(result_rows[i].clone());
                }
            }
            result_rows = filtered;
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
    rows: &[Vec<Value>],
    ctx: &JoinContext,
) -> Result<Value, String> {
    match name {
        "count" => {
            if fc.agg_star {
                Ok(Value::Int(rows.len() as i64))
            } else {
                let arg = fc
                    .args
                    .first()
                    .and_then(|a| a.node.as_ref())
                    .ok_or("COUNT requires argument")?;
                let mut count: i64 = 0;
                for row in rows {
                    match eval_expr(arg, row, ctx)? {
                        Value::Null => {} // COUNT(col) skips NULLs
                        _ => count += 1,
                    }
                }
                Ok(Value::Int(count))
            }
        }
        "sum" => {
            let arg = fc
                .args
                .first()
                .and_then(|a| a.node.as_ref())
                .ok_or("SUM requires argument")?;
            let mut si: i64 = 0;
            let mut sf: f64 = 0.0;
            let mut is_float = false;
            let mut has_val = false;
            for row in rows {
                match eval_expr(arg, row, ctx)? {
                    Value::Int(n) => {
                        si = si.checked_add(n).ok_or("integer out of range")?;
                        has_val = true;
                    }
                    Value::Float(f) => {
                        sf += f;
                        is_float = true;
                        has_val = true;
                    }
                    Value::Null => {}
                    _ => return Err("SUM requires numeric".into()),
                }
            }
            if !has_val {
                return Ok(Value::Null);
            }
            Ok(if is_float {
                Value::Float(sf + si as f64)
            } else {
                Value::Int(si)
            })
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
                match eval_expr(arg, row, ctx)? {
                    Value::Int(n) => {
                        sum += n as f64;
                        count += 1;
                    }
                    Value::Float(f) => {
                        sum += f;
                        count += 1;
                    }
                    Value::Null => {}
                    _ => return Err("AVG requires numeric".into()),
                }
            }
            Ok(if count == 0 {
                Value::Null
            } else {
                Value::Float(sum / count as f64)
            })
        }
        "min" => {
            let arg = fc
                .args
                .first()
                .and_then(|a| a.node.as_ref())
                .ok_or("MIN requires argument")?;
            let mut result: Option<Value> = None;
            for row in rows {
                let v = eval_expr(arg, row, ctx)?;
                if matches!(v, Value::Null) {
                    continue;
                }
                result = Some(match result {
                    None => v,
                    Some(cur) => {
                        if v.compare(&cur) == Some(std::cmp::Ordering::Less) {
                            v
                        } else {
                            cur
                        }
                    }
                });
            }
            Ok(result.unwrap_or(Value::Null))
        }
        "max" => {
            let arg = fc
                .args
                .first()
                .and_then(|a| a.node.as_ref())
                .ok_or("MAX requires argument")?;
            let mut result: Option<Value> = None;
            for row in rows {
                let v = eval_expr(arg, row, ctx)?;
                if matches!(v, Value::Null) {
                    continue;
                }
                result = Some(match result {
                    None => v,
                    Some(cur) => {
                        if v.compare(&cur) == Some(std::cmp::Ordering::Greater) {
                            v
                        } else {
                            cur
                        }
                    }
                });
            }
            Ok(result.unwrap_or(Value::Null))
        }
        _ => Err(format!("unknown aggregate function: {}", name)),
    }
}

fn eval_having(
    expr: &NodeEnum,
    group_rows: &[Vec<Value>],
    ctx: &JoinContext,
) -> Result<bool, String> {
    let val = eval_having_expr(expr, group_rows, ctx)?;
    Ok(matches!(val, Value::Bool(true)))
}

fn eval_having_expr(
    node: &NodeEnum,
    group_rows: &[Vec<Value>],
    ctx: &JoinContext,
) -> Result<Value, String> {
    match node {
        NodeEnum::FuncCall(fc) => {
            let name = extract_func_name(fc);
            if is_aggregate(&name) {
                compute_aggregate(&name, fc, group_rows, ctx)
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
                .map(|n| eval_having_expr(n, group_rows, ctx))
                .transpose()?;
            let right = expr
                .rexpr
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .map(|n| eval_having_expr(n, group_rows, ctx))
                .transpose()?;

            let (l, r) = match (left, right) {
                (Some(l), Some(r)) => (l, r),
                _ => return Ok(Value::Null),
            };

            if matches!(l, Value::Null) || matches!(r, Value::Null) {
                return Ok(Value::Null);
            }

            let cmp = l.compare(&r);
            let result = match op.as_str() {
                "=" => cmp.map(|o| o == std::cmp::Ordering::Equal),
                "<>" | "!=" => cmp.map(|o| o != std::cmp::Ordering::Equal),
                "<" => cmp.map(|o| o == std::cmp::Ordering::Less),
                ">" => cmp.map(|o| o == std::cmp::Ordering::Greater),
                "<=" => cmp.map(|o| o != std::cmp::Ordering::Greater),
                ">=" => cmp.map(|o| o != std::cmp::Ordering::Less),
                _ => return Err(format!("unsupported operator in HAVING: {}", op)),
            };
            Ok(result.map(Value::Bool).unwrap_or(Value::Null))
        }
        NodeEnum::AConst(_) | NodeEnum::Integer(_) | NodeEnum::Float(_) => {
            let dummy_ctx = JoinContext {
                sources: vec![],
                total_columns: 0,
            };
            eval_expr(node, &[], &dummy_ctx)
        }
        NodeEnum::BoolExpr(be) => {
            let boolop = be.boolop;
            if boolop == pg_query::protobuf::BoolExprType::AndExpr as i32 {
                for arg in &be.args {
                    if let Some(n) = arg.node.as_ref() {
                        match eval_having_expr(n, group_rows, ctx)? {
                            Value::Bool(false) => return Ok(Value::Bool(false)),
                            Value::Bool(true) => continue,
                            _ => return Ok(Value::Null),
                        }
                    }
                }
                Ok(Value::Bool(true))
            } else if boolop == pg_query::protobuf::BoolExprType::OrExpr as i32 {
                for arg in &be.args {
                    if let Some(n) = arg.node.as_ref() {
                        match eval_having_expr(n, group_rows, ctx)? {
                            Value::Bool(true) => return Ok(Value::Bool(true)),
                            _ => continue,
                        }
                    }
                }
                Ok(Value::Bool(false))
            } else {
                Err("unsupported HAVING boolean expression".into())
            }
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
        // Validate WHERE clause inside read lock via scan_with — avoids cloning all rows.
        // This catches expression errors (bad column refs, type mismatches) before mutation.
        storage::scan_with(schema, table_name, |all_rows| {
            for row in all_rows {
                eval_where(&delete.where_clause, row, &ctx)?;
            }
            Ok(())
        })?;
        if has_returning {
            let wc = delete.where_clause.clone();
            let rows = storage::delete_where_returning(schema, table_name, |row| {
                eval_where(&wc, row, &ctx).unwrap_or(false)
            })?;
            let n = rows.len() as u64;
            (n, rows)
        } else {
            let wc = delete.where_clause.clone();
            let n = storage::delete_where(schema, table_name, |row| {
                eval_where(&wc, row, &ctx).unwrap_or(false)
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

    // Validate WHERE clause inside read lock via scan_with — avoids cloning all rows.
    // This catches expression errors (bad column refs, type mismatches) before mutation.
    storage::scan_with(schema, table_name, |all_rows| {
        for row in all_rows {
            eval_where(&update.where_clause, row, &ctx)?;
        }
        Ok(())
    })?;

    let wc = update.where_clause.clone();
    let td = table_def.clone();
    let assigns = assignments.clone();
    let ctx2 = JoinContext::single(schema, table_name, td.clone());

    let has_returning = !update.returning_list.is_empty();
    let mut updated_rows: Vec<Vec<Value>> = Vec::new();

    let count = storage::update_rows_checked(
        schema,
        table_name,
        |row| eval_where(&wc, row, &ctx2).unwrap_or(false),
        |row| {
            let ctx_inner = JoinContext::single(schema, table_name, td.clone());
            let mut new_row = row.clone();
            for (col_idx, expr_node) in &assigns {
                new_row[*col_idx] = eval_expr(expr_node, row, &ctx_inner)?;
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
}
