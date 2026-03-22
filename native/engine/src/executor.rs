use std::collections::HashMap;

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
    let fields = extract_string_fields(cref);

    match fields.len() {
        1 => {
            // Unqualified: search all sources, error if ambiguous
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

    let mut row_count = 0u64;

    if let NodeEnum::SelectStmt(sel) = select {
        for values_list in &sel.values_lists {
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
                        .ok_or_else(|| format!("column \"{}\" does not exist", col_name))?;

                    row[col_idx] = eval_const(val_node.node.as_ref());
                }

                check_not_null(&table_def, &row)?;
                let (unique_checks, pk_cols) = build_unique_checks(&table_def);
                storage::insert_checked(
                    schema, table_name, row, &unique_checks, &pk_cols,
                )?;
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
                    pg_query::protobuf::a_const::Val::Sval(s) => Value::Text(s.sval.clone()),
                    pg_query::protobuf::a_const::Val::Bsval(s) => Value::Text(s.bsval.clone()),
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
                    pg_query::protobuf::a_const::Val::Sval(s) => Ok(Value::Text(s.sval.clone())),
                    pg_query::protobuf::a_const::Val::Bsval(s) => Ok(Value::Text(s.bsval.clone())),
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
        NodeEnum::String(s) => Ok(Value::Text(s.sval.clone())),
        NodeEnum::TypeCast(tc) => {
            let inner = tc
                .arg
                .as_ref()
                .and_then(|a| a.node.as_ref())
                .ok_or("TypeCast missing arg")?;
            eval_expr(inner, row, ctx)
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
        _ => Err(format!(
            "unsupported expression node: {:?}",
            std::mem::discriminant(node)
        )),
    }
}

fn eval_a_expr(
    expr: &pg_query::protobuf::AExpr,
    row: &[Value],
    ctx: &JoinContext,
) -> Result<Value, String> {
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
                Ok(Value::Text(format!("{}{}", l, r)))
            }
        };
    }

    // Arithmetic
    if matches!(op.as_str(), "+" | "-" | "*" | "/") {
        return eval_arithmetic(&op, &left, &right);
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

fn eval_scalar_function(name: &str, args: &[Value]) -> Result<Value, String> {
    match name {
        "upper" => match args.first() {
            Some(Value::Text(s)) => Ok(Value::Text(s.to_uppercase())),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err("upper() requires text argument".into()),
        },
        "lower" => match args.first() {
            Some(Value::Text(s)) => Ok(Value::Text(s.to_lowercase())),
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
            Ok(Value::Text(parts))
        }
        "abs" => match args.first() {
            Some(Value::Int(n)) => Ok(Value::Int(
                n.checked_abs().ok_or("integer out of range")?,
            )),
            Some(Value::Float(f)) => Ok(Value::Float(f.abs())),
            Some(Value::Null) => Ok(Value::Null),
            _ => Err("abs() requires numeric argument".into()),
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

            let result = nested_loop_join(
                &left_rows,
                &right_rows,
                je.jointype,
                quals,
                &merged,
                left_width,
                right_width,
            )?;

            Ok((result, merged))
        }
        _ => Err("unsupported FROM clause node".into()),
    }
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

fn exec_select(
    select: &pg_query::protobuf::SelectStmt,
) -> Result<QueryResult, String> {
    if select.from_clause.is_empty() {
        return exec_select_no_from(select);
    }

    // Execute FROM clause (handles single table, JOINs, and implicit joins)
    let (all_rows, ctx) = execute_from_clause(&select.from_clause)?;

    // WHERE filter
    let mut rows: Vec<Vec<Value>> = Vec::new();
    for row in all_rows {
        if eval_where(&select.where_clause, &row, &ctx)? {
            rows.push(row);
        }
    }

    // Route to aggregate path if needed
    if query_has_aggregates(select) || !select.group_clause.is_empty() {
        return exec_select_aggregate(select, &ctx, rows);
    }

    // ORDER BY (on full rows before projection)
    if !select.sort_clause.is_empty() {
        let sort_keys = resolve_sort_keys(&select.sort_clause, &ctx, select)?;
        rows.sort_by(|a, b| compare_rows(&sort_keys, a, b));
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
    let targets = resolve_targets(select, &ctx)?;
    let columns: Vec<(String, i32)> = targets
        .iter()
        .map(|t| match t {
            SelectTarget::Column { name, idx } => {
                Ok((name.clone(), column_type_oid(*idx, &ctx)?))
            }
            SelectTarget::Expr { name, .. } => Ok((name.clone(), TypeOid::Text.oid())),
        })
        .collect::<Result<Vec<_>, String>>()?;

    let mut result_rows: Vec<Vec<Option<String>>> = Vec::new();
    for row in &rows {
        let mut result_row = Vec::new();
        for t in &targets {
            let cell = match t {
                SelectTarget::Column { idx, .. } => {
                    row.get(*idx).and_then(|v| v.to_text())
                }
                SelectTarget::Expr { expr, .. } => {
                    eval_expr(expr, row, &ctx)?.to_text()
                }
            };
            result_row.push(cell);
        }
        result_rows.push(result_row);
    }

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
    let dummy_ctx = JoinContext {
        sources: vec![],
        total_columns: 0,
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
                Some(expr) => eval_expr(expr, &[], &dummy_ctx)?,
                None => Value::Null,
            };
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

// ── ORDER BY helpers ──────────────────────────────────────────────────

struct SortKey {
    col_idx: usize,
    ascending: bool,
    nulls_first: bool,
}

fn resolve_sort_keys(
    sort_clause: &[pg_query::protobuf::Node],
    ctx: &JoinContext,
    select: &pg_query::protobuf::SelectStmt,
) -> Result<Vec<SortKey>, String> {
    let mut keys = Vec::new();
    for snode in sort_clause {
        if let Some(NodeEnum::SortBy(sb)) = snode.node.as_ref() {
            let col_idx = match sb.node.as_ref().and_then(|n| n.node.as_ref()) {
                Some(NodeEnum::ColumnRef(cref)) => resolve_column(cref, ctx)?,
                Some(NodeEnum::AConst(ac)) => {
                    // Ordinal: ORDER BY 1 means first column in target list
                    let ordinal = match &ac.val {
                        Some(pg_query::protobuf::a_const::Val::Ival(i)) => i.ival as usize,
                        _ => return Err("invalid ORDER BY ordinal".into()),
                    };
                    resolve_ordinal_to_col_idx(ordinal, select, ctx)?
                }
                _ => return Err("unsupported ORDER BY expression".into()),
            };

            let ascending = sb.sortby_dir
                != pg_query::protobuf::SortByDir::SortbyDesc as i32;
            let nulls_first = match sb.sortby_nulls {
                x if x == pg_query::protobuf::SortByNulls::SortbyNullsFirst as i32 => true,
                x if x == pg_query::protobuf::SortByNulls::SortbyNullsLast as i32 => false,
                _ => !ascending, // default: NULLS LAST for ASC, FIRST for DESC
            };

            keys.push(SortKey {
                col_idx,
                ascending,
                nulls_first,
            });
        }
    }
    Ok(keys)
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

    let count = if delete.where_clause.is_some() {
        let table_def = catalog::get_table(schema, table_name)
            .ok_or_else(|| format!("relation \"{}\" does not exist", table_name))?;
        let ctx = JoinContext::single(schema, table_name, table_def.clone());
        // Validate WHERE clause first
        let all_rows = storage::scan(schema, table_name)?;
        for row in &all_rows {
            eval_where(&delete.where_clause, row, &ctx)?;
        }
        let wc = delete.where_clause.clone();
        storage::delete_where(schema, table_name, |row| {
            eval_where(&wc, row, &ctx).unwrap_or(false)
        })?
    } else {
        storage::delete_all(schema, table_name)?
    };

    Ok(QueryResult {
        tag: format!("DELETE {}", count),
        columns: vec![],
        rows: vec![],
    })
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

    // Validate WHERE clause against all rows first
    let all_rows = storage::scan(schema, table_name)?;
    for row in &all_rows {
        eval_where(&update.where_clause, row, &ctx)?;
    }

    let wc = update.where_clause.clone();
    let td = table_def.clone();
    let assigns = assignments.clone();
    let ctx2 = JoinContext::single(schema, table_name, td.clone());

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
            Ok(new_row)
        },
        |new_row, all_rows, skip_idx| {
            check_unique_against(&td, new_row, all_rows, skip_idx)
        },
    )?;

    Ok(QueryResult {
        tag: format!("UPDATE {}", count),
        columns: vec![],
        rows: vec![],
    })
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
}
