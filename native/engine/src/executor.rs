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

fn eval_expr(node: &NodeEnum, row: &[Value], table: &Table) -> Result<Value, String> {
    match node {
        NodeEnum::ColumnRef(cref) => {
            let col_name = cref
                .fields
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
                .ok_or("empty ColumnRef")?;
            let idx = table
                .columns
                .iter()
                .position(|c| c.name == col_name)
                .ok_or_else(|| format!("column \"{}\" does not exist", col_name))?;
            Ok(row.get(idx).cloned().unwrap_or(Value::Null))
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
            eval_expr(inner, row, table)
        }
        NodeEnum::AExpr(expr) => eval_a_expr(expr, row, table),
        NodeEnum::BoolExpr(bexpr) => eval_bool_expr(bexpr, row, table),
        NodeEnum::NullTest(nt) => {
            let inner = nt
                .arg
                .as_ref()
                .and_then(|a| a.node.as_ref())
                .ok_or("NullTest missing arg")?;
            let val = eval_expr(inner, row, table)?;
            let is_null = matches!(val, Value::Null);
            if nt.nulltesttype == pg_query::protobuf::NullTestType::IsNull as i32 {
                Ok(Value::Bool(is_null))
            } else {
                Ok(Value::Bool(!is_null))
            }
        }
        NodeEnum::FuncCall(fc) => eval_func_call(fc, row, table),
        _ => Err(format!(
            "unsupported expression node: {:?}",
            std::mem::discriminant(node)
        )),
    }
}

fn eval_a_expr(
    expr: &pg_query::protobuf::AExpr,
    row: &[Value],
    table: &Table,
) -> Result<Value, String> {
    let op = extract_op_name(&expr.name)?;

    // Unary minus
    if expr.lexpr.is_none() && op == "-" {
        let right = expr
            .rexpr
            .as_ref()
            .and_then(|n| n.node.as_ref())
            .ok_or("unary - missing operand")?;
        return match eval_expr(right, row, table)? {
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

    let left = eval_expr(left_node, row, table)?;
    let right = eval_expr(right_node, row, table)?;

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
    table: &Table,
) -> Result<Value, String> {
    let boolop = bexpr.boolop;
    if boolop == pg_query::protobuf::BoolExprType::AndExpr as i32 {
        let mut has_null = false;
        for arg in &bexpr.args {
            let inner = arg.node.as_ref().ok_or("BoolExpr: missing arg")?;
            match eval_expr(inner, row, table)? {
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
            match eval_expr(inner, row, table)? {
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
        match eval_expr(inner, row, table)? {
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
    table: &Table,
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
                table,
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
    table: &Table,
) -> Result<bool, String> {
    match where_clause {
        Some(wc) => match wc.node.as_ref() {
            Some(expr) => match eval_expr(expr, row, table)? {
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

// ── SELECT target resolution ──────────────────────────────────────────

enum SelectTarget {
    Column { name: String, idx: usize },
    Expr { name: String, expr: NodeEnum },
}

fn resolve_targets(
    select: &pg_query::protobuf::SelectStmt,
    table: &Table,
) -> Result<Vec<SelectTarget>, String> {
    let mut targets = Vec::new();

    for target in &select.target_list {
        if let Some(NodeEnum::ResTarget(rt)) = target.node.as_ref() {
            let val_node = rt.val.as_ref().and_then(|v| v.node.as_ref());
            match val_node {
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
                            targets.push(SelectTarget::Column {
                                name: col.name.clone(),
                                idx: i,
                            });
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

    let from = select.from_clause.first().ok_or("missing FROM")?;
    let (schema, table_name) = extract_from_table(from.node.as_ref())?;
    let table_def = catalog::get_table(&schema, &table_name)
        .ok_or_else(|| format!("relation \"{}\" does not exist", table_name))?;

    let all_rows = storage::scan(&schema, &table_name)?;

    // WHERE filter (propagates errors instead of silently excluding rows)
    let mut rows: Vec<Vec<Value>> = Vec::new();
    for row in all_rows {
        if eval_where(&select.where_clause, &row, &table_def)? {
            rows.push(row);
        }
    }

    // Route to aggregate path if needed
    if query_has_aggregates(select) || !select.group_clause.is_empty() {
        return exec_select_aggregate(select, &table_def, rows);
    }

    // ORDER BY (on full rows before projection)
    if !select.sort_clause.is_empty() {
        let sort_keys = resolve_sort_keys(&select.sort_clause, &table_def, select)?;
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
    let targets = resolve_targets(select, &table_def)?;
    let columns: Vec<(String, i32)> = targets
        .iter()
        .map(|t| match t {
            SelectTarget::Column { name, idx } => {
                (name.clone(), table_def.columns[*idx].type_oid.oid())
            }
            SelectTarget::Expr { name, .. } => (name.clone(), TypeOid::Text.oid()),
        })
        .collect();

    let result_rows: Vec<Vec<Option<String>>> = rows
        .iter()
        .map(|row| {
            targets
                .iter()
                .map(|t| match t {
                    SelectTarget::Column { idx, .. } => {
                        row.get(*idx).and_then(|v| v.to_text())
                    }
                    SelectTarget::Expr { expr, .. } => {
                        eval_expr(expr, row, &table_def)
                            .ok()
                            .and_then(|v| v.to_text())
                    }
                })
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
    let dummy_table = Table {
        name: String::new(),
        schema: String::new(),
        columns: vec![],
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
                Some(expr) => eval_expr(expr, &[], &dummy_table)
                    .unwrap_or(Value::Null),
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
    table: &Table,
    select: &pg_query::protobuf::SelectStmt,
) -> Result<Vec<SortKey>, String> {
    let mut keys = Vec::new();
    for snode in sort_clause {
        if let Some(NodeEnum::SortBy(sb)) = snode.node.as_ref() {
            let col_idx = match sb.node.as_ref().and_then(|n| n.node.as_ref()) {
                Some(NodeEnum::ColumnRef(cref)) => {
                    let name = extract_col_name(cref);
                    table
                        .columns
                        .iter()
                        .position(|c| c.name == name)
                        .ok_or_else(|| {
                            format!("column \"{}\" does not exist", name)
                        })?
                }
                Some(NodeEnum::AConst(ac)) => {
                    // Ordinal: ORDER BY 1 means first column in target list
                    let ordinal = match &ac.val {
                        Some(pg_query::protobuf::a_const::Val::Ival(i)) => i.ival as usize,
                        _ => return Err("invalid ORDER BY ordinal".into()),
                    };
                    // Map ordinal to actual table column index via target list
                    resolve_ordinal_to_col_idx(ordinal, select, table)?
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
    table: &Table,
) -> Result<usize, String> {
    if ordinal == 0 || ordinal > select.target_list.len() {
        return Err(format!("ORDER BY position {} is not in select list", ordinal));
    }
    let target = &select.target_list[ordinal - 1];
    if let Some(NodeEnum::ResTarget(rt)) = target.node.as_ref() {
        if let Some(NodeEnum::ColumnRef(cref)) = rt.val.as_ref().and_then(|v| v.node.as_ref()) {
            let name = extract_col_name(cref);
            return table
                .columns
                .iter()
                .position(|c| c.name == name)
                .ok_or_else(|| format!("column \"{}\" does not exist", name));
        }
    }
    Err("ORDER BY ordinal must reference a column".into())
}

fn compare_rows(keys: &[SortKey], a: &[Value], b: &[Value]) -> std::cmp::Ordering {
    for k in keys {
        let va = &a[k.col_idx];
        let vb = &b[k.col_idx];
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
    table: &Table,
    rows: Vec<Vec<Value>>,
) -> Result<QueryResult, String> {
    let group_col_indices = resolve_group_columns(&select.group_clause, table)?;

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
                            let agg_val = compute_aggregate(&name, fc, group_rows, table)?;
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
                        let col_name = extract_col_name(cref);
                        let col_idx = table
                            .columns
                            .iter()
                            .position(|c| c.name == col_name)
                            .ok_or_else(|| {
                                format!("column \"{}\" does not exist", col_name)
                            })?;

                        if !group_col_indices.contains(&col_idx) {
                            return Err(format!(
                                "column \"{}\" must appear in the GROUP BY clause or be used in an aggregate function",
                                col_name
                            ));
                        }

                        if !columns_built {
                            let alias = if rt.name.is_empty() {
                                col_name
                            } else {
                                rt.name.clone()
                            };
                            result_columns.push((alias, table.columns[col_idx].type_oid.oid()));
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
                if eval_having(having_expr, group_rows, table)? {
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
    table: &Table,
) -> Result<Vec<usize>, String> {
    let mut indices = Vec::new();
    for node in group_clause {
        if let Some(NodeEnum::ColumnRef(cref)) = node.node.as_ref() {
            let col_name = extract_col_name(cref);
            let idx = table
                .columns
                .iter()
                .position(|c| c.name == col_name)
                .ok_or_else(|| format!("column \"{}\" does not exist", col_name))?;
            indices.push(idx);
        }
    }
    Ok(indices)
}

fn compute_aggregate(
    name: &str,
    fc: &pg_query::protobuf::FuncCall,
    rows: &[Vec<Value>],
    table: &Table,
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
                let count = rows
                    .iter()
                    .filter(|row| {
                        !matches!(eval_expr(arg, row, table), Ok(Value::Null) | Err(_))
                    })
                    .count();
                Ok(Value::Int(count as i64))
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
                match eval_expr(arg, row, table)? {
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
                match eval_expr(arg, row, table)? {
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
                let v = eval_expr(arg, row, table)?;
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
                let v = eval_expr(arg, row, table)?;
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
    table: &Table,
) -> Result<bool, String> {
    let val = eval_having_expr(expr, group_rows, table)?;
    Ok(matches!(val, Value::Bool(true)))
}

fn eval_having_expr(
    node: &NodeEnum,
    group_rows: &[Vec<Value>],
    table: &Table,
) -> Result<Value, String> {
    match node {
        NodeEnum::FuncCall(fc) => {
            let name = extract_func_name(fc);
            if is_aggregate(&name) {
                compute_aggregate(&name, fc, group_rows, table)
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
                .map(|n| eval_having_expr(n, group_rows, table))
                .transpose()?;
            let right = expr
                .rexpr
                .as_ref()
                .and_then(|n| n.node.as_ref())
                .map(|n| eval_having_expr(n, group_rows, table))
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
            let dummy = Table {
                name: String::new(),
                schema: String::new(),
                columns: vec![],
            };
            eval_expr(node, &[], &dummy)
        }
        NodeEnum::BoolExpr(be) => {
            let boolop = be.boolop;
            if boolop == pg_query::protobuf::BoolExprType::AndExpr as i32 {
                for arg in &be.args {
                    if let Some(n) = arg.node.as_ref() {
                        match eval_having_expr(n, group_rows, table)? {
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
                        match eval_having_expr(n, group_rows, table)? {
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
        // Validate WHERE clause first (propagates errors instead of silently excluding rows)
        let all_rows = storage::scan(schema, table_name)?;
        for row in &all_rows {
            eval_where(&delete.where_clause, row, &table_def)?;
        }
        // Now execute the delete — safe to unwrap since we validated above
        let wc = delete.where_clause.clone();
        storage::delete_where(schema, table_name, |row| {
            eval_where(&wc, row, &table_def).unwrap_or(false)
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

    // Validate WHERE clause against all rows first (error propagation)
    let all_rows = storage::scan(schema, table_name)?;
    for row in &all_rows {
        eval_where(&update.where_clause, row, &table_def)?;
    }

    let wc = update.where_clause.clone();
    let td = table_def.clone();
    let assigns = assignments.clone();

    let count = storage::update_rows_checked(
        schema,
        table_name,
        |row| eval_where(&wc, row, &td).unwrap_or(false),
        |row| {
            // Compute new row values, propagating errors
            let mut new_row = row.clone();
            for (col_idx, expr_node) in &assigns {
                new_row[*col_idx] = eval_expr(expr_node, row, &td)?;
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
}
