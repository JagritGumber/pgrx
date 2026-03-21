use pg_query::{protobuf::ParseResult, NodeEnum};

/// Parse SQL and return a JSON summary of statement types.
pub fn parse(sql: &str) -> Result<String, String> {
    let result = pg_query::parse(sql).map_err(|e| e.to_string())?;

    let stmts: Vec<String> = result
        .protobuf
        .stmts
        .iter()
        .filter_map(|raw| raw.stmt.as_ref())
        .filter_map(|stmt| stmt.node.as_ref())
        .map(|node| classify_node(node))
        .collect();

    serde_json::to_string(&stmts).map_err(|e| e.to_string())
}

/// Parse SQL and return the full pg_query AST as JSON.
pub fn parse_ast(sql: &str) -> Result<String, String> {
    let result = pg_query::parse(sql).map_err(|e| e.to_string())?;
    // pg_query's protobuf result serializes to JSON via debug/serde
    let json = protobuf_to_json(&result.protobuf);
    Ok(json)
}

fn classify_node(node: &NodeEnum) -> String {
    match node {
        NodeEnum::SelectStmt(_) => "SELECT".to_string(),
        NodeEnum::InsertStmt(_) => "INSERT".to_string(),
        NodeEnum::UpdateStmt(_) => "UPDATE".to_string(),
        NodeEnum::DeleteStmt(_) => "DELETE".to_string(),
        NodeEnum::CreateStmt(_) => "CREATE_TABLE".to_string(),
        NodeEnum::IndexStmt(_) => "CREATE_INDEX".to_string(),
        NodeEnum::DropStmt(_) => "DROP".to_string(),
        NodeEnum::AlterTableStmt(_) => "ALTER_TABLE".to_string(),
        NodeEnum::CreateSchemaStmt(_) => "CREATE_SCHEMA".to_string(),
        NodeEnum::ViewStmt(_) => "CREATE_VIEW".to_string(),
        NodeEnum::TruncateStmt(_) => "TRUNCATE".to_string(),
        NodeEnum::TransactionStmt(_) => "TRANSACTION".to_string(),
        NodeEnum::ExplainStmt(_) => "EXPLAIN".to_string(),
        NodeEnum::CopyStmt(_) => "COPY".to_string(),
        NodeEnum::VariableSetStmt(_) => "SET".to_string(),
        NodeEnum::VariableShowStmt(_) => "SHOW".to_string(),
        _ => format!("{:?}", std::mem::discriminant(node)),
    }
}

fn protobuf_to_json(result: &ParseResult) -> String {
    serde_json::to_string(result).unwrap_or_else(|e| format!("{{\"error\": \"{}\"}}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_select() {
        let result = parse("SELECT 1").unwrap();
        assert!(result.contains("SELECT"));
    }

    #[test]
    fn parse_create_table() {
        let result = parse("CREATE TABLE users (id int, name text)").unwrap();
        assert!(result.contains("CREATE_TABLE"));
    }

    #[test]
    fn parse_insert() {
        let result = parse("INSERT INTO users (id) VALUES (1)").unwrap();
        assert!(result.contains("INSERT"));
    }

    #[test]
    fn parse_complex() {
        let result = parse(
            "SELECT u.id, u.name, COUNT(p.id) as post_count \
             FROM users u \
             LEFT JOIN posts p ON p.author_id = u.id \
             WHERE u.active = true \
             GROUP BY u.id, u.name \
             HAVING COUNT(p.id) > 5 \
             ORDER BY post_count DESC \
             LIMIT 10"
        ).unwrap();
        assert!(result.contains("SELECT"));
    }

    #[test]
    fn parse_cte() {
        let result = parse(
            "WITH active_users AS (SELECT * FROM users WHERE active = true) \
             SELECT * FROM active_users"
        ).unwrap();
        assert!(result.contains("SELECT"));
    }

    #[test]
    fn parse_multiple_statements() {
        let result = parse("SELECT 1; SELECT 2; INSERT INTO t VALUES (1)").unwrap();
        let stmts: Vec<String> = serde_json::from_str(&result).unwrap();
        assert_eq!(stmts.len(), 3);
    }

    #[test]
    fn parse_error() {
        let result = parse("SELEC BROKEN SQL");
        assert!(result.is_err());
    }
}
