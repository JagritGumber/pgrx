mod catalog;
mod executor;
mod parser;
mod storage;
mod types;

#[rustler::nif]
fn ping() -> &'static str {
    "pong from rust engine"
}

#[rustler::nif]
fn parse_sql(sql: &str) -> Result<String, String> {
    parser::parse(sql)
}

#[rustler::nif]
fn parse_sql_ast(sql: &str) -> Result<String, String> {
    parser::parse_ast(sql)
}

#[rustler::nif]
fn execute_sql(sql: &str) -> Result<String, String> {
    let result = executor::execute(sql)?;
    serde_json::to_string(&result).map_err(|e| e.to_string())
}

rustler::init!("Elixir.Engine.Native");
