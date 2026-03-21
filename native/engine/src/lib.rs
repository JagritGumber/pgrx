mod parser;

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

rustler::init!("Elixir.Engine.Native");
