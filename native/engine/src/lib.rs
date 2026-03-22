mod catalog;
mod executor;
mod parser;
mod storage;
mod types;

use rustler::{Encoder, Env, Term};

#[rustler::nif]
fn ping() -> &'static str {
    "pong from rust engine"
}

#[rustler::nif(schedule = "DirtyCpu")]
fn parse_sql(sql: &str) -> Result<String, String> {
    parser::parse(sql)
}

#[rustler::nif(schedule = "DirtyCpu")]
fn parse_sql_ast(sql: &str) -> Result<String, String> {
    parser::parse_ast(sql)
}

/// Execute SQL and return result as a native Erlang term.
/// Returns {:ok, %{tag: str, columns: [[name, oid], ...], rows: [[val, ...], ...]}}
/// or {:error, message}
#[rustler::nif(schedule = "DirtyCpu")]
fn execute_sql<'a>(env: Env<'a>, sql: &str) -> Term<'a> {
    match executor::execute(sql) {
        Ok(result) => {
            let tag = result.tag.encode(env);
            let columns: Vec<Term<'a>> = result
                .columns
                .iter()
                .map(|(name, oid)| {
                    let pair: (Term, Term) = (name.as_str().encode(env), oid.encode(env));
                    pair.encode(env)
                })
                .collect();
            let rows: Vec<Term<'a>> = result
                .rows
                .iter()
                .map(|row| {
                    let vals: Vec<Term<'a>> = row
                        .iter()
                        .map(|v| match v {
                            Some(s) => s.as_str().encode(env),
                            None => rustler::types::atom::nil().encode(env),
                        })
                        .collect();
                    vals.encode(env)
                })
                .collect();

            let map = rustler::Term::map_new(env);
            let map = map.map_put(
                rustler::types::atom::Atom::from_str(env, "tag").unwrap().encode(env),
                tag,
            ).unwrap();
            let map = map.map_put(
                rustler::types::atom::Atom::from_str(env, "columns").unwrap().encode(env),
                columns.encode(env),
            ).unwrap();
            let map = map.map_put(
                rustler::types::atom::Atom::from_str(env, "rows").unwrap().encode(env),
                rows.encode(env),
            ).unwrap();

            (rustler::types::atom::ok(), map).encode(env)
        }
        Err(msg) => {
            (rustler::types::atom::error(), msg.as_str()).encode(env)
        }
    }
}

rustler::init!("Elixir.Engine.Native");
