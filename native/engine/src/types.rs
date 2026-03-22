use serde::{Deserialize, Serialize};

/// Runtime values for SQL expressions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Bytea(Vec<u8>),
}

impl Value {
    pub fn to_text(&self) -> Option<String> {
        match self {
            Value::Null => None,
            Value::Bool(b) => Some(if *b { "t" } else { "f" }.to_string()),
            Value::Int(i) => Some(i.to_string()),
            Value::Float(f) => Some(f.to_string()),
            Value::Text(s) => Some(s.clone()),
            Value::Bytea(b) => Some(format!("\\x{}", hex_encode(b))),
        }
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Column type OIDs matching PostgreSQL.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum TypeOid {
    Bool = 16,
    Bytea = 17,
    Int8 = 20,
    Int2 = 21,
    Int4 = 23,
    Text = 25,
    Float4 = 700,
    Float8 = 701,
    Varchar = 1043,
    Numeric = 1700,
}

impl TypeOid {
    pub fn from_name(name: &str) -> Self {
        match name.to_lowercase().as_str() {
            "bool" | "boolean" => TypeOid::Bool,
            "int2" | "smallint" => TypeOid::Int2,
            "int4" | "integer" | "int" | "serial" => TypeOid::Int4,
            "int8" | "bigint" | "bigserial" => TypeOid::Int8,
            "float4" | "real" => TypeOid::Float4,
            "float8" | "double precision" => TypeOid::Float8,
            "numeric" | "decimal" => TypeOid::Numeric,
            "text" => TypeOid::Text,
            "varchar" | "character varying" => TypeOid::Varchar,
            "bytea" => TypeOid::Bytea,
            _ => TypeOid::Text,
        }
    }

    pub fn oid(&self) -> i32 {
        *self as i32
    }
}
