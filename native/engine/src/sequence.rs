use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::LazyLock;

static SEQUENCES: LazyLock<RwLock<HashMap<String, Sequence>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

struct Sequence {
    current: i64,
    increment: i64,
    start: i64,
}

fn key(schema: &str, name: &str) -> String {
    format!("{}.{}", schema, name)
}

pub fn create_sequence(
    schema: &str,
    name: &str,
    start: i64,
    increment: i64,
) -> Result<(), String> {
    let mut seqs = SEQUENCES.write();
    let k = key(schema, name);
    if seqs.contains_key(&k) {
        return Err(format!("sequence \"{}\" already exists", name));
    }
    seqs.insert(
        k,
        Sequence {
            current: start - increment, // nextval will advance to start
            increment,
            start,
        },
    );
    Ok(())
}

pub fn nextval(schema: &str, name: &str) -> Result<i64, String> {
    let mut seqs = SEQUENCES.write();
    let seq = seqs
        .get_mut(&key(schema, name))
        .ok_or_else(|| format!("sequence \"{}\" does not exist", name))?;
    seq.current = seq
        .current
        .checked_add(seq.increment)
        .ok_or("sequence value out of range")?;
    Ok(seq.current)
}

pub fn currval(schema: &str, name: &str) -> Result<i64, String> {
    let seqs = SEQUENCES.read();
    let seq = seqs
        .get(&key(schema, name))
        .ok_or_else(|| format!("sequence \"{}\" does not exist", name))?;
    Ok(seq.current)
}

pub fn setval(schema: &str, name: &str, value: i64) -> Result<i64, String> {
    let mut seqs = SEQUENCES.write();
    let seq = seqs
        .get_mut(&key(schema, name))
        .ok_or_else(|| format!("sequence \"{}\" does not exist", name))?;
    seq.current = value;
    Ok(value)
}

pub fn drop_sequence(schema: &str, name: &str) {
    let mut seqs = SEQUENCES.write();
    seqs.remove(&key(schema, name));
}

pub fn reset() {
    let mut seqs = SEQUENCES.write();
    seqs.clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[serial_test::serial]
    fn nextval_increments() {
        reset();
        create_sequence("public", "test_seq", 1, 1).unwrap();
        assert_eq!(nextval("public", "test_seq").unwrap(), 1);
        assert_eq!(nextval("public", "test_seq").unwrap(), 2);
        assert_eq!(nextval("public", "test_seq").unwrap(), 3);
    }

    #[test]
    #[serial_test::serial]
    fn custom_start_and_increment() {
        reset();
        create_sequence("public", "s", 100, 10).unwrap();
        assert_eq!(nextval("public", "s").unwrap(), 100);
        assert_eq!(nextval("public", "s").unwrap(), 110);
    }

    #[test]
    #[serial_test::serial]
    fn currval_works() {
        reset();
        create_sequence("public", "s", 1, 1).unwrap();
        nextval("public", "s").unwrap();
        assert_eq!(currval("public", "s").unwrap(), 1);
    }

    #[test]
    #[serial_test::serial]
    fn setval_works() {
        reset();
        create_sequence("public", "s", 1, 1).unwrap();
        setval("public", "s", 50).unwrap();
        assert_eq!(nextval("public", "s").unwrap(), 51);
    }
}
