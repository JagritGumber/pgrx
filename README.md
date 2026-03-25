# pgRx

> **This project is in early development and highly unstable.** APIs, storage format, and wire protocol behavior will change without notice. Do not use in production. Contributions and feedback welcome — just know that everything is actively being reworked.

**PostgreSQL-compatible database built on BEAM + Rust.**

Same SQL. Same drivers. 100x less memory per connection.

---

## Why

PostgreSQL forks an OS process for every connection — **14.6 MB each**, even idle. 50 idle connections cost **729 MB**. This architecture hasn't changed since 1986.

pgRx replaces the process-per-connection model with BEAM lightweight processes (**~2 KB** active, **0 KB** hibernated) and a Rust query engine for parsing and execution.

| | PostgreSQL 17 | pgRx |
|---|---|---|
| Per idle connection | 14.6 MB | 0 KB (hibernated) |
| Per active connection | 14.6 MB | ~15 KB |
| 50 connections overhead | +729 MB | +768 KB |
| Connection model | fork() per conn | BEAM process |
| MVCC | Dead tuples + VACUUM | Undo-log (planned) |
| Cold start | 0 ms | 0 ms |

## Architecture

```
Client (psql, any PG driver)
  → BEAM process (2 KB per connection, supervised)
    → Rust NIF (parse + execute via pg_query)
      → Shared storage (RwLock, no per-connection duplication)
```

**BEAM** handles connections, wire protocol, fault tolerance, and clustering.
**Rust** handles SQL parsing (via `pg_query` — same parser PostgreSQL uses), expression evaluation, JOINs, and storage.

## What Works

- **Wire Protocol v3** — psql connects without changes
- **SQL** — JOINs (INNER, LEFT, RIGHT, FULL, CROSS), subqueries (IN, EXISTS, scalar, derived), aggregates (COUNT, SUM, AVG, MIN, MAX), GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET
- **DML** — INSERT, UPDATE, DELETE with RETURNING
- **DDL** — CREATE TABLE, DROP TABLE, SERIAL, DEFAULT, constraints (PK, UNIQUE, NOT NULL)
- **Types** — int2/4/8, float4/8, numeric, bool, text, varchar, bytea, serial, vector(N)
- **Vector** — `vector(N)` type with `<->` (L2), `<=>` (cosine), `<#>` (inner product) operators — pgvector wire-compatible
- **Protocol** — Simple query, extended query, prepared statements
- **Connection** — Hibernation (0 KB idle), built-in pooling via BEAM
- **Tests** — 97 engine tests passing

## Quick Start

```bash
# Clone
git clone https://github.com/JagritGumber/pgrx.git
cd pgrx

# Dependencies: Elixir ~1.18, OTP 27, Rust (2024 edition)
mix deps.get

# Build the Rust NIF
cd native/engine && cargo build --release && cd ../..

# Start pgRx
mix run --no-halt

# Connect with psql (default port 5432)
psql -h localhost -p 5432 -U postgres
```

## Roadmap

| Phase | Status | What |
|---|---|---|
| Wire protocol + basic CRUD | Done | psql connects, INSERT/SELECT/UPDATE/DELETE |
| JOINs + aggregates | Done | All JOIN types, GROUP BY, HAVING, ORDER BY |
| Subqueries | Done | IN, EXISTS, scalar, derived tables |
| Connection hibernation | Done | 0 KB idle connections |
| Vector type (pgvector compat) | Done | vector(N), distance operators |
| Persistent storage | Next | B-tree indexes, WAL, crash recovery |
| ACID transactions | Planned | BEGIN/COMMIT/ROLLBACK, isolation levels |
| Undo-log MVCC | Planned | Zero bloat, no VACUUM |
| BEAM clustering | Planned | Multi-node distribution |

See [FEATURES.md](FEATURES.md) for the complete specification.

## Benchmarks

**Memory: 50 idle connections**

| | PostgreSQL 17 | pgRx |
|---|---|---|
| Additional memory | +729 MB | +0 KB |
| Per connection | 14.6 MB | 0 KB (hibernated) |

**Query throughput**

pgRx achieves 2x the query throughput of PostgreSQL on equivalent hardware in initial benchmarks.

## License

[Business Source License 1.1](LICENSE) — the same license used by CockroachDB.

- Use pgRx for anything, including commercial production use
- Self-host freely
- Cannot offer pgRx as a managed database service

Converts to AGPL-3.0 five years after each version's release.

## Links

- [Demo site](https://pgrx-demo.jagritgumber-cloudflare.workers.dev/)
- [Feature specification](FEATURES.md)
- [sqlcx — cross-language SQL codegen](https://github.com/JagritGumber/sqlcx) (companion ORM)
