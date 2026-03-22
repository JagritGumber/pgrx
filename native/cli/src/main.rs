mod wire;

use clap::Parser;
use rustyline::DefaultEditor;
use std::sync::{Arc, Barrier};
use std::time::Instant;

#[derive(Parser)]
#[command(name = "pgrx", about = "pgrx CLI — fast PostgreSQL-compatible client")]
struct Args {
    /// Host to connect to
    #[arg(short = 'h', long, default_value = "127.0.0.1")]
    host: String,

    /// Port
    #[arg(short, long, default_value_t = 5433)]
    port: u16,

    /// Username
    #[arg(short = 'U', long, default_value = "test")]
    user: String,

    /// Database name
    #[arg(short, long, default_value = "pgrx")]
    dbname: String,

    /// Password
    #[arg(short = 'W', long, default_value = "")]
    password: String,

    /// Execute a single command and exit
    #[arg(short)]
    c: Option<String>,

    /// Show query timing
    #[arg(long, default_value_t = false)]
    timing: bool,

    /// Benchmark mode: run command N times and show stats
    #[arg(long)]
    bench: Option<u32>,

    /// Concurrent benchmark: number of parallel clients
    #[arg(long)]
    clients: Option<u32>,

    /// Memory test: open N connections, run a query, then hold idle
    #[arg(long)]
    memtest: Option<u32>,
}

fn main() {
    let args = Args::parse();

    let mut conn = match wire::Connection::connect(
        &args.host,
        args.port,
        &args.user,
        &args.dbname,
        &args.password,
    ) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("connection failed: {}", e);
            std::process::exit(1);
        }
    };

    // Memory test mode
    if let Some(n) = args.memtest {
        conn.close();
        run_memtest(&args.host, args.port, &args.user, &args.dbname, &args.password, n);
        return;
    }

    // Concurrent benchmark mode
    if let (Some(sql), Some(n), Some(clients)) = (&args.c, args.bench, args.clients) {
        conn.close();
        run_concurrent(&args.host, args.port, &args.user, &args.dbname, &args.password, sql, n, clients);
        return;
    }

    // Single command mode
    if let Some(sql) = &args.c {
        if let Some(n) = args.bench {
            run_bench(&mut conn, sql, n);
        } else {
            run_one(&mut conn, sql, args.timing);
        }
        conn.close();
        return;
    }

    // Interactive REPL
    println!(
        "pgrx cli 0.1.0 — connected to {}:{}/{}",
        args.host, args.port, args.dbname
    );
    println!("Type \\q to quit, \\timing to toggle timing.\n");

    let mut rl = DefaultEditor::new().unwrap();
    let mut timing = args.timing;
    let mut buf = String::new();

    loop {
        let prompt = if buf.is_empty() {
            format!("{}=> ", args.dbname)
        } else {
            format!("{}-> ", args.dbname)
        };

        match rl.readline(&prompt) {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed == "\\q" || trimmed == "exit" || trimmed == "quit" {
                    break;
                }
                if trimmed == "\\timing" {
                    timing = !timing;
                    println!("Timing is {}.", if timing { "on" } else { "off" });
                    continue;
                }

                buf.push_str(&line);
                buf.push(' ');

                // Execute when we see a semicolon
                if buf.trim_end().ends_with(';') {
                    let sql = buf.trim().to_string();
                    rl.add_history_entry(&sql).ok();
                    run_one(&mut conn, &sql, timing);
                    buf.clear();
                }
            }
            Err(_) => break,
        }
    }

    conn.close();
}

fn run_one(conn: &mut wire::Connection, sql: &str, timing: bool) {
    let start = Instant::now();
    match conn.query(sql) {
        Ok(result) => {
            print_result(&result);
            if timing {
                println!("Time: {:.3} ms", start.elapsed().as_secs_f64() * 1000.0);
            }
        }
        Err(e) => eprintln!("ERROR: {}", e),
    }
}

fn run_bench(conn: &mut wire::Connection, sql: &str, n: u32) {
    println!("Benchmarking: {} ({} iterations)", sql, n);

    // Warmup
    for _ in 0..5 {
        let _ = conn.query(sql);
    }

    let mut times = Vec::with_capacity(n as usize);
    let total_start = Instant::now();

    for _ in 0..n {
        let start = Instant::now();
        match conn.query(sql) {
            Ok(_) => times.push(start.elapsed().as_secs_f64() * 1000.0),
            Err(e) => {
                eprintln!("ERROR: {}", e);
                return;
            }
        }
    }

    let total = total_start.elapsed().as_secs_f64() * 1000.0;
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let avg = times.iter().sum::<f64>() / times.len() as f64;
    let p50 = times[times.len() / 2];
    let p99 = times[(times.len() as f64 * 0.99) as usize];
    let min = times[0];
    let max = times[times.len() - 1];
    let qps = (n as f64 / total) * 1000.0;

    println!();
    println!("  Iterations: {}", n);
    println!("  Total:      {:.1} ms", total);
    println!("  QPS:        {:.0}", qps);
    println!("  Avg:        {:.3} ms", avg);
    println!("  P50:        {:.3} ms", p50);
    println!("  P99:        {:.3} ms", p99);
    println!("  Min:        {:.3} ms", min);
    println!("  Max:        {:.3} ms", max);
}

fn run_concurrent(
    host: &str, port: u16, user: &str, dbname: &str, password: &str,
    sql: &str, queries_per_client: u32, n_clients: u32,
) {
    let total_queries = n_clients * queries_per_client;
    println!("{} clients × {} queries = {} total", n_clients, queries_per_client, total_queries);

    let barrier = Arc::new(Barrier::new(n_clients as usize + 1));
    let sql = Arc::new(sql.to_string());
    let host = Arc::new(host.to_string());
    let user = Arc::new(user.to_string());
    let dbname = Arc::new(dbname.to_string());
    let password = Arc::new(password.to_string());

    let mut handles = Vec::new();

    for _ in 0..n_clients {
        let barrier = barrier.clone();
        let sql = sql.clone();
        let host = host.clone();
        let user = user.clone();
        let dbname = dbname.clone();
        let password = password.clone();

        handles.push(std::thread::spawn(move || -> Result<Vec<f64>, String> {
            let mut conn = wire::Connection::connect(&host, port, &user, &dbname, &password)?;

            // Warmup
            for _ in 0..3 {
                let _ = conn.query(&sql);
            }

            // Synchronize all threads
            barrier.wait();

            let mut times = Vec::with_capacity(queries_per_client as usize);
            for _ in 0..queries_per_client {
                let start = Instant::now();
                conn.query(&sql)?;
                times.push(start.elapsed().as_secs_f64() * 1000.0);
            }

            conn.close();
            Ok(times)
        }));
    }

    // Synchronize with worker threads so wall clock starts at the same moment
    barrier.wait();
    let wall_start = Instant::now();

    let mut all_times = Vec::new();
    let mut errors = 0u32;

    for h in handles {
        match h.join() {
            Ok(Ok(times)) => all_times.extend(times),
            Ok(Err(e)) => { errors += 1; eprintln!("  client error: {}", e); }
            Err(_) => { errors += 1; eprintln!("  client panicked"); }
        }
    }

    let wall_time = wall_start.elapsed().as_secs_f64() * 1000.0;

    if all_times.is_empty() {
        println!("  ALL FAILED ({} errors)", errors);
        return;
    }

    all_times.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let n = all_times.len();
    let avg = all_times.iter().sum::<f64>() / n as f64;
    let p50 = all_times[n / 2];
    let p99 = all_times[(n as f64 * 0.99) as usize];
    let qps = n as f64 / (wall_time / 1000.0);

    println!("  Completed:  {} queries", n);
    println!("  Wall time:  {:.0} ms", wall_time);
    println!("  Throughput: {:.0} QPS", qps);
    println!("  Avg:        {:.3} ms", avg);
    println!("  P50:        {:.3} ms", p50);
    println!("  P99:        {:.3} ms", p99);
    println!("  Min:        {:.3} ms", all_times[0]);
    println!("  Max:        {:.3} ms", all_times[n - 1]);
    if errors > 0 {
        println!("  Errors:     {}/{}", errors, n_clients);
    }
}

fn print_result(result: &wire::QueryResult) {
    if result.columns.is_empty() {
        println!("{}", result.tag);
        return;
    }

    let col_names: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();
    let num_cols = col_names.len();

    // Calculate column widths
    let mut widths: Vec<usize> = col_names.iter().map(|n| n.len()).collect();
    for row in &result.rows {
        for (i, val) in row.iter().enumerate() {
            if i < num_cols {
                let len = val.as_deref().unwrap_or("NULL").len();
                widths[i] = widths[i].max(len);
            }
        }
    }

    // Header
    let header: Vec<String> = col_names
        .iter()
        .enumerate()
        .map(|(i, n)| format!(" {:width$} ", n, width = widths[i]))
        .collect();
    println!("{}", header.join("|"));

    // Separator
    let sep: Vec<String> = widths.iter().map(|w| "-".repeat(w + 2)).collect();
    println!("{}", sep.join("+"));

    // Rows
    for row in &result.rows {
        let vals: Vec<String> = row
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let s = v.as_deref().unwrap_or("NULL");
                let w = if i < widths.len() { widths[i] } else { s.len() };
                format!(" {:width$} ", s, width = w)
            })
            .collect();
        println!("{}", vals.join("|"));
    }

    println!("({} rows)", result.rows.len());
}

fn get_beam_rss() -> Option<u64> {
    let output = std::process::Command::new("pgrep")
        .args(["-f", "beam.smp"])
        .output()
        .ok()?;
    let pid_str = String::from_utf8_lossy(&output.stdout);
    let pid = pid_str.trim().lines().next()?.trim();
    let status = std::fs::read_to_string(format!("/proc/{}/status", pid)).ok()?;
    for line in status.lines() {
        if line.starts_with("VmRSS:") {
            let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
            return Some(kb);
        }
    }
    None
}

fn run_memtest(
    host: &str, port: u16, user: &str, dbname: &str, password: &str,
    n_clients: u32,
) {
    let rss0 = get_beam_rss().unwrap_or(0);
    println!("=== Memory Test: {} connections ===", n_clients);
    println!("BASELINE:    {} KB RSS", rss0);

    // Open N connections
    let mut conns = Vec::new();
    for i in 0..n_clients {
        match wire::Connection::connect(host, port, user, dbname, password) {
            Ok(c) => conns.push(c),
            Err(e) => {
                eprintln!("  conn {} failed: {}", i, e);
                break;
            }
        }
    }
    println!("  {} connections opened", conns.len());

    // Run a query on each
    for conn in &mut conns {
        let _ = conn.query("SELECT 1;");
    }
    println!("  All queried (SELECT 1)");

    std::thread::sleep(std::time::Duration::from_secs(2));
    let rss1 = get_beam_rss().unwrap_or(0);
    let per_active = if conns.len() > 0 { (rss1.saturating_sub(rss0)) * 1024 / conns.len() as u64 } else { 0 };
    println!("ACTIVE:      {} KB RSS (+{} KB, ~{} bytes/conn)", rss1, rss1.saturating_sub(rss0), per_active);

    // Wait for hibernate
    println!("  Waiting 10s for hibernate...");
    std::thread::sleep(std::time::Duration::from_secs(10));

    let rss2 = get_beam_rss().unwrap_or(0);
    let freed = rss1.saturating_sub(rss2);
    let per_hib = if conns.len() > 0 { (rss2.saturating_sub(rss0)) * 1024 / conns.len() as u64 } else { 0 };
    println!("HIBERNATED:  {} KB RSS (freed {} KB, ~{} bytes/conn)", rss2, freed, per_hib);

    // Wake them all up
    for conn in &mut conns {
        let _ = conn.query("SELECT 1;");
    }
    std::thread::sleep(std::time::Duration::from_secs(1));
    let rss3 = get_beam_rss().unwrap_or(0);
    println!("WOKEN:       {} KB RSS (all re-queried)", rss3);

    // Close all
    for mut conn in conns {
        conn.close();
    }
    std::thread::sleep(std::time::Duration::from_secs(2));
    let rss4 = get_beam_rss().unwrap_or(0);
    println!("CLOSED:      {} KB RSS (residual +{} KB)", rss4, rss4.saturating_sub(rss0));

    println!();
    println!("=== SUMMARY ===");
    println!("  Baseline:      {} KB", rss0);
    println!("  {} active:     {} KB (+{} KB)", n_clients, rss1, rss1.saturating_sub(rss0));
    println!("  {} hibernated: {} KB (freed {} KB)", n_clients, rss2, freed);
    println!("  After close:   {} KB", rss4);
}
