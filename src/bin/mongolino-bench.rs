#[path = "../main.rs"]
#[allow(dead_code)]
mod mongolino;

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bson::{Bson, Document, doc};
use mongolino::{ClientState, Result, handle_command_with_state, init_connection};
use rusqlite::Connection;

const DB: &str = "bench";
const COLL: &str = "users";

#[derive(Clone, Copy, Debug)]
struct Profile {
    name: &'static str,
    documents: usize,
    iterations: usize,
    insert_batch: usize,
}

impl Profile {
    fn from_name(name: &str) -> Option<Self> {
        match name {
            "smoke" => Some(Self {
                name: "smoke",
                documents: 400,
                iterations: 25,
                insert_batch: 50,
            }),
            "ci" => Some(Self {
                name: "ci",
                documents: 600,
                iterations: 30,
                insert_batch: 60,
            }),
            "local" => Some(Self {
                name: "local",
                documents: 3_000,
                iterations: 100,
                insert_batch: 100,
            }),
            _ => None,
        }
    }
}

#[derive(Debug)]
struct Args {
    profile: Profile,
    json_path: Option<PathBuf>,
    check_budget: bool,
}

#[derive(Debug)]
struct BenchResult {
    name: &'static str,
    dataset_size: usize,
    iterations: usize,
    elapsed: Duration,
    operations: usize,
}

impl BenchResult {
    fn elapsed_ms(&self) -> f64 {
        self.elapsed.as_secs_f64() * 1_000.0
    }

    fn ops_per_second(&self) -> f64 {
        self.operations as f64 / self.elapsed.as_secs_f64().max(0.000_001)
    }

    fn latency_ms(&self) -> f64 {
        self.elapsed_ms() / self.iterations.max(1) as f64
    }
}

fn main() {
    match run() {
        Ok(()) => {}
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    }
}

fn run() -> Result<()> {
    let args = parse_args()?;
    let git_commit = git_commit();
    let mut results = Vec::new();

    results.push(bench_insert_batch(args.profile)?);

    let mut harness = Harness::new(args.profile)?;
    harness.seed()?;
    results.push(harness.bench_command(
        "find_id_equality",
        args.profile.iterations,
        doc! {
            "find": COLL,
            "filter": { "_id": format!("user-{}", args.profile.documents / 2) },
            "singleBatch": true,
            "$db": DB,
        },
    )?);
    results.push(harness.bench_command(
        "find_collection_scan",
        args.profile.iterations,
        doc! {
            "find": COLL,
            "filter": { "profile.city": "Stockholm" },
            "singleBatch": true,
            "$db": DB,
        },
    )?);
    results.push(harness.bench_command(
        "find_indexed_scalar_equality",
        args.profile.iterations,
        doc! {
            "find": COLL,
            "filter": { "email": format!("user{}@example.test", args.profile.documents / 2) },
            "singleBatch": true,
            "$db": DB,
        },
    )?);
    results.push(harness.bench_command(
        "count_empty_filter",
        args.profile.iterations,
        doc! {
            "count": COLL,
            "query": {},
            "$db": DB,
        },
    )?);
    results.push(harness.bench_command(
        "count_simple_equality",
        args.profile.iterations,
        doc! {
            "count": COLL,
            "query": { "team": "platform" },
            "$db": DB,
        },
    )?);
    results.push(harness.bench_update_index_refresh()?);
    results.push(harness.bench_command(
        "aggregation_match_count",
        args.profile.iterations,
        doc! {
            "aggregate": COLL,
            "pipeline": [
                { "$match": { "team": "platform" } },
                { "$count": "n" },
            ],
            "cursor": { "batchSize": 1000_i32 },
            "$db": DB,
        },
    )?);
    results.push(harness.bench_command(
        "aggregation_unwind_group",
        args.profile.iterations,
        doc! {
            "aggregate": COLL,
            "pipeline": [
                { "$unwind": "$tags" },
                { "$group": { "_id": "$tags", "n": { "$sum": 1_i32 } } },
            ],
            "cursor": { "batchSize": 1000_i32 },
            "$db": DB,
        },
    )?);

    print_summary(args.profile, &git_commit, &results);
    if let Some(path) = args.json_path {
        fs::write(path, json_output(args.profile, &git_commit, &results))?;
    }
    if args.check_budget {
        check_budget(args.profile, &results)?;
    }

    Ok(())
}

fn parse_args() -> Result<Args> {
    let mut profile = Profile::from_name("smoke").expect("smoke profile exists");
    let mut json_path = None;
    let mut check_budget = false;
    let mut args = env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--profile" => {
                let value = args.next().ok_or_else(|| {
                    mongolino::MongolinoError::Protocol("--profile requires a value".to_string())
                })?;
                profile = Profile::from_name(&value).ok_or_else(|| {
                    mongolino::MongolinoError::Protocol(format!(
                        "unknown profile '{value}', expected smoke, ci, or local"
                    ))
                })?;
            }
            "--json" => {
                json_path = Some(PathBuf::from(args.next().ok_or_else(|| {
                    mongolino::MongolinoError::Protocol("--json requires a path".to_string())
                })?));
            }
            "--check-budget" => check_budget = true,
            "--help" | "-h" => {
                println!(
                    "mongolino-bench\n\nUsage: cargo run --bin mongolino-bench -- [--profile smoke|ci|local] [--json PATH] [--check-budget]"
                );
                std::process::exit(0);
            }
            unknown => {
                return Err(mongolino::MongolinoError::Protocol(format!(
                    "unknown argument: {unknown}"
                )));
            }
        }
    }

    Ok(Args {
        profile,
        json_path,
        check_budget,
    })
}

struct Harness {
    profile: Profile,
    conn: Connection,
    _temp_db: TempBenchmarkDatabase,
    client_state: ClientState,
}

impl Harness {
    fn new(profile: Profile) -> Result<Self> {
        let temp_db = TempBenchmarkDatabase::new("workload");
        let conn = Connection::open(temp_db.path())?;
        init_connection(&conn)?;
        Ok(Self {
            profile,
            conn,
            _temp_db: temp_db,
            client_state: ClientState::default(),
        })
    }

    fn seed(&mut self) -> Result<()> {
        self.command(doc! {
            "createIndexes": COLL,
            "indexes": [
                { "key": { "email": 1_i32 }, "name": "email_1" },
                { "key": { "team": 1_i32 }, "name": "team_1" },
                { "key": { "active": 1_i32 }, "name": "active_1" },
            ],
            "$db": DB,
        })?;

        for chunk_start in (0..self.profile.documents).step_by(self.profile.insert_batch) {
            let chunk_end = (chunk_start + self.profile.insert_batch).min(self.profile.documents);
            let documents = (chunk_start..chunk_end)
                .map(seed_document)
                .map(Bson::Document)
                .collect::<Vec<_>>();
            self.command(doc! {
                "insert": COLL,
                "documents": documents,
                "ordered": true,
                "$db": DB,
            })?;
        }
        Ok(())
    }

    fn command(&mut self, command: Document) -> Result<Document> {
        let response = handle_command_with_state(&self.conn, &mut self.client_state, &command)?;
        assert_ok(&response, &format!("{command:?}"))?;
        Ok(response)
    }

    fn bench_command(
        &mut self,
        name: &'static str,
        iterations: usize,
        command: Document,
    ) -> Result<BenchResult> {
        let start = Instant::now();
        for _ in 0..iterations {
            self.command(command.clone())?;
        }
        Ok(BenchResult {
            name,
            dataset_size: self.profile.documents,
            iterations,
            elapsed: start.elapsed(),
            operations: iterations,
        })
    }

    fn bench_update_index_refresh(&mut self) -> Result<BenchResult> {
        let start = Instant::now();
        for i in 0..self.profile.iterations {
            let id = format!("user-{}", i % self.profile.documents);
            let updated_team = if i % 2 == 0 { "platform" } else { "growth" };
            self.command(doc! {
                "update": COLL,
                "updates": [
                    {
                        "q": { "_id": id },
                        "u": {
                            "$set": {
                                "team": updated_team,
                                "email": format!("updated{}@example.test", i),
                            },
                            "$inc": { "score": 1_i32 },
                        },
                        "multi": false,
                        "upsert": false,
                    }
                ],
                "ordered": true,
                "$db": DB,
            })?;
        }
        Ok(BenchResult {
            name: "update_index_refresh",
            dataset_size: self.profile.documents,
            iterations: self.profile.iterations,
            elapsed: start.elapsed(),
            operations: self.profile.iterations,
        })
    }
}

fn bench_insert_batch(profile: Profile) -> Result<BenchResult> {
    let temp_db = TempBenchmarkDatabase::new("insert");
    let conn = Connection::open(temp_db.path())?;
    init_connection(&conn)?;
    let mut client_state = ClientState::default();
    let start = Instant::now();
    let mut operations = 0;

    for batch in 0..profile.iterations {
        let offset = batch * profile.insert_batch;
        let documents = (offset..offset + profile.insert_batch)
            .map(seed_document)
            .map(Bson::Document)
            .collect::<Vec<_>>();
        let command = doc! {
            "insert": COLL,
            "documents": documents,
            "ordered": true,
            "$db": DB,
        };
        let response = handle_command_with_state(&conn, &mut client_state, &command)?;
        assert_ok(&response, "insert batch")?;
        operations += profile.insert_batch;
    }

    Ok(BenchResult {
        name: "insert_batch_throughput",
        dataset_size: profile.iterations * profile.insert_batch,
        iterations: profile.iterations,
        elapsed: start.elapsed(),
        operations,
    })
}

#[derive(Debug)]
struct TempBenchmarkDatabase {
    path: PathBuf,
}

impl TempBenchmarkDatabase {
    fn new(label: &str) -> Self {
        Self {
            path: temp_db_path(label),
        }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempBenchmarkDatabase {
    fn drop(&mut self) {
        cleanup_sqlite_database(&self.path);
    }
}

fn cleanup_sqlite_database(path: &Path) {
    for candidate in sqlite_database_paths(path) {
        let _ = fs::remove_file(candidate);
    }
}

fn sqlite_database_paths(path: &Path) -> [PathBuf; 4] {
    [
        path.to_path_buf(),
        sqlite_sidecar_path(path, "-wal"),
        sqlite_sidecar_path(path, "-shm"),
        sqlite_sidecar_path(path, "-journal"),
    ]
}

fn sqlite_sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_os_string();
    value.push(suffix);
    PathBuf::from(value)
}

fn seed_document(i: usize) -> Document {
    let team = match i % 4 {
        0 => "platform",
        1 => "growth",
        2 => "infra",
        _ => "data",
    };
    let city = match i % 5 {
        0 => "Stockholm",
        1 => "London",
        2 => "New York",
        3 => "Berlin",
        _ => "Paris",
    };
    let tags = match i % 3 {
        0 => vec!["rust", "sqlite", "wire"],
        1 => vec!["storage", "query"],
        _ => vec!["aggregation", "indexes", "bson"],
    };
    doc! {
        "_id": format!("user-{i}"),
        "email": format!("user{i}@example.test"),
        "team": team,
        "active": i % 2 == 0,
        "score": (i % 1_000) as i32,
        "profile": {
            "city": city,
            "level": (i % 7) as i32,
        },
        "tags": tags,
    }
}

fn assert_ok(response: &Document, context: &str) -> Result<()> {
    match response.get_f64("ok") {
        Ok(value) if (value - 1.0).abs() < f64::EPSILON => Ok(()),
        _ => Err(mongolino::MongolinoError::Protocol(format!(
            "command failed during benchmark {context}: {response:?}"
        ))),
    }
}

fn temp_db_path(label: &str) -> PathBuf {
    let mut path = env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time after epoch")
        .as_nanos();
    path.push(format!(
        "mongolino-bench-{label}-{}-{nanos}.sqlite3",
        std::process::id()
    ));
    path
}

fn git_commit() -> String {
    Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}

fn print_summary(profile: Profile, git_commit: &str, results: &[BenchResult]) {
    println!(
        "mongolino benchmark profile={} git_commit={git_commit}",
        profile.name
    );
    println!(
        "{:<34} {:>10} {:>10} {:>12} {:>12} {:>12}",
        "benchmark", "dataset", "iters", "elapsed_ms", "ops_sec", "latency_ms"
    );
    for result in results {
        println!(
            "{:<34} {:>10} {:>10} {:>12.2} {:>12.2} {:>12.3}",
            result.name,
            result.dataset_size,
            result.iterations,
            result.elapsed_ms(),
            result.ops_per_second(),
            result.latency_ms()
        );
    }
}

fn json_output(profile: Profile, git_commit: &str, results: &[BenchResult]) -> String {
    let mut out = String::new();
    out.push_str("{\n");
    out.push_str(&format!("  \"profile\": \"{}\",\n", profile.name));
    out.push_str(&format!("  \"git_commit\": \"{}\",\n", escape(git_commit)));
    out.push_str("  \"benchmarks\": [\n");
    for (index, result) in results.iter().enumerate() {
        let comma = if index + 1 == results.len() { "" } else { "," };
        out.push_str(&format!(
            concat!(
                "    {{",
                "\"name\":\"{}\",",
                "\"dataset_size\":{},",
                "\"iterations\":{},",
                "\"elapsed_ms\":{:.3},",
                "\"operations\":{},",
                "\"ops_per_second\":{:.3},",
                "\"latency_ms\":{:.6}",
                "}}{}\n"
            ),
            result.name,
            result.dataset_size,
            result.iterations,
            result.elapsed_ms(),
            result.operations,
            result.ops_per_second(),
            result.latency_ms(),
            comma
        ));
    }
    out.push_str("  ]\n");
    out.push_str("}\n");
    out
}

fn escape(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn check_budget(profile: Profile, results: &[BenchResult]) -> Result<()> {
    let mut failures = Vec::new();
    for result in results {
        let threshold = budget_threshold(profile.name, result.name);
        if result.latency_ms() > threshold.max_latency_ms {
            failures.push(format!(
                "{} latency {:.3}ms exceeded {:.3}ms",
                result.name,
                result.latency_ms(),
                threshold.max_latency_ms
            ));
        }
        if result.ops_per_second() < threshold.min_ops_per_second {
            failures.push(format!(
                "{} throughput {:.2} ops/s below {:.2} ops/s",
                result.name,
                result.ops_per_second(),
                threshold.min_ops_per_second
            ));
        }
    }

    if failures.is_empty() {
        println!("performance budget passed for profile={}", profile.name);
        Ok(())
    } else {
        Err(mongolino::MongolinoError::Protocol(format!(
            "performance budget failed:\n{}",
            failures.join("\n")
        )))
    }
}

struct BudgetThreshold {
    max_latency_ms: f64,
    min_ops_per_second: f64,
}

fn budget_threshold(profile: &str, benchmark: &str) -> BudgetThreshold {
    let scale = if profile == "ci" { 1.0 } else { 1.5 };
    let (max_latency_ms, min_ops_per_second) = match benchmark {
        "insert_batch_throughput" => (2_500.0, 50.0),
        "find_id_equality" => (25.0, 40.0),
        "find_collection_scan" => (250.0, 4.0),
        "find_indexed_scalar_equality" => (80.0, 12.0),
        "count_empty_filter" => (250.0, 4.0),
        "count_simple_equality" => (250.0, 4.0),
        "update_index_refresh" => (150.0, 6.0),
        "aggregation_match_count" => (350.0, 3.0),
        "aggregation_unwind_group" => (600.0, 1.5),
        _ => (1_000.0, 1.0),
    };
    BudgetThreshold {
        max_latency_ms: max_latency_ms * scale,
        min_ops_per_second,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_sqlite_database_removes_main_file_and_sidecars() {
        let db_path = temp_db_path("cleanup-test");
        let json_path = sqlite_sidecar_path(&db_path, ".json");
        let paths = sqlite_database_paths(&db_path);

        for path in &paths {
            fs::write(path, b"test").expect("create sqlite cleanup test file");
        }
        fs::write(&json_path, b"{}").expect("create json output test file");

        cleanup_sqlite_database(&db_path);

        for path in &paths {
            assert!(!path.exists(), "{} should be removed", path.display());
        }
        assert!(
            json_path.exists(),
            "{} should not be treated as a sqlite sidecar",
            json_path.display()
        );

        fs::remove_file(json_path).expect("remove json output test file");
    }

    #[test]
    fn cleanup_sqlite_database_ignores_missing_files() {
        let db_path = temp_db_path("cleanup-missing-test");

        cleanup_sqlite_database(&db_path);
        cleanup_sqlite_database(&db_path);
    }
}
