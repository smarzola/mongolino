# Goal: Performance Benchmark Tempfile Hygiene Fix Loop

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to fix the adversarial review finding from the performance benchmark foundation uplift: `mongolino-bench` creates realistic file-backed SQLite databases in the system temp directory, but it leaves `mongolino-bench-*.sqlite3` files behind after each run.

This is a focused fix. Do not broaden benchmark scope or change benchmark semantics beyond cleanup and regression coverage.

## Repository Rules

Follow `AGENTS.md`.

Important reminders:

- Keep the server implementation in Rust and the durable storage layer in SQLite.
- Treat MongoDB wire compatibility as observable behavior: validate changes with real client handshakes when possible.
- Run `cargo fmt` and `cargo test` before handing off code changes.
- Keep unsupported MongoDB commands explicit by returning command errors instead of silently accepting behavior.
- Do not require Docker, external MongoDB services, localhost binding, or PyMongo for the benchmark cleanup path.
- Do not revert unrelated user or agent changes.
- You are not alone in the codebase. Other agents may have touched nearby files; work with current state and do not revert unrelated edits.

## Review Finding

Running the benchmark command leaves temp SQLite files behind:

```sh
find "${TMPDIR:-/tmp}" -maxdepth 1 -name 'mongolino-bench-*.sqlite3*' -print
```

Example leaked files:

```text
mongolino-bench-workload-...sqlite3
mongolino-bench-insert-...sqlite3
```

The harness should still use file-backed SQLite databases because that better represents durable storage behavior, but it must clean up after successful runs and best-effort clean up after failures.

## Target Behavior

- Benchmark runs use temporary file-backed SQLite databases.
- Temp database files are removed when the benchmark connection is dropped.
- SQLite sidecar files are also removed if present:
  - `*.sqlite3-wal`
  - `*.sqlite3-shm`
  - `*.sqlite3-journal`
- Cleanup is best-effort and should not mask benchmark failures.
- JSON output files requested by the user are not removed.
- The benchmark command remains deterministic and still exercises real storage paths.

## Definition Of Done

The fix is complete only when:

1. `mongolino-bench` cleans up temporary benchmark SQLite files after successful runs.
2. Cleanup is best-effort on early errors where practical.
3. JSON output paths are preserved.
4. There is regression coverage for cleanup behavior, either through Rust unit tests for the cleanup helper or a focused command-level test that proves no benchmark DB files remain in a controlled temp directory.
5. CI budget command still passes.
6. Baseline/README docs are updated only if needed.
7. `cargo fmt -- --check`, `cargo test`, `cargo build`, `cargo run --bin mongolino-bench -- --profile ci --check-budget`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`, and `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e` pass locally. Use unsandboxed execution for localhost binding if needed.
8. This file has a completed status note with exact commands and final commit hash.
9. The completed fix is committed with a focused commit.

## Milestone Checklist

- [x] Milestone 0: Add cleanup helper and regression coverage
- [x] Milestone 1: Wire cleanup into benchmark harness
- [x] Milestone 2: Final verification and commit

## Milestone 0: Add Cleanup Helper And Regression Coverage

Problem:

- Cleanup should cover the main database file and SQLite sidecars without making benchmark code noisy.

Acceptance criteria:

- Add a small helper that owns or receives the temp database path and removes the main file plus sidecars.
- The helper is testable without running the full benchmark.
- Add tests proving the helper removes:
  - the main `.sqlite3` file;
  - `-wal`;
  - `-shm`;
  - `-journal`;
  - and ignores missing files.
- Tests do not delete user-requested JSON output files.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/bin/mongolino-bench.rs`
- `docs/performance-benchmark-adversarial-fix-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test --bin mongolino-bench
```

Status:

- 2026-07-04: Added `TempBenchmarkDatabase` and `cleanup_sqlite_database`
  helpers in `src/bin/mongolino-bench.rs`. Added unit tests covering removal
  of the main `.sqlite3` file, `-wal`, `-shm`, `-journal`, missing-file
  tolerance, and preservation of a non-sidecar `.json` output file. Verified
  with `cargo fmt -- --check` and `cargo test --bin mongolino-bench`.

## Milestone 1: Wire Cleanup Into Benchmark Harness

Problem:

- The current benchmark creates temp database paths but does not remove them.

Acceptance criteria:

- `Harness` cleans up its workload database when dropped.
- The insert benchmark cleans up its insert database.
- Cleanup occurs after successful runs and best-effort after failures.
- Running `cargo run --bin mongolino-bench -- --profile smoke` does not leave `mongolino-bench-*.sqlite3*` files in a controlled temp directory.
- Milestone status is marked done in this file and committed.

Verification:

```bash
cargo fmt -- --check
cargo test --bin mongolino-bench
cargo run --bin mongolino-bench -- --profile smoke --json /tmp/mongolino-bench-smoke.json
cargo run --bin mongolino-bench -- --profile ci --check-budget
```

Status:

- 2026-07-04: Wired workload and insert benchmark databases through
  `TempBenchmarkDatabase`, preserving file-backed SQLite benchmark semantics
  while removing the main DB and sidecars on drop. Verified no leaked
  benchmark SQLite files with
  `TMPDIR=/private/tmp/mongolino-bench-cleanup.diNDc0 cargo run --bin mongolino-bench -- --profile smoke --json /private/tmp/mongolino-bench-cleanup.diNDc0/mongolino-bench-smoke.json`
  followed by
  `find /private/tmp/mongolino-bench-cleanup.diNDc0 -maxdepth 1 -name 'mongolino-bench-*.sqlite3*' -print`;
  the JSON file remained present and the find command printed no SQLite files.

## Milestone 2: Final Verification And Commit

Acceptance criteria:

- Update this checklist and add a status note with:
  - date;
  - exact commands run;
  - any sandbox limitation encountered;
  - final commit hash.
- Full verification passes.

Verification:

```bash
cargo fmt -- --check
cargo test
cargo build
cargo run --bin mongolino-bench -- --profile ci --check-budget
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

Use unsandboxed execution for the PyMongo e2e suite if the sandbox blocks localhost binding.

Commit requirement:

- Commit with a focused message such as `Clean up benchmark temp databases`.

Status:

- 2026-07-04: Final verification completed. Exact commands run:
  `cargo fmt -- --check`;
  `cargo test --bin mongolino-bench`;
  `TMPDIR=/private/tmp/mongolino-bench-cleanup.diNDc0 cargo run --bin mongolino-bench -- --profile smoke --json /private/tmp/mongolino-bench-cleanup.diNDc0/mongolino-bench-smoke.json`;
  `find /private/tmp/mongolino-bench-cleanup.diNDc0 -maxdepth 1 -name 'mongolino-bench-*.sqlite3*' -print`;
  `cargo test`;
  `cargo build`;
  `cargo run --bin mongolino-bench -- --profile ci --check-budget`;
  `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`;
  `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`;
  `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e`;
  `find /private/tmp -maxdepth 1 -name 'mongolino-bench-*.sqlite3*' -print`.
  The sandboxed e2e run failed at localhost port allocation with
  `PermissionError: [Errno 1] Operation not permitted` from
  `sock.bind(("127.0.0.1", 0))`; the same e2e command passed outside the
  sandbox with 119 passed and 1 skipped. Final focused fix commit hash:
  `23f793e`.
