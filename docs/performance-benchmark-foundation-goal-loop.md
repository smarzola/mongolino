# Goal: Performance Benchmark Foundation And SQLite Query Engine Targets

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to deliver the first large performance uplift in a three-uplift performance sequence: build a repeatable benchmark foundation, capture current baseline behavior, and define measurable performance goals for moving more query work into SQLite instead of treating SQLite only as a BSON blob store.

This uplift must not optimize blindly. It should create the performance contract and evidence loop that the next two uplifts can use to push filtering/counting/projection and aggregation/grouping work down toward SQLite.

## Repository Rules

Follow `AGENTS.md`.

Important reminders:

- Keep the server implementation in Rust and the durable storage layer in SQLite.
- Treat MongoDB wire compatibility as observable behavior: validate changes with real client handshakes when possible.
- Run `cargo fmt` and `cargo test` before handing off code changes.
- Keep unsupported MongoDB commands explicit by returning command errors instead of silently accepting behavior.
- Use real local benchmark commands, not synthetic claims in docs.
- Do not require Docker or external MongoDB services.
- Do not revert unrelated user or agent changes.
- You are not alone in the codebase. Other agents may have touched nearby files; work with current state and do not revert unrelated edits.
- At the end of each milestone, run verification, mark the milestone done in this file, commit the completed milestone, and report the commit hash before continuing.

## Performance Goal

Set the working performance goal for this sequence:

> For simple supported queries and metadata operations, `mongolino` should use SQLite as the query engine when doing so is behaviorally equivalent to the documented MongoDB subset. The Rust BSON matcher remains the compatibility fallback, but benchmarked hot paths should avoid decoding entire namespaces when SQLite can narrow, count, or order the candidate set correctly.

Initial measurable targets for the three-uplift sequence:

1. Establish reproducible local and CI performance benchmarks for the current implementation.
2. Make benchmark output stable enough to compare before/after changes without external services.
3. Add regression budgets that catch severe performance regressions while remaining tolerant of CI variance.
4. Identify and document at least three SQLite-pushdown candidates with baseline timings:
   - `_id` equality and simple indexed scalar equality `find`;
   - `count`/`count_documents` for empty filters and simple equality filters;
   - aggregation `$match`/`$count` and `$group` workloads that currently decode full namespaces.
5. Preserve all correctness gates while adding performance gates.

## Current State

The repo currently has:

- SQLite durable storage in `documents(namespace, id_key, bson, created_at, updated_at)`.
- A primary key on `(namespace, id_key)`.
- `idx_documents_namespace_created` for namespace scans in insertion order.
- A maintained `index_entries` table for scalar equality index planning.
- `candidate_documents` that uses maintained index entries only for a conservative simple-equality subset, otherwise falls back to `documents_for_namespace`.
- `documents_for_namespace` that decodes all BSON blobs for the namespace.
- Aggregation that loads the namespace into memory and executes stages in Rust.
- CI that runs Rust formatting, Rust tests, Rust build, uv sync, and PyMongo e2e.

Important gaps:

- No benchmark suite exists.
- No performance budgets exist.
- README does not describe how to measure performance.
- CI does not run performance smoke checks.
- There is no written baseline that can guide SQLite pushdown decisions.

## Definition Of Done

The goal is complete only when:

1. A local benchmark command exists and is documented.
2. The benchmark command is deterministic enough for repeated local runs on the same machine.
3. The benchmark covers at least:
   - insert batch throughput;
   - `_id` equality find;
   - collection scan find;
   - indexed scalar equality find;
   - count empty filter;
   - count simple equality filter;
   - update path that refreshes index entries;
   - aggregation `$match`/`$count`;
   - aggregation `$unwind`/`$group`.
4. Benchmarks use the actual `mongolino` code paths and SQLite storage, not only isolated pure functions.
5. A performance budget command exists for CI-friendly smoke protection.
6. The budget command has explicit thresholds that are tolerant but useful.
7. CI runs the budget command.
8. Benchmark artifacts are text/JSON outputs suitable for comparing before and after changes.
9. A baseline document records the current measured results and explains machine/CI variance.
10. The baseline document identifies at least three concrete SQLite query-engine pushdown candidates for the next two uplifts.
11. The README development section mentions how to run benchmarks and performance smoke checks.
12. Correctness verification still passes: `cargo fmt -- --check`, `cargo test`, `cargo build`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`, and `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e`. Use unsandboxed execution for localhost binding if needed.
13. Milestone checkboxes in this file are marked `[x]` as work completes.
14. Each completed milestone has a focused commit.

## Milestone Checklist

When a milestone is complete:

1. Run the milestone's verification commands.
2. Update this checklist by changing the milestone from `[ ]` to `[x]`.
3. Add a short status note under that milestone with the date, exact commands run, and commit hash if available.
4. Commit the code, tests, docs, and checklist/status update with a focused commit message.
5. Report the commit hash in the goal-loop status before starting the next milestone.

- [x] Milestone 0: Benchmark harness design and workload seeding
- [x] Milestone 1: Benchmark command and machine-readable output
- [x] Milestone 2: CI-friendly performance budget smoke
- [x] Milestone 3: Baseline results and SQLite pushdown roadmap
- [x] Milestone 4: Final correctness verification and docs

## Milestone 0: Benchmark Harness Design And Workload Seeding

Status note:

- 2026-07-04: Added `src/bin/mongolino-bench.rs` with configurable `smoke`,
  `ci`, and `local` profiles. The harness opens temporary SQLite databases,
  initializes them through `init_connection`, seeds documents through the real
  `insert` command, creates maintained scalar indexes through `createIndexes`,
  and exercises real command/storage/query paths without localhost, PyMongo,
  Docker, or external services. Verification run:
  `cargo fmt -- --check`; `cargo test`. Commit hash: `558595c`.

Problem:

- Performance work needs representative data and stable workloads before optimization.

Desired behavior:

- Add a benchmark harness that creates temporary SQLite databases, initializes `mongolino` storage, seeds representative collections, and exercises real server/storage functions.

Acceptance criteria:

- Workloads use real SQLite files or temporary databases through the same initialization path as the server.
- Seed data includes enough documents to distinguish full namespace scans from indexed/equality paths.
- Seed data includes:
  - scalar `_id`;
  - indexed scalar fields such as `email`, `team`, `active`;
  - nested fields such as `profile.city`;
  - arrays for `$unwind`/`$group`;
  - documents that exercise index-entry refresh during update.
- The harness keeps dataset sizes configurable with sane defaults for local use and smaller CI smoke use.
- The harness does not require localhost binding, PyMongo, Docker, or external services.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs` if internal functions need test/bench access helpers
- `benches/` or `src/bin/`
- `docs/performance-benchmark-foundation-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 1: Benchmark Command And Machine-Readable Output

Status note:

- 2026-07-04: Documented `cargo run --bin mongolino-bench -- --profile
  smoke|ci|local` and JSON output. The smoke profile was run in readable and
  JSON modes, producing `/tmp/mongolino-bench-smoke.json` with profile,
  git commit, dataset size, iteration count, elapsed time, operations,
  ops/sec, and latency fields. Verification run: `cargo fmt -- --check`;
  `cargo test`; `cargo run --bin mongolino-bench -- --profile smoke`;
  `cargo run --bin mongolino-bench -- --profile smoke --json
  /tmp/mongolino-bench-smoke.json`. Commit hash: `2758dca`.

Problem:

- Developers need one command that records timings in a comparable format.

Desired behavior:

- Add a command such as `cargo run --bin mongolino-bench -- --profile local` or an equivalent repo-native command.

Acceptance criteria:

- The command prints a readable summary and writes JSON output when requested.
- JSON output includes:
  - benchmark name;
  - dataset size;
  - iteration count;
  - elapsed time;
  - operations per second or latency where appropriate;
  - git commit when available;
  - profile name.
- Benchmarks include every workload listed in Definition of Done item 3.
- The command exits non-zero on malformed arguments.
- The command is fast enough for local iteration by default.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/bin/mongolino-bench.rs`
- `README.md`
- `docs/performance-benchmark-foundation-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test
cargo run --bin mongolino-bench -- --profile smoke
cargo run --bin mongolino-bench -- --profile smoke --json /tmp/mongolino-bench-smoke.json
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 2: CI-Friendly Performance Budget Smoke

Status note:

- 2026-07-04: Added the `ci` profile smoke budget to GitHub Actions after
  `cargo build`, and documented that thresholds are intentionally coarse enough
  for CI variance while still catching severe regressions. Verification run:
  `cargo fmt -- --check`; `cargo run --bin mongolino-bench -- --profile ci
  --check-budget`; `cargo test`. Commit hash: `ab487d3`.

Problem:

- Full benchmarks are noisy in CI, but severe regressions should still be caught.

Desired behavior:

- Add a smoke-budget mode that runs fast and verifies coarse thresholds.

Acceptance criteria:

- Add a command such as `cargo run --bin mongolino-bench -- --profile ci --check-budget`.
- Thresholds are documented and intentionally coarse.
- Budget failures include benchmark names, measured values, and thresholds.
- The command does not depend on network, localhost binding, or PyMongo.
- CI runs this budget smoke after Rust build or after e2e, whichever fits the current workflow best.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/bin/mongolino-bench.rs`
- `.github/workflows/ci.yml`
- `README.md`
- `docs/performance-benchmark-foundation-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo run --bin mongolino-bench -- --profile ci --check-budget
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 3: Baseline Results And SQLite Pushdown Roadmap

Status note:

- 2026-07-04: Added `docs/performance-baseline.md` with smoke and local
  benchmark results from commit `ab487d3`, macOS/Darwin machine notes,
  interpretation of full namespace decode workloads, and prioritized SQLite
  pushdown candidates for count, indexed/equality find, aggregation
  `$match`/`$count`, and future `$unwind`/`$group` exploration. Verification
  run: `cargo run --bin mongolino-bench -- --profile smoke --json
  /tmp/mongolino-bench-smoke.json`; `cargo run --bin mongolino-bench --
  --profile local --json /tmp/mongolino-bench-local.json`; `cargo fmt --
  --check`. Commit hash: `ac1baef`.

Problem:

- The next performance uplifts need concrete targets and evidence, not broad claims.

Desired behavior:

- Record baseline results and turn them into a focused SQLite query-engine roadmap.

Acceptance criteria:

- Add `docs/performance-baseline.md` or equivalent.
- Include:
  - benchmark command used;
  - machine/OS note if available;
  - smoke profile results;
  - local profile results if feasible;
  - interpretation of which workloads decode full namespaces;
  - at least three prioritized SQLite pushdown candidates;
  - expected correctness risks for each candidate;
  - proposed measurement for each candidate.
- The roadmap must explicitly connect to the user's aspirational target: SQLite should become the query engine when behaviorally safe.
- Milestone status is marked done in this file and committed.

Likely files:

- `docs/performance-baseline.md`
- `README.md`
- `docs/performance-benchmark-foundation-goal-loop.md`

Verification:

```bash
cargo run --bin mongolino-bench -- --profile smoke --json /tmp/mongolino-bench-smoke.json
cargo run --bin mongolino-bench -- --profile local --json /tmp/mongolino-bench-local.json
cargo fmt -- --check
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 4: Final Correctness Verification And Docs

Status note:

- 2026-07-04: Final README, CI, benchmark, baseline, and checklist updates
  are in place. Verification run: `cargo fmt -- --check`; `cargo test`;
  `cargo build`; `cargo run --bin mongolino-bench -- --profile ci
  --check-budget`; `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock
  --check`; `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked
  --dev`; `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked
  pytest tests/e2e`. The sandboxed e2e run failed at localhost port
  allocation with `PermissionError: [Errno 1] Operation not permitted` from
  `sock.bind(("127.0.0.1", 0))` in `tests/e2e/conftest.py:103`; the same
  command passed unsandboxed with `119 passed, 1 skipped`. Commit hash:
  pending.

Problem:

- Benchmark infrastructure should not weaken compatibility or make CI brittle.

Acceptance criteria:

- README development docs include benchmark and budget commands.
- CI remains deterministic.
- Performance budget thresholds are not so tight that ordinary CI variance will fail them.
- Full correctness verification passes.
- Milestone status is marked done in this file and committed.

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

- Commit after marking this milestone done and adding the status note.

## Final Response Requirements

When complete, report:

- commits made;
- files changed;
- benchmark command examples;
- baseline headline results;
- budget command status;
- final verification commands and outcomes;
- prioritized SQLite query-engine pushdown candidates for the next uplift;
- known residual risks.
