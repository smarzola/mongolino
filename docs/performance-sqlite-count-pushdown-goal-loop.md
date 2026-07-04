# Goal: SQLite Count And Match-Count Pushdown Performance Uplift

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to deliver the second large performance uplift in the performance sequence: use SQLite as the query engine for safe count workloads instead of decoding full namespaces in Rust. The benchmark baseline shows `count_empty_filter`, `count_simple_equality`, and aggregation `$match` + `$count` clustering with collection-scan latency. This uplift should push the safe subset into SQLite while preserving MongoDB-compatible behavior for the documented subset.

This is performance uplift 2 of 3.

## Repository Rules

Follow `AGENTS.md`.

Important reminders:

- Keep the server implementation in Rust and the durable storage layer in SQLite.
- Treat MongoDB wire compatibility as observable behavior: validate changes with real client handshakes when possible.
- Run `cargo fmt` and `cargo test` before handing off code changes.
- Keep unsupported MongoDB commands explicit by returning command errors instead of silently accepting behavior.
- Do not require Docker or external MongoDB services.
- Do not optimize by weakening matcher semantics.
- Do not revert unrelated user or agent changes.
- You are not alone in the codebase. Other agents may have touched nearby files; work with current state and do not revert unrelated edits.
- At the end of each milestone, run verification, mark the milestone done in this file, commit the completed milestone, and report the commit hash before continuing.

## Performance Target

Use SQLite for count workloads when behavior is equivalent:

- empty-filter `count` should use `SELECT COUNT(*) FROM documents WHERE namespace = ?`;
- `_id` equality count should use the `(namespace, id_key)` primary key;
- simple indexed scalar equality count should use maintained `index_entries`;
- aggregation pipelines shaped as safe `$match` followed by `$count` should reuse the same count planner and avoid full BSON namespace decode;
- unsupported or semantically risky filters must fall back to the existing Rust matcher path.

Baseline from `docs/performance-baseline.md` local profile:

- `count_empty_filter`: `29.298 ms/op`;
- `count_simple_equality`: `30.109 ms/op`;
- `aggregation_match_count`: `30.032 ms/op`;
- `_id` equality `find`: `0.023 ms/op`, proving SQLite primary-key lookup can be much faster;
- indexed scalar equality `find`: `2.098 ms/op`, proving maintained `index_entries` can narrow candidates.

Performance success target:

- improve `count_empty_filter`, `count_simple_equality`, and `aggregation_match_count` materially on the local benchmark profile;
- keep all correctness tests passing;
- keep fallback behavior for unsupported filters unchanged;
- update baseline docs with before/after numbers.

## Current State

The current implementation:

- `count_documents_command` always calls `documents_for_namespace`, decodes every BSON blob, then counts in Rust.
- `aggregate_pipeline_documents` always starts by loading all namespace documents.
- `candidate_documents` already uses `_id` and simple scalar index-entry narrowing for `find`, but count does not reuse this.
- `index_entries(namespace, index_name, key_value, id_key)` is maintained on insert/update/delete/drop.
- `simple_equality_filter_field` can identify one simple equality predicate from a filter.

Important constraints:

- MongoDB-style array matching and dotted array traversal are subtle. Only push down filters that are already represented by maintained scalar index entries or the `_id` primary key.
- `skip` and `limit` count semantics must remain correct. Empty filters with skip/limit can be expressed in SQL; indexed equality with skip/limit can be expressed after counting candidates.
- Unsupported filters must use the Rust matcher path, not silently change behavior.

## Definition Of Done

The goal is complete only when:

1. Empty-filter `count` avoids BSON namespace decode.
2. `_id` equality `count` avoids BSON namespace decode.
3. Simple scalar equality count uses maintained index entries when a matching single-field index exists.
4. Count with `skip` and `limit` preserves current semantics for pushed-down cases.
5. Count falls back to the existing Rust matcher for unsupported filters, array equality, unsupported operators, logical operators, and unindexed fields.
6. Aggregation pipeline `[{"$match": <safe filter>}, {"$count": <field>}]` uses the same SQLite count pushdown when safe.
7. Aggregation `$match` + `$count` fallback behavior remains unchanged for unsupported filters and for empty results.
8. Command errors for malformed count and aggregation stages remain unchanged.
9. Benchmarks include before/after evidence for `count_empty_filter`, `count_simple_equality`, and `aggregation_match_count`.
10. Performance budget command still passes.
11. README or performance baseline docs describe the new SQLite count pushdown and remaining fallback cases.
12. PyMongo e2e and spec corpus coverage prove count and aggregation count behavior through the real driver.
13. `cargo fmt -- --check`, `cargo test`, `cargo build`, `cargo run --bin mongolino-bench -- --profile ci --check-budget`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`, and `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e` pass locally. Use unsandboxed execution for localhost binding if needed.
14. Milestone checkboxes in this file are marked `[x]` as work completes.
15. Each completed milestone has a focused commit.

## Milestone Checklist

When a milestone is complete:

1. Run the milestone's verification commands.
2. Update this checklist by changing the milestone from `[ ]` to `[x]`.
3. Add a short status note under that milestone with the date, exact commands run, and commit hash if available.
4. Commit the code, tests, docs, and checklist/status update with a focused commit message.
5. Report the commit hash in the goal-loop status before starting the next milestone.

- [x] Milestone 0: Count planner design and safety tests
- [x] Milestone 1: Empty and `_id` count pushdown
- [x] Milestone 2: Indexed scalar equality count pushdown
- [x] Milestone 3: Aggregation `$match` + `$count` pushdown
- [x] Milestone 4: Benchmarks, docs, and final verification

## Milestone 0: Count Planner Design And Safety Tests

Status 2026-07-04:

- Added a conservative count planner that recognizes empty filters, exact `_id` equality, and exact indexed scalar equality, while falling back for arrays, logical operators, unsupported operators, multi-predicate filters, and unindexed fields.
- Verification: `cargo fmt -- --check`; `cargo test count`; `cargo test planner`; `cargo test`.
- Commit: `adc43aa`.

Problem:

- Count pushdown must be conservative. Returning a fast wrong count is worse than a full scan.

Desired behavior:

- Add a small count planner that classifies filters as SQL-countable or fallback.

Acceptance criteria:

- Planner recognizes:
  - empty filter;
  - `_id` literal equality or `$eq`;
  - simple scalar equality or `$eq` only when a matching maintained single-field index exists.
- Planner rejects/falls back for:
  - arrays;
  - logical operators;
  - unsupported operators;
  - `$in`, `$nin`, `$ne`, range operators, `$exists`, `$not`;
  - unindexed fields;
  - dotted fields unless a maintained single-field index exists and planner-entry semantics prove safe.
- Add Rust tests for planner classification.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `docs/performance-sqlite-count-pushdown-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test count
cargo test planner
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 1: Empty And `_id` Count Pushdown

Status 2026-07-04:

- Wired command `count` to use SQLite for empty filters and exact `_id` equality, with shared skip/limit adjustment. Indexed equality still falls back pending Milestone 2.
- Verification: `cargo fmt -- --check`; `cargo test count`; `cargo test`; sandboxed `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_metadata.py` failed at `socket.bind(("127.0.0.1", 0))` with `PermissionError: [Errno 1] Operation not permitted`; unsandboxed rerun of the same PyMongo command passed.
- Commit: `242cf4e`.

Problem:

- Counting all documents or one `_id` currently decodes full namespaces.

Desired behavior:

- Execute safe empty and `_id` counts directly through SQLite.

Acceptance criteria:

- Empty-filter count uses SQL count by namespace.
- `_id` equality count uses the primary key without decoding BSON.
- `skip` and `limit` are applied correctly:
  - skip removes the first matched results from the count;
  - positive limit caps the count;
  - limit `0` keeps existing no-limit semantics.
- Existing count tests and PyMongo `estimated_document_count()` behavior still pass.
- Add Rust tests proving pushed-down counts match fallback semantics.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_metadata.py`
- `docs/performance-sqlite-count-pushdown-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test count
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_metadata.py
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 2: Indexed Scalar Equality Count Pushdown

Status 2026-07-04:

- Extended SQLite count pushdown to exact scalar equality filters with a matching maintained single-field index, including dotted scalar paths represented in `index_entries`.
- Added scalar safety rejection for array, document, and null indexed operands; unsupported, unindexed, and multi-predicate filters continue to fall back to the Rust matcher path.
- Verification: `cargo fmt -- --check`; `cargo test count`; `cargo test planner`; `cargo test index`; unsandboxed `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_metadata.py tests/e2e/test_indexes.py`; `cargo test`.
- Commit: `1110639`.

Problem:

- Simple indexed equality count still decodes the namespace.

Desired behavior:

- Count through `index_entries` for maintained single-field indexes when safe.

Acceptance criteria:

- Count uses `index_entries` for simple scalar equality filters with matching single-field indexes.
- Count decodes no BSON on the pushed-down path.
- Count falls back when no matching index exists.
- Count falls back for array operands and unsupported operator shapes.
- Maintained index-entry freshness after insert/update/delete/drop remains covered.
- Add tests where count changes correctly after indexed field updates and deletes.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_metadata.py`
- `tests/e2e/test_indexes.py`
- `docs/performance-sqlite-count-pushdown-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test count
cargo test planner
cargo test index
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_metadata.py tests/e2e/test_indexes.py
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 3: Aggregation `$match` Plus `$count` Pushdown

Status 2026-07-04:

- Added an aggregation fast path for exact `[{"$match": <filter>}, {"$count": <field>}]` pipelines that reuses the SQLite count planner and preserves empty-result `$count` shape.
- Unsupported filters, malformed stages, and non-exact pipelines continue through the existing Rust aggregation executor and existing error behavior.
- Verification: `cargo fmt -- --check`; `cargo test aggregate`; unsandboxed `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_aggregation.py tests/e2e/test_spec_corpus.py`; `cargo test`.
- Commit: `4402545`.

Problem:

- Aggregation `$match` + `$count` currently loads the namespace and then counts in Rust.

Desired behavior:

- Detect safe two-stage pipelines and return cursor results from the SQLite count planner.

Acceptance criteria:

- Pipeline exactly shaped as `$match` followed by `$count` uses SQLite count planner when safe.
- Empty match result returns an empty firstBatch, preserving existing `$count` behavior.
- Unsupported filters fall back to existing aggregation pipeline execution.
- Malformed pipeline/stage errors remain unchanged.
- Cursor response shape and batch behavior remain correct.
- Add Rust and PyMongo e2e tests for safe pushed-down aggregation count and fallback cases.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_aggregation.py`
- `tests/spec_corpus/aggregation_pipeline.json`
- `docs/performance-sqlite-count-pushdown-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test aggregate
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_aggregation.py tests/e2e/test_spec_corpus.py
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 4: Benchmarks, Docs, And Final Verification

Status 2026-07-04:

- Ran smoke, CI, and local benchmarks after count pushdown; JSON outputs were written to `/tmp/mongolino-bench-count-smoke.json` and `/tmp/mongolino-bench-count-local.json`.
- Updated `docs/performance-baseline.md` with before/after count-pushdown evidence and remaining fallback cases.
- Local headline changes: `count_empty_filter` 29.298 -> 0.116 ms/op; `count_simple_equality` 30.109 -> 0.066 ms/op; `aggregation_match_count` 30.032 -> 0.071 ms/op.
- Verification: `cargo fmt -- --check`; `cargo test`; `cargo build`; `cargo run --bin mongolino-bench -- --profile smoke --json /tmp/mongolino-bench-count-smoke.json`; `cargo run --bin mongolino-bench -- --profile local --json /tmp/mongolino-bench-count-local.json`; `cargo run --bin mongolino-bench -- --profile ci --check-budget`; `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`; `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`; unsandboxed `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e`.
- Sandbox note: PyMongo e2e cannot bind localhost inside the sandbox; the exact sandbox error observed earlier was `PermissionError: [Errno 1] Operation not permitted` at `socket.bind(("127.0.0.1", 0))`.
- Commit: `bf175a1`.

Problem:

- The uplift must prove performance changed and document remaining SQLite-engine work.

Acceptance criteria:

- Run smoke and local benchmarks before/after if feasible.
- Update `docs/performance-baseline.md` with after numbers for:
  - `count_empty_filter`;
  - `count_simple_equality`;
  - `aggregation_match_count`.
- Update the pushdown roadmap to reflect completed count pushdown and next candidates.
- Ensure `cargo run --bin mongolino-bench -- --profile ci --check-budget` passes.
- Milestone status is marked done in this file and committed.

Verification:

```bash
cargo fmt -- --check
cargo test
cargo build
cargo run --bin mongolino-bench -- --profile smoke --json /tmp/mongolino-bench-count-smoke.json
cargo run --bin mongolino-bench -- --profile local --json /tmp/mongolino-bench-count-local.json
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
- pushed-down count cases;
- fallback cases that intentionally remain Rust-side;
- benchmark before/after headline numbers;
- final verification commands and outcomes;
- known residual risks.
