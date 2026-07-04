# Goal: Scalar Multikey Index Compatibility And Performance Uplift

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to deliver the third large index uplift in the index
compatibility and performance sequence: add conservative scalar multikey index
semantics for array fields. The current planner omits array values from
maintained index entries, so array-field queries fall back to Rust scans even
when a MongoDB-style index would be useful.

This is index uplift 3 of 3. It must move the repo-local index compatibility
scorecard in `docs/index-uplifts-roadmap.md` from the post-sparse/partial
target of **67% to at least 78%**.

## Repository Rules

Follow `AGENTS.md`.

Important reminders:

- Keep the server implementation in Rust and the durable storage layer in
  SQLite.
- Treat MongoDB wire compatibility as observable behavior: validate changes
  with real client handshakes when possible.
- Run `cargo fmt` and `cargo test` before handing off code changes.
- Keep unsupported MongoDB commands explicit by returning command errors
  instead of silently accepting behavior.
- Do not require Docker or external MongoDB services.
- Do not optimize by weakening matcher semantics.
- Do not revert unrelated user or agent changes.
- You are not alone in the codebase. Other agents may have touched nearby
  files; work with current state and do not revert unrelated edits.
- At the end of each milestone, run verification, mark the milestone done in
  this file, commit the completed milestone, and report the commit hash before
  continuing.

## Target State

Support a conservative scalar multikey subset:

- Single-field indexes can maintain one `index_entries` row per scalar array
  element.
- Dotted paths through arrays can maintain scalar leaf entries when the path is
  unambiguous and supported by `values_at_path`.
- Scalar equality `find` and `count` can use multikey entries and still validate
  candidates through the Rust matcher.
- update/delete/findAndModify target selection can use multikey entries for
  safe scalar equality filters and still validate candidates before mutation.
- Unique multikey behavior is either implemented conservatively or returns
  explicit errors for unsupported unique array cases.
- Compound multikey indexes remain unsupported unless fully implemented and
  tested.

## Compatibility Target

Move the scorecard from **67% to at least 78%**:

- Sparse/partial/multikey practical semantics increase through scalar multikey
  query/index behavior.
- Planner use for reads and writes increases because array-element equality can
  use maintained entries.
- Explicit unsupported behavior stays complete for unique multikey and compound
  multikey shapes not implemented.

Do not claim support for `$elemMatch`, geospatial arrays, text indexes, wildcard
indexes, collation-aware multikey behavior, or full MongoDB multikey compound
rules in this uplift.

## Performance Target

Add benchmark cases for scalar array field indexing.

Required local-profile targets:

- `find_multikey_scalar_equality`: at least **5x faster** than collection scan
  and below **6 ms/op**.
- `count_multikey_scalar_equality`: below **1 ms/op** for safe scalar array
  element counts.
- `update_multikey_target`: below **4 ms/op** for selective scalar array
  equality write target selection.

## Current State

Relevant current behavior before this uplift:

- `bson_values_equal` and `values_at_path` let the Rust matcher match scalar
  predicates against array elements.
- `planner_key_for_document` currently skips array values.
- `indexed_candidate_documents` skips only array operands, not array fields, but
  array-field documents have no maintained entries.
- Unique indexes currently reject array values for supported unique semantics.
- Count and write planners rely on maintained `index_entries`.

Known constraints:

- Multikey entries can create multiple rows per document for a single index.
  The planner must deduplicate document ids before counting or returning
  candidates where needed.
- Unique multikey semantics are tricky. Prefer explicit unsupported errors over
  silently wrong duplicate handling.
- Numeric array values must preserve cross-type numeric matcher semantics. Fall
  back for numeric multikey planner paths unless equivalent encoding is proven.
- Compound multikey is out of scope unless complete.

## Definition Of Done

The goal is complete only when:

1. Single-field scalar multikey entries are maintained for supported arrays.
2. Dotted array leaf paths are handled where `values_at_path` semantics are
   safe and covered.
3. Duplicate index entries from repeated array elements do not duplicate query
   results or counts.
4. find/count/aggregation `$match` + `$count` use multikey entries for safe
   non-numeric scalar equality.
5. update/delete/findAndModify target selection uses multikey entries for safe
   non-numeric scalar equality and still validates candidates in Rust.
6. Insert/update/upsert/delete/findAndModify refresh multikey entries.
7. Unique array index behavior is either conservatively implemented or returns
   explicit command/write errors for unsupported cases.
8. Sparse/partial index membership from uplift 2 remains correct when array
   fields are involved.
9. PyMongo e2e covers scalar array equality find/count/update/delete and
   mutation freshness.
10. Benchmarks and docs record before/after numbers for multikey planner paths.
11. The scorecard in `docs/index-uplifts-roadmap.md` is updated to at least
    78% with evidence.
12. Verification commands pass locally.
13. Milestone checkboxes in this file are marked `[x]` as work completes.
14. Each completed milestone has a focused commit.

## Milestone Checklist

When a milestone is complete:

1. Run the milestone's verification commands.
2. Update this file by changing the milestone from `[ ]` to `[x]`.
3. Add a short status note under that milestone with the date, exact commands
   run, and commit hash if available.
4. Commit the code, tests, docs, and checklist/status update with a focused
   commit message.
5. Report the commit hash in the goal-loop status before starting the next
   milestone.

- [x] Milestone 0: Multikey entry model and deduplication
- [x] Milestone 1: Entry maintenance and mutation freshness
- [x] Milestone 2: Read/count/write planner pushdown
- [x] Milestone 3: Unique and unsupported multikey semantics
- [x] Milestone 4: Benchmarks, docs, and final verification

## Milestone 0: Multikey Entry Model And Deduplication

Problem:

- A single document can produce multiple index entries for one multikey index,
  and repeated array values can produce duplicate logical keys.

Desired behavior:

- Generate one logical key per distinct supported scalar value per document.
- Ensure SQL lookups do not return duplicate documents.
- Keep existing scalar single-value entries working.

Acceptance criteria:

- Rust tests cover repeated array element deduplication.
- Rust tests cover dotted scalar leaf extraction through arrays.
- Numeric arrays fall back unless equivalent encoding is proven.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `docs/index-multikey-scalar-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test planner
cargo test index
```

Status 2026-07-04: implemented distinct supported scalar multikey entry
generation for single-field indexes, dotted array leaf extraction, distinct SQL
candidate/count lookups, and numeric-array fallback sentinels. Verified with
`cargo fmt -- --check`, `cargo test planner`, and `cargo test index`. Commit:
`72c352f`.

## Milestone 1: Entry Maintenance And Mutation Freshness

Problem:

- Multikey entries must stay synchronized when arrays are inserted, updated,
  renamed, unset, pushed, pulled, or deleted.

Desired behavior:

- Rebuild and refresh multikey entries through existing index refresh hooks.
- Preserve sparse/partial membership if those uplifts are already present.
- Remove all stale entries on delete, drop index, drop collection, and drop
  database.

Acceptance criteria:

- PyMongo e2e covers insert, update operators, replacement update, delete, and
  findAndModify freshness.
- Rust tests cover stale entry removal.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_indexes.py`
- `tests/e2e/test_update_operators.py`
- `tests/e2e/test_find_and_modify.py`
- `docs/index-multikey-scalar-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test index
cargo test update
cargo test find_and_modify
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_indexes.py tests/e2e/test_update_operators.py tests/e2e/test_find_and_modify.py
cargo test
```

Status 2026-07-04: added PyMongo coverage for scalar multikey insert,
replacement, delete, update operators, and findAndModify freshness; adjusted
legacy omission-sentinel cleanup fixtures to use unsupported numeric arrays.
Verified with `cargo fmt -- --check`, `cargo test index`, `cargo test update`,
`cargo test find_and_modify`, `cargo build`,
`UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_indexes.py tests/e2e/test_update_operators.py tests/e2e/test_find_and_modify.py`,
and `cargo test`. The first sandboxed PyMongo attempt failed at localhost bind
with `PermissionError: Operation not permitted`, then passed unsandboxed.
Commit: `bd725f2`.

## Milestone 2: Read/Count/Write Planner Pushdown

Problem:

- Scalar array-element queries currently scan even when an index exists.

Desired behavior:

- Use multikey entries for safe scalar equality `find`.
- Use multikey entries for safe scalar equality `count` and exact aggregation
  `$match` + `$count`.
- Use multikey entries for safe update/delete/findAndModify target selection.
- Always validate candidates with the Rust matcher.

Acceptance criteria:

- PyMongo e2e covers scalar array equality find/count/update/delete.
- Rust tests prove duplicate entries do not duplicate results or counts.
- Fallback remains for numeric, arrays-as-query-operands, documents, and
  unsupported operators.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_indexes.py`
- `tests/e2e/test_metadata.py`
- `tests/e2e/test_aggregation.py`
- `tests/e2e/test_crud.py`
- `docs/index-multikey-scalar-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test find
cargo test count
cargo test aggregate_match_count
cargo test update
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_indexes.py tests/e2e/test_metadata.py tests/e2e/test_aggregation.py tests/e2e/test_crud.py
cargo test
```

Status 2026-07-04: kept covered indexed count pushdown on distinct maintained
entry document ids for safe equality filters; added PyMongo coverage for scalar
multikey count, aggregation `$match` + `$count`, update, and delete targeting,
with numeric/document fallback shapes still returning correct matcher results.
Verified with `cargo fmt -- --check`, `cargo test find`, `cargo test count`,
`cargo test aggregate_match_count`, `cargo test update`, `cargo build`,
`UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_indexes.py tests/e2e/test_metadata.py tests/e2e/test_aggregation.py tests/e2e/test_crud.py`,
and `cargo test`. Commit: `d01f7a3`.

## Milestone 3: Unique And Unsupported Multikey Semantics

Problem:

- Unique multikey semantics are easy to get wrong and must not silently accept
  unsupported behavior.

Desired behavior:

- Keep unique array indexes explicitly unsupported unless a conservative correct
  subset is implemented.
- If implemented, enforce duplicate array element conflicts within and across
  documents correctly for the supported subset.
- Return explicit errors for compound multikey, document-valued array elements,
  numeric multikey unique values if cross-type semantics are unsafe, and other
  unsupported shapes.

Acceptance criteria:

- Existing unique array rejection tests still pass or are replaced with stricter
  supported-semantics tests.
- PyMongo e2e covers unsupported unique multikey errors.
- No duplicate-key invariant regresses.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_indexes.py`
- `tests/e2e/test_update_operators.py`
- `docs/index-multikey-scalar-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test unique
cargo test index
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_indexes.py tests/e2e/test_update_operators.py
cargo test
```

Status 2026-07-04: kept unique multikey unsupported explicitly by rejecting any
unique indexed path that traverses arrays, including single-leaf dotted arrays;
mapped unsupported unique multikey write errors to code 72; added PyMongo
coverage for unique array create, compound/dotted unique multikey create,
update, upsert, and insert errors. Verified with `cargo fmt -- --check`,
`cargo test unique`, `cargo test index`, `cargo build`,
`UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_indexes.py tests/e2e/test_update_operators.py`,
and `cargo test`. Commit: `4a45f3c`.

## Milestone 4: Benchmarks, Docs, And Final Verification

Desired behavior:

- Add multikey benchmark cases.
- Update scorecard and performance docs with before/after evidence.
- Run full verification.

Acceptance criteria:

- `find_multikey_scalar_equality`, `count_multikey_scalar_equality`, and
  `update_multikey_target` benchmark cases exist.
- Local benchmark evidence meets this prompt's performance targets.
- CI budget includes thresholds for new benchmarks.
- `docs/performance-baseline.md` and `docs/index-uplifts-roadmap.md` are
  updated.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/bin/mongolino-bench.rs`
- `docs/performance-baseline.md`
- `docs/index-uplifts-roadmap.md`
- `docs/index-multikey-scalar-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test
cargo build
cargo run --bin mongolino-bench -- --profile smoke --json /tmp/mongolino-bench-index-multikey-smoke.json
cargo run --bin mongolino-bench -- --profile local --json /tmp/mongolino-bench-index-multikey-local.json
cargo run --bin mongolino-bench -- --profile ci --check-budget
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

Use unsandboxed execution for PyMongo e2e if the sandbox blocks localhost
binding.

Status 2026-07-04: added `find_multikey_scalar_equality`,
`count_multikey_scalar_equality`, and `update_multikey_target` benchmark cases
and CI budget thresholds; updated the compatibility scorecard to 78%; recorded
smoke, local, and CI benchmark evidence in the performance docs. Final local
profile multikey numbers: find `1.519 ms/op`, count `0.029 ms/op`, update
`2.042 ms/op`; find is about `20.3x` faster than collection scan. Verified with
`cargo fmt -- --check`, `cargo test`, `cargo build`,
`cargo run --bin mongolino-bench -- --profile smoke --json /tmp/mongolino-bench-index-multikey-smoke.json`,
`cargo run --bin mongolino-bench -- --profile local --json /tmp/mongolino-bench-index-multikey-local.json`,
`cargo run --bin mongolino-bench -- --profile ci --check-budget`,
`UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`,
`UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`, and
`UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e`
(`159 passed, 1 skipped`; run unsandboxed for localhost binding). Commit:
pending.

## Final Response Requirements

When complete, report:

- commits made;
- files changed;
- compatibility scorecard movement;
- supported multikey subset;
- unsupported multikey cases and explicit errors;
- benchmark before/after headline numbers;
- final verification commands and outcomes;
- known residual risks.
