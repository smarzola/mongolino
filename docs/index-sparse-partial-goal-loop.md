# Goal: Sparse And Partial Index Compatibility And Planner Uplift

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to deliver the second large index uplift in the index
compatibility and performance sequence: support practical sparse and partial
index semantics for the server's supported btree index subset. The current
implementation rejects sparse and partial index options, which blocks common
ODM/application index definitions and prevents filtered index entries from
being used for safe planner paths.

This is index uplift 2 of 3. It must move the repo-local index compatibility
scorecard in `docs/index-uplifts-roadmap.md` from the post-compound target of
**55% to at least 67%**.

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

Support sparse and partial indexes for a conservative, tested subset:

- `sparse: true` is accepted, persisted, listed, and dropped.
- `partialFilterExpression` is accepted only for supported predicate shapes.
- Unsupported sparse/partial combinations return explicit command errors.
- Index entries are maintained only for documents included by the sparse or
  partial rule.
- Unique sparse and unique partial indexes enforce duplicate-key constraints
  only among included documents.
- Planner pushdown may use sparse/partial entries only when the query filter
  safely implies index membership.
- Existing non-sparse/non-partial index behavior must not regress.

## Compatibility Target

Move the scorecard from **55% to at least 67%**:

- Index command surface and catalog behavior: improve by accepting, storing,
  listing, and dropping sparse/partial options.
- Unique enforcement semantics: improve by enforcing unique sparse/partial
  index membership correctly.
- Sparse/partial practical semantics: move from near-zero to a useful tested
  subset.
- Explicit unsupported behavior must remain complete for unsupported partial
  filter operators.

Do not claim support for full MongoDB partial filter implication, collation,
wildcard, text, geospatial, hidden, TTL, or multikey semantics in this uplift.

## Performance Target

Add targeted benchmark cases if they do not already exist after uplift 1.

Required local-profile targets:

- `find_partial_index_equality`: at least **8x faster** than collection scan
  when the query filter implies the partial predicate, and below **4 ms/op**.
- `count_partial_index_equality`: below **0.5 ms/op** for safe pushed-down
  partial indexed counts.
- `update_partial_unique_check`: below **2 ms/op** for safe non-numeric scalar
  unique conflict checks in a selective partial index.

## Current State

Relevant current behavior before this uplift:

- `parse_index_spec` accepts only `key`, `name`, `unique`, and `v`.
- PyMongo e2e expects `partialFilterExpression` to return code `72`.
- Index metadata stores only `name`, `key`, and `unique`.
- `index_entries` maintenance has no concept of index membership predicates.
- Unique index validation and write-time unique checks scan or use entries for
  supported shapes, but do not filter by sparse/partial membership.

Known constraints:

- Partial filter implication is subtle. Implement only a conservative subset
  and fall back when unsure.
- Sparse indexes must exclude documents where the indexed field is missing, but
  must include explicit `null` values unless this prompt is updated with
  evidence for a narrower supported subset.
- Partial indexes should start with exact equality and `$exists: true` shapes,
  then expand only when tests prove equivalence.
- Candidate narrowing must never exclude a document the Rust matcher would
  accept.

## Definition Of Done

The goal is complete only when:

1. `IndexSpec` and persisted metadata represent sparse and partial index
   options.
2. `listIndexes` returns supported sparse and partial fields.
3. `createIndexes` validates sparse and partial options with explicit errors
   for unsupported shapes.
4. Index entry maintenance includes only documents that belong in each sparse
   or partial index.
5. Unique sparse and unique partial index creation rejects existing duplicates
   only among included documents.
6. insert/update/upsert/delete/findAndModify preserve sparse/partial unique
   enforcement and entry freshness.
7. Planner pushdown uses sparse/partial entries only when query filters imply
   index membership.
8. Unsupported partial filter expressions and unsafe implication cases fall
   back or error explicitly as appropriate.
9. PyMongo e2e covers catalog, create/list/drop, unique sparse, unique partial,
   planner-safe partial finds/counts, and mutation freshness.
10. Benchmarks and docs record before/after numbers for sparse/partial planner
    paths.
11. The scorecard in `docs/index-uplifts-roadmap.md` is updated to at least
    67% with evidence.
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

- [x] Milestone 0: Metadata model and parser
- [x] Milestone 1: Sparse membership and unique semantics
- [x] Milestone 2: Partial filter subset and unique semantics
- [ ] Milestone 3: Planner-safe sparse/partial pushdown
- [ ] Milestone 4: Benchmarks, docs, and final verification

## Milestone 0: Metadata Model And Parser

Problem:

- Sparse and partial options are rejected and cannot be persisted or listed.

Desired behavior:

- Extend index metadata to store `sparse` and a BSON partial filter document.
- Accept `sparse: true|false`.
- Accept `partialFilterExpression` for a conservative subset and reject
  unsupported shapes with explicit errors.
- Ensure existing databases migrate safely.

Acceptance criteria:

- Rust tests cover parser acceptance and rejection.
- PyMongo e2e covers `create_index(..., sparse=True)` and
  `partialFilterExpression` list/drop roundtrip.
- Existing unsupported index tests are updated to match the new supported
  subset.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_indexes.py`
- `docs/index-sparse-partial-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test index
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_indexes.py
```

Status 2026-07-04: Complete. Ran `cargo fmt -- --check`, `cargo test index`,
and `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest
tests/e2e/test_indexes.py` (unsandboxed after sandboxed localhost bind failed).
Commit: this milestone commit.

## Milestone 1: Sparse Membership And Unique Semantics

Problem:

- Sparse indexes should include only documents with the indexed field present,
  and unique sparse indexes should enforce uniqueness only among included
  documents.

Desired behavior:

- Maintain entries for sparse indexes only when indexed fields are present.
- Unique sparse checks ignore documents outside the sparse index.
- Compound sparse indexes require every indexed field to be present before the
  document is included.

Acceptance criteria:

- Unique sparse creation allows multiple documents missing the indexed field.
- Unique sparse rejects duplicate present values.
- Insert/update/upsert/delete/findAndModify refresh membership correctly.
- Explicit `null` is treated as present.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_indexes.py`
- `tests/e2e/test_find_and_modify.py`
- `tests/e2e/test_update_operators.py`
- `docs/index-sparse-partial-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test unique
cargo test index
cargo test find_and_modify
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_indexes.py tests/e2e/test_find_and_modify.py tests/e2e/test_update_operators.py
cargo test
```

Status 2026-07-04: Complete. Ran `cargo fmt -- --check`, `cargo test unique`,
`cargo test index`, `cargo test find_and_modify`, `cargo test`, `cargo build`,
and `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest
tests/e2e/test_indexes.py tests/e2e/test_find_and_modify.py
tests/e2e/test_update_operators.py` (unsandboxed for localhost bind). Commit:
this milestone commit.

## Milestone 2: Partial Filter Subset And Unique Semantics

Problem:

- Partial indexes need membership predicates, and unique partial indexes must
  enforce constraints only for matching documents.

Desired behavior:

- Support a conservative partial filter subset: field exact equality,
  field `$eq`, field `$exists: true`, and `$and` of supported predicates.
- Reject unsupported operators and ambiguous shapes.
- Evaluate partial membership with the same matcher semantics where supported.
- Enforce unique partial indexes only among included documents.

Acceptance criteria:

- PyMongo e2e covers unique partial creation and mutation behavior.
- Rust tests cover membership evaluation and unsupported partial shapes.
- Numeric partial predicates fall back or are rejected unless cross-type
  semantics are proven.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_indexes.py`
- `tests/e2e/test_crud.py`
- `tests/e2e/test_find_and_modify.py`
- `docs/index-sparse-partial-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test partial
cargo test unique
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_indexes.py tests/e2e/test_crud.py tests/e2e/test_find_and_modify.py
cargo test
```

Status 2026-07-04: Complete. Ran `cargo fmt -- --check`, `cargo test partial`,
`cargo test unique`, `cargo test`, `cargo build`, and
`UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest
tests/e2e/test_indexes.py tests/e2e/test_crud.py
tests/e2e/test_find_and_modify.py` (unsandboxed for localhost bind). Commit:
this milestone commit.

## Milestone 3: Planner-Safe Sparse/Partial Pushdown

Problem:

- Sparse and partial entries are only safe for planning when the query filter
  implies that every matching document is included in the index.

Desired behavior:

- Use sparse entries when the query constrains the indexed fields to present
  equality values.
- Use partial entries only when the query filter includes or implies the
  supported partial predicate.
- Keep Rust matcher validation after candidate narrowing.
- Fall back for uncertain implication cases.

Acceptance criteria:

- PyMongo e2e covers sparse and partial indexed find/count cases.
- Rust tests prove fallback for unsafe implication cases.
- Existing single-field and compound planner behavior does not regress.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_indexes.py`
- `tests/e2e/test_metadata.py`
- `tests/e2e/test_aggregation.py`
- `docs/index-sparse-partial-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test planner
cargo test count
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_indexes.py tests/e2e/test_metadata.py tests/e2e/test_aggregation.py
cargo test
```

## Milestone 4: Benchmarks, Docs, And Final Verification

Desired behavior:

- Add sparse/partial benchmark cases.
- Update scorecard and performance docs with before/after evidence.
- Run full verification.

Acceptance criteria:

- `find_partial_index_equality`, `count_partial_index_equality`, and
  `update_partial_unique_check` benchmark cases exist.
- Local benchmark evidence meets this prompt's performance targets.
- CI budget includes thresholds for new benchmarks.
- `docs/performance-baseline.md` and `docs/index-uplifts-roadmap.md` are
  updated.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/bin/mongolino-bench.rs`
- `docs/performance-baseline.md`
- `docs/index-uplifts-roadmap.md`
- `docs/index-sparse-partial-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test
cargo build
cargo run --bin mongolino-bench -- --profile smoke --json /tmp/mongolino-bench-index-sparse-partial-smoke.json
cargo run --bin mongolino-bench -- --profile local --json /tmp/mongolino-bench-index-sparse-partial-local.json
cargo run --bin mongolino-bench -- --profile ci --check-budget
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

Use unsandboxed execution for PyMongo e2e if the sandbox blocks localhost
binding.

## Final Response Requirements

When complete, report:

- commits made;
- files changed;
- compatibility scorecard movement;
- supported sparse/partial subset;
- unsupported sparse/partial cases and explicit errors;
- benchmark before/after headline numbers;
- final verification commands and outcomes;
- known residual risks.
