# Goal: Aggregation Lookup-Side Candidate Narrowing

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to deliver the next substantial query/planner/pushdown uplift:
make simple aggregation `$lookup` use SQLite-backed candidate narrowing on the
foreign collection when that is behaviorally equivalent to the documented
MongoDB subset. The current `$lookup` implementation loads the entire foreign
namespace once per stage and then compares every foreign document against every
input document in Rust. That is correct but expensive for selective local values
when the foreign side has `_id` or maintained index entries.

This is a performance uplift, not a compatibility expansion. Preserve the
existing accepted `$lookup` surface exactly: same-database simple equality
joins using `from`, `localField`, `foreignField`, and `as`; no pipeline/`let`;
missing local or foreign fields compare as `null`; local arrays match scalar
foreign values by any element; unsupported forms remain explicit command
errors.

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
- Do not optimize by weakening matcher or `$lookup` semantics.
- Do not claim full MongoDB aggregation or planner parity.
- Do not revert unrelated user or agent changes.
- You are not alone in the codebase. Other agents may have touched nearby
  files; work with current state and do not revert unrelated edits.

## Current State

Source inspection on 2026-07-05 shows:

- `aggregate_pipeline_documents` now starts general aggregation pipelines from
  narrowed candidates when the first stage is a safe `$match`.
- `apply_aggregate_lookup_stage` still does:
  - `sweep_ttl_namespace(conn, &foreign_namespace)?`;
  - `documents_for_namespace(conn, &foreign_namespace)?`;
  - nested Rust comparison of every input document against every foreign
    document.
- `lookup_values_at_path` implements current local/foreign value extraction:
  missing paths become `[null]`, empty arrays become `[null]`, and arrays expand
  to their values.
- `lookup_values_match` uses `collation.values_equal`.
- `candidate_documents`, `indexed_candidate_documents_with_collation`,
  `stored_document_by_id_key`, planner key helpers, collation checks, and
  maintained `index_entries` already exist for safe candidate narrowing.
- Benchmarks already include `aggregation_lookup_single_document`, which now
  benefits from first-stage `_id` narrowing on the source side but still scans
  the foreign side for the join.

Likely files:

- `src/main.rs`
- `src/bin/mongolino-bench.rs`
- `tests/e2e/test_aggregation.py`
- `docs/performance-baseline.md`
- `docs/query-planner-pushdown-roadmap-goal-loop.md`
- this file

## Target State

For each input document in a simple `$lookup`, `mongolino` should attempt to
load only safe foreign candidates when possible:

- If `foreignField` is `_id` and every relevant local value is safe under the
  command collation, load foreign documents by `(namespace, id_key)`.
- If `foreignField` has a maintained compatible scalar index, use
  `index_entries` for safe local scalar values.
- If no safe plan exists for the local values or foreign field, fall back to the
  existing full-foreign-namespace scan for that lookup stage or document.
- Even after candidate narrowing, run the existing `$lookup` equality semantics
  before adding a foreign document to the result.

The result array must keep the same deterministic foreign document order as the
current implementation for all tested shapes.

## Semantic Fences

Do not narrow when doing so could drop a document that the current Rust
comparison would include. Fall back for:

- local values that include `null` from missing paths or empty arrays;
- non-simple collation string `_id` equality;
- local numeric values, because MongoDB numeric equality compares across
  `Int32`, `Int64`, and `Double` while `index_entries` keys are type-tagged;
- local arrays or value shapes that are not supported scalar planner keys;
- unindexed foreign fields;
- incompatible index collation;
- foreign-field paths with unsupported multikey omission sentinels;
- document-valued or array-valued join values unless current helper semantics
  can prove exact candidate completeness.

Preserve:

- foreign TTL sweep timing;
- `as` path collision errors;
- pipeline validation and malformed `$lookup` errors before TTL sweep for
  malformed lookup specs;
- cursor batching and `getMore` behavior after the full pipeline;
- first-stage `$match` narrowing behavior added by the previous uplift.

## Definition Of Done

The goal is complete only when:

1. `$lookup` foreign-side candidate narrowing supports safe `_id` local scalar
   values under simple collation.
2. `$lookup` foreign-side candidate narrowing supports safe indexed scalar
   foreign fields with compatible collation.
3. Candidate narrowing still runs the existing `lookup_values_match` or an
   equivalent Rust equality check before returning matches.
4. Candidate narrowing preserves foreign result ordering and avoids duplicates
   when multiple local values point at the same foreign document.
5. Missing/null/empty-array lookup semantics remain unchanged.
6. Local array lookup semantics remain unchanged.
7. Non-simple collation string `_id` lookup falls back safely and returns all
   collation-equal foreign documents.
8. Numeric lookup values fall back safely and preserve cross-type numeric
   equality.
9. Unsupported/malformed `$lookup` forms still return command errors before TTL
   sweeps.
10. Foreign TTL sweep behavior is preserved.
11. Benchmarks include a lookup row that isolates foreign-side narrowing enough
    to measure the improvement.
12. Docs record benchmark numbers only if the commands were actually run.
13. `docs/query-planner-pushdown-roadmap-goal-loop.md` is updated with status.
14. `cargo fmt -- --check`, `cargo test lookup`, `cargo test aggregation`,
    `cargo run --bin mongolino-bench -- --profile smoke --check-budget`, and
    `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest
    tests/e2e/test_aggregation.py` pass, using unsandboxed execution for
    localhost e2e if needed.

## Checkpoint Protocol

When a milestone is complete:

1. Run the milestone's verification commands.
2. Update this file by changing the milestone checkbox from `[ ]` to `[x]`.
3. Add a short status note with the date, exact commands run, and commit hash
   if available.
4. Commit the code, tests, docs, and status-note update with a focused commit
   message.
5. Report the commit hash in the goal-loop status before starting the next
   milestone.

## Milestone Checklist

- [x] Milestone 0: Planner design and adversarial tests
- [x] Milestone 1: Implement safe `_id` and indexed foreign candidate loading
- [x] Milestone 2: Preserve fallback semantics and lookup ordering
- [x] Milestone 3: Benchmarks, docs, and final verification

Status note 2026-07-05:

- Delivered lookup-side candidate narrowing in `src/main.rs` for safe simple
  `_id` local scalars and compatible maintained single-field scalar index
  entries. Narrowed candidates are still checked with `lookup_values_match`
  before being appended.
- Preserved fallback to the existing full foreign namespace scan for local
  null/missing/empty-array values, local arrays or array traversal, numeric
  local values, non-simple string `_id` collation, unindexed foreign fields,
  incompatible index collation, partial/sparse membership that is not implied,
  and unsafe multikey omission sentinels.
- Added Rust planner classification and aggregate behavior tests plus PyMongo
  e2e coverage for indexed foreign lookup, `_id` lookup fallbacks, numeric
  cross-type equality, non-simple collation string `_id`, duplicate local array
  behavior, malformed lookup error ordering, and TTL sweep behavior.
- Added benchmark row `aggregation_lookup_indexed_foreign_equality` and
  recorded smoke numbers in `docs/performance-baseline.md`.
- Verification run:
  - `cargo fmt -- --check`: passed.
  - `cargo test lookup`: passed, 6 tests in `src/main.rs` and 6 tests via the
    bench target.
  - `cargo test aggregation`: passed, 6 tests in `src/main.rs` and 6 tests via
    the bench target.
  - `cargo run --bin mongolino-bench -- --profile smoke --check-budget`:
    passed; `aggregation_lookup_indexed_foreign_equality` measured 0.447 ms/op
    on 400 documents.
  - `cargo test`: passed, 202 tests in `src/main.rs` and 204 tests via the
    bench target.
  - `cargo build`: passed with existing dead-code warnings for planner helper
    functions that are only used by tests or benchmark-target compilation.
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest
    tests/e2e/test_aggregation.py`: sandbox run failed before server startup
    because `127.0.0.1:0` bind returned `PermissionError: [Errno 1] Operation
    not permitted`; the same command passed outside the sandbox with 28 tests.
- Commit hash: `d9c2715`.

## Milestone 0: Planner Design And Adversarial Tests

Problem:

- `$lookup` equality has tricky MongoDB-like behavior for missing values,
  arrays, numeric equality, collation, and result ordering. A fast wrong join is
  worse than a full scan.

Desired behavior:

- Add or identify a small lookup-side candidate planner that classifies each
  local value set as narrowable or fallback.

Acceptance criteria:

- Tests cover the intended planner classifications:
  - `_id` under simple collation is narrowable for safe scalar values;
  - indexed scalar foreign fields are narrowable for safe scalar local values;
  - null/missing/empty arrays fall back;
  - numeric local values fall back;
  - non-simple collation string `_id` falls back;
  - unindexed foreign fields fall back;
  - local arrays either narrow only when every element is safe and duplicate
    handling is proven, or fall back.
- Existing `$lookup` tests still pass.

Verification:

```bash
cargo fmt -- --check
cargo test lookup
```

## Milestone 1: Implement Safe `_id` And Indexed Foreign Candidate Loading

Problem:

- `apply_aggregate_lookup_stage` always scans all foreign documents.

Desired behavior:

- Add a helper that loads foreign candidates for one input document:
  - computes local values with `lookup_values_at_path`;
  - tries `_id` lookup or compatible maintained index entries for safe values;
  - deduplicates candidate documents by stable `_id`/`id_key`;
  - falls back to the provided full foreign document set when any value or
    planner condition is unsafe.

Acceptance criteria:

- The helper is small, locally testable, and does not change the accepted
  `$lookup` command surface.
- Candidate narrowing still calls `lookup_values_match` before appending a
  foreign document.
- Foreign TTL sweep still runs once before candidate loading/scan.
- Candidate result ordering matches the existing `documents_for_namespace`
  order for supported shapes.

Likely files:

- `src/main.rs`

Verification:

```bash
cargo fmt -- --check
cargo test lookup
cargo test aggregation
```

## Milestone 2: Preserve Fallback Semantics And Lookup Ordering

Problem:

- The optimization must not alter behavior for null/missing, arrays, numeric
  values, non-simple collation, or malformed lookup specs.

Desired behavior:

- Expand Rust and PyMongo tests for the dangerous cases.

Acceptance criteria:

- Tests prove missing local values still match missing/null foreign values.
- Tests prove local arrays still match any scalar foreign value.
- Tests prove numeric local values still match numeric foreign values across
  BSON numeric types.
- Tests prove non-simple string collation lookup returns all case-insensitive
  foreign matches and does not use binary `_id` lookup unsafely.
- Tests prove malformed lookup specs still preserve TTL and command-error
  ordering.
- Tests prove duplicate local values do not duplicate a foreign document in the
  output array unless the previous implementation did.

Likely files:

- `src/main.rs`
- `tests/e2e/test_aggregation.py`

Verification:

```bash
cargo fmt -- --check
cargo test lookup
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_aggregation.py
```

## Milestone 3: Benchmarks, Docs, And Final Verification

Problem:

- Existing `aggregation_lookup_single_document` is useful but includes
  first-stage source narrowing and a broad same-team join. Add or refine a row
  that makes the foreign-side narrowing benefit observable.

Desired behavior:

- Add a benchmark row such as `aggregation_lookup_indexed_foreign_equality`
  where a selective source document joins against a selective indexed foreign
  field, and record smoke or local results that were actually run.

Acceptance criteria:

- Benchmark uses real `mongolino` command handlers.
- Benchmark does not bypass `$lookup`.
- Performance docs record only actually measured rows.
- `docs/query-planner-pushdown-roadmap-goal-loop.md` records the delivered
  lookup-side status and residual fallbacks.

Verification:

```bash
cargo fmt -- --check
cargo test
cargo build
cargo run --bin mongolino-bench -- --profile smoke --check-budget
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_aggregation.py
```

## Final Response Requirements

Report:

- files changed;
- commits made;
- exact verification commands and results;
- benchmark commands and numbers actually run;
- lookup shapes now pushed down;
- intentionally preserved fallbacks and residual risks.
