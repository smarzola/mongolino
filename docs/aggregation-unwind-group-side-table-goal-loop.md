# Goal: Aggregation `$unwind`/`$group` Side-Table Pushdown

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to deliver the next substantial query/planner/pushdown
uplift: accelerate the common pipeline shape that unwinds a scalar field and
groups by that same field, without weakening the existing MongoDB-compatible
aggregation subset.

The target workload is represented by the existing benchmark row:

```javascript
[
  { "$unwind": "$tags" },
  { "$group": { "_id": "$tags", "n": { "$sum": 1 } } }
]
```

This is a correctness-first performance uplift. The current Rust executor is
the semantic source of truth. Use SQLite only when a side table or equivalent
metadata can prove it will produce the same output as the existing `$unwind`
then `$group` stages for the supported subset.

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
- Do not optimize by weakening `$unwind`, `$group`, matcher, collation, cursor,
  or update semantics.
- Do not claim full MongoDB aggregation or planner parity.
- Do not revert unrelated user or agent changes.
- You are not alone in the codebase. Other agents may have touched nearby
  files; work with current state and do not revert unrelated edits.

## Current State

Source inspection on 2026-07-05 shows:

- `aggregate_pipeline_documents` executes aggregation stages sequentially in
  Rust after optional first-stage `$match` candidate narrowing.
- `apply_unwind_stage` currently implements the accepted `$unwind` behavior:
  arrays emit one document per element in order, duplicate values are preserved,
  scalar non-null values emit one document, and missing/null/empty arrays are
  dropped unless `preserveNullAndEmptyArrays` is requested.
- `apply_group_stage` supports field-path group keys and accumulators including
  `$sum`.
- `index_entries` is not sufficient for this goal because it deduplicates by
  `(namespace, index_name, key_value, id_key)`. It cannot count duplicate
  occurrences such as `{ tags: ["a", "a"] }` correctly.
- `index_multikey_omissions` tracks planner safety for some index paths but
  does not encode `$unwind` occurrence semantics.
- Index lifecycle hooks already exist in and around:
  `insert_index_entry_for_document_tx`, `refresh_index_entries_for_document_tx`,
  `delete_index_entries_for_document_tx`, `rebuild_index_entries_tx`,
  `dropIndexes`, collection drop, database drop, update, delete, TTL cleanup,
  and findAndModify paths.
- The benchmark suite already includes `aggregation_unwind_group`, currently
  running against the main `users` collection's `tags` array field.

Likely files:

- `src/main.rs`
- `src/bin/mongolino-bench.rs`
- `tests/e2e/test_aggregation.py`
- `docs/performance-baseline.md`
- `docs/query-planner-pushdown-roadmap-goal-loop.md`
- this file

## Target State

`mongolino` should maintain enough SQLite-backed occurrence metadata to answer
a narrow, behaviorally equivalent `$unwind` + `$group` count pipeline without
decoding and unwinding every source document.

The optimized shape is intentionally bounded:

- exactly two stages after any already-supported first-stage `$match`
  narrowing is considered only if correctness can be proven;
- first optimized stage is `$unwind` in string form or document form with only
  `path`;
- no `preserveNullAndEmptyArrays`;
- no `includeArrayIndex`;
- next stage is `$group`;
- group `_id` is the same field path as the unwind path;
- accumulators are one or more `$sum: 1` numeric constant count fields;
- command collation is simple, unless the implementation explicitly proves and
  tests collation-equivalent grouping;
- output values use the same BSON values that the Rust executor would have
  produced as group `_id` values.

The implementation may use a new SQLite side table such as
`unwind_group_entries`, or another equivalent design, but it must preserve one
row per emitted `$unwind` occurrence, not one row per distinct value per
document. The design must support duplicate array values and scalar field
values.

## Semantic Fences

Fall back to the existing Rust executor unless every part of the pipeline shape
is proven safe.

Do not optimize when:

- `$unwind` uses `preserveNullAndEmptyArrays`;
- `$unwind` uses `includeArrayIndex`;
- `$unwind` path and `$group._id` path differ;
- `$group` includes unsupported accumulators or accumulator expressions other
  than `$sum: 1`;
- the pipeline has later stages whose ordering or error behavior would be
  changed by the fast path;
- command collation could change string grouping and is not explicitly handled;
- documents include unsupported value shapes for the side table;
- the side table has any omission/safety marker for the target namespace/path;
- there is a preceding stage other than a first `$match` shape whose current
  candidate-narrowing behavior has already been validated;
- an optimization would alter command error ordering, TTL sweep behavior,
  cursor batching, or `getMore`.

Preserve current `$unwind` behavior:

- duplicate array elements count as duplicate emitted documents;
- scalar present values count once;
- missing fields, `null`, and empty arrays contribute no group rows by default;
- array element order affects `$first`, `$last`, and `$push` in the general
  executor, so the fast path must not claim support for those accumulators;
- malformed stages return the same explicit command errors as before.

## Definition Of Done

The goal is complete only when:

1. A SQLite-backed occurrence representation exists for optimized unwind/group
   fields, or an equivalent design is implemented and documented in code.
2. The occurrence representation stores one row per emitted default `$unwind`
   occurrence for supported values, including duplicate array values.
3. Scalar present values are represented and counted once.
4. Missing fields, `null`, and empty arrays are represented in a way that keeps
   the default optimized count at zero for those documents.
5. Unsupported values or paths create a conservative safety signal that forces
   fallback instead of silently dropping rows.
6. Inserts, replacements, modifier updates, pipeline updates if supported,
   deletes, findAndModify updates/deletes, TTL deletes, dropIndexes, collection
   drop, and database drop keep the occurrence representation fresh.
7. Creating or rebuilding a relevant index or side-table source rebuilds
   occurrence metadata from stored documents.
8. The aggregate executor detects only the safe `$unwind` + `$group` count
   shape and returns the same documents as the Rust executor for that subset.
9. The optimized path preserves result BSON values for `_id`; it must not
   return internal key strings such as `str:rust`.
10. Duplicate values in a single array are counted separately.
11. Scalar and array values for the same field group together exactly as the
    current executor groups them.
12. Null, missing, and empty-array default `$unwind` behavior remains unchanged.
13. Non-simple collation either falls back or is explicitly supported with
    tests showing matching grouping semantics.
14. Pipelines with `preserveNullAndEmptyArrays`, `includeArrayIndex`, different
    group paths, non-count accumulators, extra unsupported stages, malformed
    stages, and unsupported expressions fall back or error exactly as before.
15. Cursor batching and `getMore` work for optimized results.
16. The benchmark suite isolates and measures the optimized path, either by
    updating `aggregation_unwind_group` to use a maintained optimized source or
    by adding a dedicated row with clear naming.
17. `docs/performance-baseline.md` records benchmark numbers only if commands
    were actually run.
18. `docs/query-planner-pushdown-roadmap-goal-loop.md` is updated with the
    selected slice status and residual risks.
19. PyMongo e2e coverage includes happy paths, not-happy paths, and adversarial
    paths for the optimized shape and fallback shapes.
20. `cargo fmt -- --check`, `cargo test aggregate`, `cargo test index`,
    `cargo test`, `cargo run --bin mongolino-bench -- --profile smoke
    --check-budget`, and `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run
    --locked pytest tests/e2e/test_aggregation.py` pass, using unsandboxed
    execution for localhost e2e if needed.

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

- [ ] Milestone 0: Design the occurrence metadata and safety model
- [ ] Milestone 1: Maintain occurrence metadata through write/index lifecycle
- [ ] Milestone 2: Add the safe aggregation fast path
- [ ] Milestone 3: Add adversarial Rust and PyMongo coverage
- [ ] Milestone 4: Benchmark, document, verify, and record residual risks

## Milestone 0: Design The Occurrence Metadata And Safety Model

Problem:

- Existing `index_entries` intentionally deduplicates multikey values per
  document, which is correct for many candidate-selection paths but wrong for
  `$unwind` counts.

Desired behavior:

- Introduce a small internal design that models default `$unwind` emissions for
  one scalar field path.

Acceptance criteria:

- Pick the side-table schema or equivalent representation.
- The representation can distinguish duplicate occurrences from one document.
- The representation can reconstruct or decode the BSON group `_id` value.
- The representation has an omission/safety mechanism for unsupported values,
  unsupported paths, or unsupported collations.
- The representation is scoped enough to avoid speculative planner bloat. Prefer
  maintaining entries only for single-field indexed paths or another clearly
  bounded source of optimized fields.
- Add focused unit tests for metadata extraction from documents:
  - array values with duplicates;
  - scalar values;
  - missing field;
  - `null`;
  - empty array;
  - nested path if supported, or explicit fallback if not;
  - unsupported value shapes.
- Mark this milestone done in this file and commit it.

Likely files:

- `src/main.rs`
- this file

Verification:

```bash
cargo fmt -- --check
cargo test unwind
cargo test aggregate
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 1: Maintain Occurrence Metadata Through Write/Index Lifecycle

Problem:

- A side table is useful only if it stays fresh after every mutation path that
  can alter the indexed/unwound field.

Desired behavior:

- Hook occurrence metadata maintenance into existing index/document lifecycle
  points without duplicating broad write logic.

Acceptance criteria:

- Inserts populate occurrence rows for relevant fields.
- Replacement and modifier updates refresh rows for changed documents.
- findAndModify update/delete paths refresh or delete rows.
- Delete and TTL delete paths remove rows for deleted documents.
- `dropIndexes` removes occurrence rows for the dropped source.
- collection drop and database drop clean occurrence rows.
- Rebuilding a relevant source rebuilds occurrence rows from stored documents.
- Tests prove freshness after:
  - insert;
  - `$set` from array to scalar;
  - `$push` that creates duplicate values;
  - `$pull` or `$unset` that removes values;
  - replacement;
  - delete;
  - findAndModify;
  - TTL cleanup if the existing helper makes that practical;
  - dropIndexes/dropDatabase cleanup.
- Mark this milestone done in this file and commit it.

Likely files:

- `src/main.rs`
- this file

Verification:

```bash
cargo fmt -- --check
cargo test index
cargo test update
cargo test ttl
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 2: Add The Safe Aggregation Fast Path

Problem:

- The executor currently decodes all candidate documents and performs `$unwind`
  plus `$group` in Rust even for a simple count-by-unwound-field pipeline.

Desired behavior:

- Detect the bounded safe shape and answer it from occurrence metadata while
  preserving the existing executor as fallback.

Acceptance criteria:

- Add an internal planner/detector for the safe shape.
- The detector validates `$unwind` path, `$group._id`, accumulator shape,
  collation, stage ordering, and safety status.
- Optimized results contain normal BSON `_id` values and count fields with the
  same integer type as the current executor uses for `$sum: 1`.
- Multiple `$sum: 1` count fields, if accepted by the detector, all return the
  same count. Otherwise, fall back.
- Output order is deterministic and covered by tests. If the fast path cannot
  preserve current order, add an explicit sort in SQL or fall back.
- Cursor batching and `getMore` use the existing aggregate cursor machinery.
- Unsafe shapes fall back to the current Rust executor, not a partial fast path.
- Mark this milestone done in this file and commit it.

Likely files:

- `src/main.rs`
- this file

Verification:

```bash
cargo fmt -- --check
cargo test aggregate
cargo test unwind
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 3: Add Adversarial Rust And PyMongo Coverage

Problem:

- This optimization can pass happy paths while being wrong for duplicate
  elements, scalar values, null/missing behavior, lifecycle freshness, and
  fallback semantics.

Desired behavior:

- Add tests that prove the fast path is behaviorally equivalent for the safe
  subset and invisible for unsafe shapes.

Acceptance criteria:

- Rust tests cover:
  - duplicate array values count separately;
  - scalar values count once and group with matching array values;
  - missing/null/empty arrays do not contribute under default unwind;
  - update/delete/drop lifecycle freshness;
  - fallback for `preserveNullAndEmptyArrays`;
  - fallback for `includeArrayIndex`;
  - fallback for different group path;
  - fallback for non-count accumulator;
  - fallback for non-simple collation unless explicitly supported;
  - unsupported value safety marker;
  - malformed stages still return existing command errors.
- PyMongo e2e tests cover:
  - indexed happy path;
  - duplicate array value count;
  - scalar plus array grouping;
  - fallback shape with `preserveNullAndEmptyArrays`;
  - cursor batching with optimized results.
- If practical, add a debug/test-only assertion helper that proves the optimized
  path was used for at least one Rust test or benchmark setup.
- Mark this milestone done in this file and commit it.

Likely files:

- `src/main.rs`
- `tests/e2e/test_aggregation.py`
- this file

Verification:

```bash
cargo fmt -- --check
cargo test aggregate
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_aggregation.py
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 4: Benchmark, Document, Verify, And Record Residual Risks

Problem:

- A performance uplift needs measured evidence and honest documentation of the
  boundary.

Desired behavior:

- Benchmark the optimized shape, update docs with measured numbers only, and
  finish with full verification.

Acceptance criteria:

- The benchmark suite contains a row that clearly exercises the optimized path.
- Run the smoke benchmark with `--check-budget`; run the `ci` profile if time
  permits.
- Update `docs/performance-baseline.md` with exact commands and numbers that
  were actually run.
- Update `docs/query-planner-pushdown-roadmap-goal-loop.md` with the status of
  this slice and remaining planner candidates.
- Do not overstate support. Document that this is a bounded optimized
  `$unwind` + `$group` count shape, not general SQL aggregation.
- Final verification passes:
  - `cargo fmt -- --check`
  - `cargo test aggregate`
  - `cargo test index`
  - `cargo test`
  - `cargo build`
  - `cargo run --bin mongolino-bench -- --profile smoke --check-budget`
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest
    tests/e2e/test_aggregation.py`
- Mark this milestone done in this file and commit it.

Likely files:

- `src/bin/mongolino-bench.rs`
- `docs/performance-baseline.md`
- `docs/query-planner-pushdown-roadmap-goal-loop.md`
- this file

Verification:

```bash
cargo fmt -- --check
cargo test aggregate
cargo test index
cargo test
cargo build
cargo run --bin mongolino-bench -- --profile smoke --check-budget
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_aggregation.py
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Final Response Requirements

Report:

- milestones completed and commit hashes;
- files changed;
- the exact verification commands run and whether they passed;
- benchmark command output summary, including profile and measured row values;
- any fallback shapes intentionally left unoptimized;
- any residual risks or follow-up goal prompts needed.
