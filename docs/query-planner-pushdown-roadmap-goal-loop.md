# Goal: Query Planner And Pushdown Uplift Roadmap

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to break down and begin delivering the remaining
query/planner/pushdown performance work after the existing count, write
targeting, compound, sparse/partial, multikey, range, hint, and conservative
sort-aware planner uplifts. Do not repeat completed work. Use the current
worktree as truth, preserve MongoDB-compatible behavior for the documented
subset, and choose atomic implementation slices that can be delegated to
workers and reviewed adversarially.

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
- Do not claim full MongoDB planner parity.
- Do not revert unrelated user or agent changes.
- You are not alone in the codebase. Other agents may have touched nearby
  files; work with current state and do not revert unrelated edits.

## Current State

Source and docs inspection on 2026-07-05 shows:

- `_id`, scalar equality, compound equality, compound prefix, safe range,
  sparse/partial, scalar multikey, hint, explain, command count,
  aggregation `$match` + `$count`, update/delete target selection,
  findAndModify target selection, and safe unique checks already have SQLite
  pushdown paths.
- `docs/performance-baseline.md` identifies the remaining slow clusters as:
  collection-scan `find`, unsupported count filters, general aggregation that
  loads full namespaces into memory, `$unwind` + `$group`, and write filters
  outside the conservative planner.
- The benchmark suite already contains regression sentinels for many find,
  count, update, and aggregation paths.
- README now documents the architecture and conservative planner safety model.

Parent inspection note 2026-07-05:

- `aggregate_command_with_state` has a special `aggregate_match_count_pushdown`
  path for exact `$match` + `$count` pipelines.
- `aggregate_pipeline_documents` otherwise starts with
  `documents_for_namespace(conn, namespace)?`, so general aggregation pipelines
  beginning with `$match` still decode the full namespace before filtering.
- `candidate_documents` and `shape_documents` already provide the right safety
  model for first-stage `$match`: load narrowed candidates when the planner can
  prove it is safe, then run the Rust matcher before downstream stages.
- Existing benchmark rows `aggregation_expression_add_fields` and
  `aggregation_lookup_single_document` are good sentinels for this next slice.

## Target State

The next planner work is organized into a sequence of atomic, reviewable
uplifts with clear correctness fences and benchmark evidence. At least the
first implementation slice should be delivered, tested, documented, and
committed by a worker before this goal is considered meaningfully underway.

The roadmap should prioritize:

1. high-confidence SQLite candidate narrowing that still runs the Rust matcher;
2. aggregation pushdown for shapes that are behaviorally equivalent;
3. benchmark coverage that catches severe regressions and records before/after
   numbers;
4. explicit fallback or command errors for unsafe MongoDB semantics.

## Candidate Uplifts

### Uplift A: Covered Find Projection For `_id` And Fully Covered Scalar Indexes

Hypothesis:

- Some `find` shapes can avoid decoding full BSON documents when the requested
  projection is fully covered by `_id` or a maintained scalar index entry.

Behavior fence:

- Only support inclusion projections over `_id` and indexed scalar fields when
  BSON type, field presence, collation, sort, skip, and limit semantics are
  proven equivalent.
- Fall back to current candidate decode plus matcher for arrays, dotted paths
  through arrays, document values, numeric cross-type equality risks,
  exclusion projections, computed projection-like shapes, and unsupported
  operators.

Potential value:

- Reduces latency for read-heavy integration tests that fetch only indexed
  lookup fields.

### Uplift B: Lookup-Side Candidate Narrowing For Simple `$lookup`

Hypothesis:

- Existing simple same-database equality `$lookup` can narrow the foreign side
  with `_id` or maintained index entries instead of scanning the foreign
  namespace for every source document.

Behavior fence:

- Preserve current `$lookup` semantics: same-database only, simple
  `from`/`localField`/`foreignField`/`as`, missing fields compare as `null`,
  local arrays match scalar foreign values by any element, unsupported
  pipeline/`let` forms remain errors.
- Continue to run the Rust matcher or an equivalent equality check on narrowed
  candidates.
- Fall back for null/missing, arrays, numeric cross-type comparisons,
  unindexed foreign fields, collation mismatches, and unsupported value shapes.

Potential value:

- This is likely the best next substantial pushdown because aggregation v2
  added `$lookup` compatibility and the benchmark suite already has a
  lookup sentinel.

### Uplift C: Aggregation `$match` Pushdown At Pipeline Start

Hypothesis:

- General aggregation pipelines whose first stage is a safe `$match` can start
  from narrowed candidates instead of loading the full namespace.

Behavior fence:

- Only push down the first `$match`; later stages keep current Rust execution.
- Candidate narrowing must still run the matcher before downstream stages.
- Preserve stage-order semantics, collation behavior, and error ordering.
- Fall back for unsupported filters and unsafe shapes.

Potential value:

- Improves `$match` + `$project`, `$match` + `$addFields`, `$match` + `$lookup`,
  and other common pipelines without implementing SQL aggregation.

### Uplift D: Bounded `$unwind`/`$group` Side-Table

Hypothesis:

- A maintained side table for selected scalar array entries might accelerate
  `$unwind` + `$group` workloads.

Behavior fence:

- Delivered on 2026-07-05 as a bounded optimized shape for simple single-field
  indexed paths: default `$unwind` followed by `$group` on the same field with
  `$sum: 1` count accumulators.
- The side table stores one row per default unwind occurrence and keeps
  duplicate array values, scalar values, null array elements, and finite
  numeric cross-type grouping aligned with the Rust executor.
- Preserve-null unwind, include-array-index, different group paths, non-count
  accumulators, non-simple command collation, unwound document/array values,
  partial index sources, and broader aggregation pipelines remain Rust
  fallbacks.

Potential value:

- `aggregation_unwind_group` now exercises the maintained side-table source in
  the smoke benchmark and is covered by Rust plus PyMongo tests.

## Definition Of Done

This roadmap goal is complete only when:

1. The current planner/pushdown state is accurately summarized from source and
   docs.
2. Remaining performance opportunities are ranked by value and semantic risk.
3. At least three atomic implementation goal prompts are either written here or
   split into separate `docs/*goal-loop.md` files.
4. The first selected implementation slice is dispatched to a worker.
5. The first slice returns with code/tests/docs, is adversarially reviewed, and
   any blocking issue is sent to a fix worker.
6. The final integrated work passes `cargo fmt -- --check`, `cargo test`, and
   relevant benchmark/e2e subsets.
7. `docs/performance-baseline.md` or a more specific performance doc records
   any benchmark commands and numbers actually run.

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

- [x] Milestone 0: Confirm current planner state and rank remaining uplifts
- [x] Milestone 1: Select and specify the first implementation slice
- [x] Milestone 2: Deliver first-stage aggregation `$match` candidate narrowing
- [x] Milestone 3: Deliver lookup-side candidate narrowing or write its
      separate executable goal prompt
- [x] Milestone 4: Benchmarks, docs, adversarial review, and fix loop

Planner roadmap closeout status 2026-07-05:

- Milestone 0 and 1 are complete. Source and benchmark inspection ranked the
  next substantial planner slices as first-stage aggregation `$match` candidate
  narrowing, lookup-side candidate narrowing, and bounded `$unwind`/`$group`
  side-table pushdown. Covered find projection remains deferred because the
  maintained planner metadata does not store enough original BSON value state
  to avoid decoding documents for general projections without adding another
  value-side metadata design.
- Milestone 2 delivered first-stage aggregation `$match` candidate narrowing
  and its adversarial fix loop. Commits: `9c38ef2`, `fc377a3`, `be70000`.
- Milestone 3 delivered lookup-side candidate narrowing and benchmark coverage.
  Commits: `d9c2715`, `04552c0`.
- Milestone 4 expanded into the bounded `$unwind`/`$group` side-table slice,
  implemented through `docs/aggregation-unwind-group-side-table-goal-loop.md`
  and fixed through
  `docs/aggregation-unwind-group-side-table-adversarial-fix-goal-loop.md`.
  Commits: `fac0186`, `3c4311a`, `c83ae22`, `0adcbe1`, `a56add6`.
- Parent verification after the final fix passed `cargo fmt -- --check`,
  `git diff --check`, `cargo test unwind_group`, `cargo test aggregate`,
  `cargo test index`, `cargo test`, `cargo build`,
  `cargo run --bin mongolino-bench -- --profile smoke --check-budget`,
  `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`,
  `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`,
  focused PyMongo aggregation e2e outside the sandbox, and full PyMongo e2e
  outside the sandbox with 226 tests passed.
- Residual planner work is intentionally deferred rather than part of this
  roadmap closeout: broad collection scans without selective predicates,
  general SQL aggregation, broad sort pushdown, covered projection without a
  value metadata design, text/geospatial/hashed/wildcard index planning, and
  non-simple collation range planning.

## Milestone 0: Confirm Current Planner State And Rank Remaining Uplifts

Problem:

- The repo has many completed planner uplifts. The next work must not duplicate
  solved count/write/index paths.

Acceptance criteria:

- Inspect `src/main.rs`, `src/bin/mongolino-bench.rs`,
  `docs/performance-baseline.md`, and relevant planner goal files.
- Record which pushdown paths already exist and which remain slow.
- Rank the next three uplifts by likely user value, performance value, and
  semantic risk.

Verification:

```bash
rg -n "aggregation_match|lookup|candidate|planner|pushdown" src/main.rs src/bin/mongolino-bench.rs docs
```

Status note 2026-07-05:

- Completed in the parent roadmap inspection. The implemented planner state is
  summarized above and in `docs/performance-baseline.md`; residual candidates
  are explicitly deferred because they require new semantics or metadata beyond
  the conservative candidate-narrowing model.

## Milestone 1: Select And Specify The First Implementation Slice

Problem:

- The best next slice needs to be substantial but still reviewable.

Desired behavior:

- Prefer first-stage aggregation `$match` candidate narrowing if source
  inspection confirms it can reuse existing candidate planners without changing
  downstream aggregation semantics.

Acceptance criteria:

- Write concrete target behavior, fallback behavior, likely files, tests, and
  benchmarks for the selected slice.
- Use first-stage `$match` candidate narrowing unless fresh source inspection
  finds it has already been implemented in the current worktree.
- If first-stage `$match` is unsafe or already implemented, choose lookup-side
  candidate narrowing instead and explain why.
- Treat `aggregation_expression_add_fields` and
  `aggregation_lookup_single_document` as the first benchmark rows to compare
  before/after because both begin with a selective `$match` and then exercise
  downstream aggregation behavior.

Verification:

```bash
cargo test aggregation
cargo run --bin mongolino-bench -- --profile smoke --check-budget
```

Status note 2026-07-05:

- Completed. First-stage aggregation `$match` candidate narrowing was selected
  and delivered first. Lookup-side candidate narrowing and bounded
  `$unwind`/`$group` side-table pushdown followed as separate executable goal
  prompts and worker/fix loops.

## Milestone 2: Deliver First-Stage Aggregation `$match` Candidate Narrowing

Problem:

- General aggregation currently loads full namespace documents before running
  most pipelines. Pipelines beginning with a safe `$match` should be able to
  reuse existing find candidate narrowing, then continue through the current
  Rust aggregation executor.

Desired behavior:

- For aggregation pipelines whose first stage is `$match` with a pushdown-safe
  filter, load only narrowed candidates, run the Rust matcher over them, then
  execute the remaining stages exactly as before.
- Preserve command-level collation, readConcern, cursor batching, getMore
  behavior, stage validation, and error ordering.
- Fall back to full namespace load for unsafe filters or unsupported shapes.

Acceptance criteria:

- Add Rust tests proving:
  - safe first-stage `_id` equality starts from narrowed candidates;
  - safe indexed equality starts from narrowed candidates;
  - unsafe filters fall back and still return correct results;
  - invalid filters/stages preserve current command errors;
  - downstream `$project`, `$addFields`, `$lookup`, `$unwind`, and `$group`
    still receive correct documents.
- Add PyMongo e2e coverage for at least one aggregation pipeline that benefits
  from first-stage narrowing.
- Add or update benchmarks for aggregation pipelines beyond `$match` + `$count`
  so the improvement can be measured.

Likely files:

- `src/main.rs`
- `src/bin/mongolino-bench.rs`
- `tests/e2e/test_aggregation.py`
- `docs/performance-baseline.md`
- this goal file

Verification:

```bash
cargo fmt -- --check
cargo test aggregation
cargo test planner
cargo run --bin mongolino-bench -- --profile smoke --check-budget
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_aggregation.py
```

Status note 2026-07-05:

- Delivered first-stage aggregation `$match` candidate narrowing in
  `src/main.rs`, including `_id` candidate support in the shared candidate
  loader. The exact `$match` plus `$count` fast path remains separate.
- Added Rust tests for `_id` narrowing, indexed narrowing, unsafe numeric
  fallback, and downstream matcher/project behavior. Added PyMongo e2e coverage
  for indexed first-stage `$match` plus `$addFields` and `_id` first-stage
  `$match` plus `$lookup`.
- Verification run:
  - `cargo fmt -- --check`: passed.
  - `cargo test aggregation`: passed, 4 tests in `src/main.rs` and 4 tests via
    the bench target.
  - `cargo test planner`: passed, 20 tests in `src/main.rs` and 20 tests via
    the bench target.
  - `cargo run --bin mongolino-bench -- --profile smoke --check-budget`:
    passed. Target rows were `aggregation_match_count` 0.088 ms/op,
    `aggregation_expression_add_fields` 2.480 ms/op, and
    `aggregation_lookup_single_document` 5.507 ms/op.
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest
    tests/e2e/test_aggregation.py`: sandbox run failed before server startup
    because `127.0.0.1:0` bind returned `PermissionError: [Errno 1] Operation
    not permitted`; the same command passed outside the sandbox with 27 tests.

Parent review fix-loop status 2026-07-05:

- Parent adversarial review of commit `9c38ef2` found two missing regression
  tests and two stale status/documentation notes, tracked in
  `docs/aggregation-match-pushdown-adversarial-fix-goal-loop.md`.
- Fix loop scope is intentionally tests/docs only: pin later-stage aggregation
  command-error ordering after a safe first `$match`, pin non-simple collation
  string `_id` fallback for first-stage aggregation `$match`, clarify the
  benchmark note, and preserve the existing planner semantics.

## Milestone 3: Deliver Lookup-Side Candidate Narrowing Or Split Prompt

Problem:

- Simple `$lookup` can be expensive when the foreign side is scanned repeatedly.

Desired behavior:

- Either deliver lookup-side candidate narrowing directly if it is isolated
  enough after Milestone 2, or write a separate executable goal-loop prompt for
  a worker to implement it.

Acceptance criteria:

- If implementing:
  - use `_id` or maintained scalar index entries on the foreign collection;
  - preserve missing/null, local array, collation, and unsupported-form
    behavior;
  - add Rust and PyMongo coverage;
  - benchmark the lookup sentinel.
- If splitting:
  - create `docs/aggregation-lookup-pushdown-goal-loop.md` with full target
    state, current state, milestones, tests, benchmark commands, and checkpoint
    protocol.

Verification:

```bash
cargo fmt -- --check
cargo test lookup
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_aggregation.py
```

Status note 2026-07-05:

- Delivered lookup-side candidate narrowing directly via
  `docs/aggregation-lookup-pushdown-goal-loop.md` instead of only splitting a
  prompt. Simple `$lookup` now attempts safe foreign candidate loading for
  `_id` and compatible maintained single-field scalar indexes, then still runs
  Rust lookup equality before returning matches.
- Preserved full-scan fallback for null/missing/empty-array local values, local
  arrays or array traversal, numeric local values, non-simple collation string
  `_id`, unindexed fields, incompatible index collation, partial/sparse
  membership not implied by the lookup value, and unsafe multikey omissions.
- Added Rust and PyMongo lookup coverage plus benchmark row
  `aggregation_lookup_indexed_foreign_equality`. Smoke budget passed with the
  new row at 0.447 ms/op on 400 documents.
- Verification run:
  - `cargo fmt -- --check`: passed.
  - `cargo test lookup`: passed.
  - `cargo test aggregation`: passed.
  - `cargo run --bin mongolino-bench -- --profile smoke --check-budget`:
    passed.
  - `cargo test`: passed.
  - `cargo build`: passed with existing dead-code warnings for planner helper
    functions that are only used by tests or benchmark-target compilation.
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest
    tests/e2e/test_aggregation.py`: sandbox run hit localhost bind
    `PermissionError`; rerun outside the sandbox passed with 28 tests.
- Parent adversarial review and fix-loop status 2026-07-05: lookup-side
  candidate narrowing received adversarial regression coverage through
  `docs/aggregation-match-pushdown-adversarial-fix-goal-loop.md` and the later
  full planner verification runs. No remaining blocking lookup issue is open in
  this roadmap.

## Milestone 4: Benchmarks, Docs, Review, And Fix Loop

Problem:

- Planner work is risky because fast wrong answers are worse than slow correct
  answers.

Acceptance criteria:

- Run and record at least the smoke benchmark profile.
- Run relevant Rust and PyMongo tests.
- Update docs with measured numbers only when actually run.
- Parent review must explicitly look for:
  - dropped documents due to unsafe narrowing;
  - changed command-error ordering;
  - changed collation behavior;
  - stale index entries after writes;
  - cursor/getMore regressions;
  - benchmark gaming that bypasses real command handlers.
- Blocking findings must become a fix prompt for a worker.

Verification:

```bash
cargo fmt -- --check
cargo test
cargo build
cargo run --bin mongolino-bench -- --profile ci --check-budget
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_aggregation.py
```

Status note 2026-07-05:

- Completed. Parent review found a real adversarial gap in the
  `$unwind`/`$group` side-table implementation for existing databases with
  indexes but empty new side tables. The fix loop added an idempotent
  `unwind_group_side_table_backfill_v1` initialization migration, deterministic
  optimized ordering aligned with executor scan order, and regression tests for
  legacy backfill, idempotence, unsupported-value fallback, and order
  alignment. Final parent verification is recorded in the closeout status
  above.

## Final Response Requirements

Report:

- chosen ranking and rationale;
- files changed;
- commits made, if any;
- exact verification commands and results;
- benchmark commands and numbers actually run;
- any residual planner risks or intentionally deferred pushdown shapes.
