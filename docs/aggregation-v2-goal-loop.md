# Goal: Aggregation v2 Compatibility Uplift

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to deliver the next large MongoDB compatibility uplift after
collation: make aggregation useful for common application pipelines by adding a
bounded expression engine, computed document-shaping stages, and a simple
same-database equality `$lookup` subset.

This is uplift 5 of the seven-uplift MongoDB compatibility sequence. It should
move the repo-local scorecard from **72%** to at least **77%** by raising
aggregation compatibility from **10%** to at least **15%**, without weakening
explicit errors for unsupported aggregation features.

## Repository Rules

Follow `AGENTS.md`.

Important reminders:

- Keep the server implementation in Rust and the durable storage layer in
  SQLite.
- Treat MongoDB wire compatibility as observable behavior: validate changes
  with real client handshakes when possible.
- Run `cargo fmt` and `cargo test` before handing off code changes.
- Keep unsupported MongoDB commands explicit by returning command errors instead
  of silently accepting behavior.
- Use the existing PyMongo e2e suite for real driver verification.
- Use `uv` for Python tooling.
- Do not use Docker or external MongoDB services for this goal.
- Do not revert unrelated user or agent changes.
- You are not alone in the codebase. Other agents may have touched nearby
  files; work with current state and do not revert unrelated edits.
- At the end of each milestone, run verification, mark the milestone done in
  this file, commit the completed milestone, and report the commit hash before
  continuing.

## Target State

By the end, `mongolino` should support aggregation pipelines that can:

- evaluate a practical expression subset across aggregation stages;
- compute fields with `$project`, `$addFields`, and `$set`;
- remove fields with `$unset`;
- replace the root document with `$replaceRoot` and `$replaceWith`;
- join another collection in the same database with a simple equality `$lookup`;
- compose the new stages with existing `$match`, `$sort`, `$skip`, `$limit`,
  `$project`, `$count`, `$unwind`, and `$group`;
- use the supported collation subset where equality semantics are observable in
  `$match`, `$sort`, and simple `$lookup`;
- preserve aggregate cursor batching and `getMore`;
- reject unsupported stages, expression operators, malformed paths, unsafe path
  collisions, and unsupported `$lookup` pipeline/`let` forms explicitly.

This goal must remain honest and bounded:

- No `$lookup` pipeline form.
- No cross-database lookup.
- No `$graphLookup`, `$facet`, `$bucket`, `$sortByCount`, `$out`, `$merge`,
  `$geoNear`, `$redact`, window stages, or server-side JavaScript.
- No general variables beyond the documented small subset.
- No `allowDiskUse`, aggregate `hint`, aggregate `explain`, `maxTimeMS`, `let`,
  read concern, or write concern.
- No attempt to perfectly match every MongoDB numeric promotion or string
  conversion edge. Use deterministic, documented semantics and cover them with
  tests.

## Current State

The repo currently has:

- `aggregate` command dispatch with per-client cursor support.
- Sequential `$match`, `$sort`, `$skip`, `$limit`, find-style `$project`,
  `$count`, `$unwind`, and `$group`.
- `$group` support for bounded `_id` expressions and accumulators `$sum`,
  `$avg`, `$min`, `$max`, `$first`, `$last`, `$push`, and `$addToSet`.
- A small `AggregationExpression` that supports field paths, scalar literals,
  and document key specs only.
- Supported collation threaded through aggregate `$match`, `$sort`, and
  `$count`.
- PyMongo e2e and local spec-corpus coverage for the current aggregation subset.

Important gaps:

- `$project` is still find-style inclusion/exclusion only; it cannot compute
  aliases or expression fields.
- `$addFields`, `$set`, `$unset`, `$replaceRoot`, `$replaceWith`, and `$lookup`
  are unsupported.
- The expression parser rejects arrays and all operator documents except `$group`
  accumulator documents.
- There is no reusable expression context for `$$ROOT`, `$$CURRENT`, or
  stage-specific expression evaluation.
- README and the roadmap still describe these aggregation features as missing.

## Definition Of Done

The goal is complete only when:

1. A reusable aggregation expression evaluator exists for the supported subset.
2. Supported expression operands include field paths, scalar literals, document
   expressions, array expressions where explicitly allowed, `$$ROOT`, and
   `$$CURRENT`.
3. Supported expression operators include at least `$literal`, `$ifNull`,
   `$concat`, `$toString`, `$toLower`, `$toUpper`, `$eq`, `$ne`, `$gt`, `$gte`,
   `$lt`, `$lte`, `$and`, `$or`, `$not`, `$cond`, `$add`, `$subtract`,
   `$multiply`, and `$divide`.
4. Unsupported expression operators, malformed operator arity, unsupported
   operand types, invalid field paths, invalid variables, and division by zero
   return explicit command errors.
5. Existing `$group` behavior continues to work, and accumulator operands can
   use the new expression subset where safe for each accumulator.
6. `$project` continues to support existing inclusion/exclusion behavior,
   including `_id` handling.
7. `$project` supports computed fields and aliases using the expression subset.
8. `$project` rejects unsafe mixes and path collisions explicitly.
9. `$addFields` and `$set` add or overwrite top-level and dotted-path fields
   using supported expressions.
10. `$unset` supports string and array-of-string forms and removes top-level or
    dotted-path fields.
11. `$replaceRoot` and `$replaceWith` replace the output document with a
    document-valued expression and reject non-document results explicitly.
12. `$lookup` supports the simple equality form:
    `{ from, localField, foreignField, as }`.
13. `$lookup` reads from another collection in the same database and returns an
    array under `as` while preserving the input document order.
14. `$lookup` supports scalar equality, `null`/missing equality behavior
    documented by tests, local array matching against scalar foreign values, and
    the supported collation subset for string equality.
15. `$lookup` rejects pipeline form, `let`, cross-database namespaces,
    malformed collection/field/as names, unsupported options, and path
    collisions explicitly.
16. Aggregate cursor batching and `getMore` continue to work with the new
    stages.
17. Invalid aggregation pipelines are validated before TTL sweeps or observable
    mutation-adjacent behavior.
18. Existing query, index, TTL, collation, update, validation, and cursor tests
    continue to pass.
19. PyMongo e2e tests cover happy paths, not-happy paths, and adversarial paths
    for every new stage and expression family.
20. The local spec corpus includes representative success and error cases for
    computed projection, addFields/set/unset, replaceRoot, and lookup.
21. README compatibility tables and aggregation notes accurately describe the
    supported Aggregation v2 subset and remaining gaps.
22. `docs/mongodb-compatibility-uplifts-roadmap.md` marks uplift 5 complete,
    moves aggregation compatibility from 10% to at least 15%, and moves the
    total score from 72% to at least 77%.
23. `docs/performance-baseline.md` and/or benchmark coverage are updated if the
    new stages add meaningful performance risk.
24. `cargo fmt -- --check`, `cargo test`, `cargo build`,
    `cargo run --bin mongolino-bench -- --profile ci --check-budget`,
    `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`,
    `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`, and
    `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest
    tests/e2e` pass locally. Use unsandboxed execution for localhost binding if
    needed.
25. Milestone checkboxes in this file are marked `[x]` as work completes.
26. Each completed milestone has a focused commit.

## Milestone Checklist

When a milestone is complete:

1. Run the milestone's verification commands.
2. Update this checklist by changing the milestone from `[ ]` to `[x]`.
3. Add a short status note under that milestone with the date, exact commands
   run, and commit hash if available.
4. Commit the code, tests, docs, and checklist/status update with a focused
   commit message.
5. Report the commit hash in the goal-loop status before starting the next
   milestone.

- [x] Milestone 0: Expression evaluator v2
- [x] Milestone 1: Computed `$project`, `$addFields`, `$set`, and `$unset`
- [x] Milestone 2: `$replaceRoot`, `$replaceWith`, and group operand expansion
- [x] Milestone 3: Simple equality `$lookup`
- [x] Milestone 4: PyMongo e2e, spec corpus, docs, scorecard, and benchmarks
- [x] Milestone 5: Final verification and handoff

## Milestone 0: Expression Evaluator v2

Problem:

- The current expression helper only supports field paths, literals, and small
  document key specs. Computed projection, addFields, replaceRoot, and lookup
  need a reusable evaluator with explicit unsupported-shape errors.

Desired behavior:

- Replace or extend `AggregationExpression` into a bounded expression AST with
  context-aware parsing and evaluation.
- Preserve existing `$group` and `$unwind` behavior.
- Keep missing-field behavior deterministic and documented by tests.

Acceptance criteria:

- Parse and evaluate field paths, scalar literals, simple document expressions,
  and array expressions where the target operator allows arrays.
- Support variables `$$ROOT` and `$$CURRENT`.
- Support expression operators:
  - `$literal`;
  - `$ifNull`;
  - `$concat`;
  - `$toString`;
  - `$toLower`;
  - `$toUpper`;
  - `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`;
  - `$and`, `$or`, `$not`;
  - `$cond` in array and document forms;
  - `$add`, `$subtract`, `$multiply`, `$divide`.
- Use the repo's deterministic BSON ordering for comparison expressions.
- Use the active supported collation for string comparisons in expression
  equality/order where the command collation is available.
- Reject unknown operators, documents with multiple expression operators,
  invalid arity, invalid variable names, invalid field paths, unsupported value
  types, unsupported arrays in contexts that disallow arrays, and division by
  zero.
- Add Rust unit tests for supported expression parsing/evaluation and
  adversarial malformed shapes.
- Existing aggregation, collation, and group tests pass.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `docs/aggregation-v2-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test aggregation_expression
cargo test aggregate
cargo test collation
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

Status note (2026-07-05):

- Implemented a reusable aggregation expression AST/parser/evaluator with
  field paths, literals, document expressions, arrays, `$$ROOT`, `$$CURRENT`,
  `$literal`, `$ifNull`, `$concat`, `$toString`, `$toLower`, `$toUpper`,
  comparison operators, boolean operators, `$cond`, and arithmetic operators.
- Runtime expression failures such as division by zero now return command
  errors instead of silently becoming `null`; unsupported operators and
  malformed arity are rejected during aggregate preflight.
- Verification commands run:
  - `cargo fmt -- --check` - passed.
  - `cargo test aggregation_expression` - passed, 2 main tests and 2
    bench-target tests.
  - `cargo test aggregate` - passed, 16 main tests and 16 bench-target tests.
  - `cargo test collation` - passed, 12 main tests and 12 bench-target tests.
  - `cargo test` - passed, 173 main tests and 175 bench-target tests.
- Commit: generated by this milestone commit.

## Milestone 1: Computed `$project`, `$addFields`, `$set`, And `$unset`

Problem:

- Application pipelines commonly reshape documents with computed projection and
  derived fields. Current `$project` is find-style projection only, and
  `$addFields`, `$set`, and `$unset` are unsupported.

Desired behavior:

- Add computed shaping stages over the existing in-memory aggregation document
  stream.
- Preserve existing inclusion/exclusion projection semantics and errors.

Acceptance criteria:

- `$project` still supports existing inclusion and exclusion modes, including
  `_id` handling.
- `$project` supports computed fields using expression values such as
  `{ "display": { "$concat": ["$first", " ", "$last"] } }`.
- `$project` supports field aliases such as `{ "authorName": "$author.name" }`.
- `$project` supports nested computed document output where safe.
- `$project` rejects unsafe inclusion/exclusion/computed mixes, empty field
  names, invalid dotted paths, and parent/child path collisions.
- `$addFields` and `$set` accept a document of output paths to expressions and
  add or overwrite fields without dropping existing fields.
- `$addFields` and `$set` support dotted output paths and reject path collisions.
- `$unset` supports string and array-of-string forms and removes top-level or
  dotted fields.
- `$unset` rejects non-string entries, empty paths, dollar-prefixed segments, and
  path collisions.
- Invalid stage specs return command errors before TTL sweeps.
- Add Rust and PyMongo e2e tests for happy, not-happy, and adversarial paths.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_aggregation.py`
- `docs/aggregation-v2-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test aggregate
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_aggregation.py
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

Status note (2026-07-05):

- Added aggregate-specific computed `$project` support while preserving
  existing inclusion/exclusion behavior and `_id` handling.
- Added `$addFields` and `$set` for top-level and dotted-path computed writes,
  plus `$unset` string and array-of-string forms.
- Added explicit command errors for unsafe inclusion/exclusion/computed mixes,
  empty or dollar-prefixed paths, parent/child path collisions, malformed unset
  arrays, unsupported expressions, and runtime expression failures.
- Added Rust aggregation tests and PyMongo e2e coverage for happy paths,
  not-happy paths, and adversarial path-collision/runtime-error cases.
- Verification commands run:
  - `cargo fmt -- --check` - passed.
  - `cargo test aggregate` - passed, 18 main tests and 18 bench-target tests.
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest
    tests/e2e/test_aggregation.py` - sandboxed run failed before tests with
    `PermissionError: [Errno 1] Operation not permitted` while binding
    localhost.
  - `cargo build` - passed; rebuilt `target/debug/mongolino` for PyMongo e2e.
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest
    tests/e2e/test_aggregation.py` - passed unsandboxed, 20 passed.
  - `cargo test` - passed, 175 main tests and 177 bench-target tests.
- Commit: generated by this milestone commit.

## Milestone 2: `$replaceRoot`, `$replaceWith`, And Group Operand Expansion

Problem:

- Many pipelines compute a subdocument and promote it to the root, or use
  expression-derived values inside group accumulators. The current group parser
  only accepts a narrow expression subset.

Desired behavior:

- Add bounded root replacement and safely allow new expressions in group
  accumulator operands.

Acceptance criteria:

- `$replaceRoot` supports `{ newRoot: <document-valued expression> }`.
- `$replaceWith` supports a document-valued expression directly.
- Field-path expressions that evaluate to documents can become the new root.
- Computed document expressions can become the new root.
- Non-document results, missing fields, malformed specs, unknown options, and
  invalid expressions return explicit command errors.
- `$group` `_id` and accumulator operands use the new evaluator where safe.
- `$sum` and `$avg` preserve numeric-only behavior; unsupported nonnumeric
  constants still error where the previous subset required it.
- `$push`, `$addToSet`, `$first`, `$last`, `$min`, and `$max` can consume
  supported computed expressions.
- Existing `$addToSet` whole-value equality remains intact.
- Add Rust and PyMongo e2e tests for root replacement, group computed operands,
  and adversarial non-document replacement errors.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_aggregation.py`
- `tests/spec_corpus/aggregation_pipeline.json`
- `docs/aggregation-v2-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test aggregate
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_aggregation.py
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

Status note (2026-07-05):

- Added `$replaceRoot` with `{ newRoot: <expr> }` and `$replaceWith` with a
  document-valued expression.
- Expanded `$group` operands so `_id` and accumulators can consume the
  expression subset where safe; `$sum`/`$avg` still reject unsupported scalar
  constants while accepting computed numeric expressions.
- Added explicit command errors for malformed root replacement specs,
  unsupported options, unsupported expression operators, missing/non-document
  replacement results, and malformed accumulator operands.
- Added Rust and PyMongo e2e coverage for field-path root replacement,
  computed document replacement, computed accumulator operands, and adversarial
  non-document replacement failures.
- Verification commands run:
  - `cargo fmt -- --check` - passed.
  - `cargo test aggregate` - passed, 20 main tests and 20 bench-target tests.
  - `cargo build` - passed; rebuilt `target/debug/mongolino` for PyMongo e2e.
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest
    tests/e2e/test_aggregation.py` - passed unsandboxed, 22 passed.
  - `cargo test` - passed, 177 main tests and 179 bench-target tests.
- Commit: generated by this milestone commit.

## Milestone 3: Simple Equality `$lookup`

Problem:

- `$lookup` is one of the largest practical aggregation gaps. A conservative
  local equality join gives ODMs and application tests a major compatibility
  uplift without implementing pipeline lookup.

Desired behavior:

- Support only the common same-database simple equality form:
  `{ "$lookup": { "from": "profiles", "localField": "profileId",
  "foreignField": "_id", "as": "profile" } }`.

Acceptance criteria:

- `$lookup` accepts only `from`, `localField`, `foreignField`, and `as`.
- `from` must be a simple collection name in the same database.
- `localField`, `foreignField`, and `as` must be valid aggregation paths.
- The join preserves the original input document order.
- Each input document receives an array under `as`.
- Scalar local values match scalar foreign values using aggregation equality.
- Local array values match foreign scalar values when any local array element is
  equal.
- Missing and `null` behavior is documented and covered by tests.
- String equality uses the supported command collation subset.
- Self-lookup works without corrupting input stream state.
- Dotted local/foreign fields work for reads; dotted `as` writes are supported
  only if existing path helpers make them safe, otherwise reject them explicitly.
- `$lookup` rejects pipeline form, `let`, `as` path collisions, cross-database
  namespaces, non-string options, unsupported option keys, invalid paths, and
  noncollection `from` values.
- Aggregate cursor batching still works for lookup results.
- Add Rust and PyMongo e2e tests for happy path, no-match arrays, array local
  values, null/missing cases, collation, self-lookup, malformed specs, and
  unsupported pipeline/let forms.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_aggregation.py`
- `tests/spec_corpus/aggregation_pipeline.json` or a new focused lookup corpus
  file
- `docs/aggregation-v2-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test lookup
cargo test aggregate
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_aggregation.py
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

Status note (2026-07-05):

- Added same-database simple equality `$lookup` for `{ from, localField,
  foreignField, as }`.
- Implemented scalar equality, local array-to-foreign scalar matching,
  missing/null equality behavior, supported collation string equality,
  self-lookup, dotted read fields, cursor batching, and explicit rejection of
  pipeline/`let`/cross-db/invalid-path forms.
- Added Rust and PyMongo e2e coverage for happy paths, no-match arrays,
  array local values, null/missing behavior, collation, self-lookup, cursor
  batching, malformed specs, and unsupported forms.
- Verification commands run:
  - `cargo fmt -- --check` - passed.
  - `cargo test lookup` - passed, 2 main tests and 2 bench-target tests.
  - `cargo test aggregate` - passed, 22 main tests and 22 bench-target tests.
  - `cargo build` - passed; rebuilt `target/debug/mongolino` for PyMongo e2e.
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest
    tests/e2e/test_aggregation.py` - passed unsandboxed, 24 passed.
  - `cargo test` - passed, 179 main tests and 181 bench-target tests.
- Commit: generated by this milestone commit.

## Milestone 4: PyMongo E2E, Spec Corpus, Docs, Scorecard, And Benchmarks

Problem:

- This uplift touches shared aggregation semantics. The supported subset and
  unsupported boundary must be visible through real drivers and docs.

Desired behavior:

- Extend end-to-end tests, local corpus, README, roadmap, and benchmark evidence
  so the new compatibility surface is durable.

Acceptance criteria:

- PyMongo e2e tests cover:
  - expression operators in computed projection and addFields;
  - `$set` alias behavior;
  - `$unset` string and array forms;
  - `$replaceRoot` and `$replaceWith`;
  - `$lookup` simple equality, arrays, null/missing, collation, and self-lookup;
  - cursor batching over pipelines containing at least one new stage;
  - explicit errors for unsupported expression operators, malformed specs,
    path collisions, `$lookup` pipeline form, `$lookup` `let`, and unsupported
    aggregate command options.
- Local spec corpus covers representative success and failure cases for the new
  stages.
- README updates the aggregate row and aggregation notes with the supported
  v2 subset and residual unsupported behavior.
- `docs/mongodb-compatibility-uplifts-roadmap.md` marks uplift 5 complete,
  updates aggregation compatibility to at least 15%, updates total score to at
  least 77%, and points the next prompt to update pipelines and array filters.
- Benchmark coverage or `docs/performance-baseline.md` includes representative
  expression/addFields and lookup cases, or documents why the existing CI budget
  is sufficient.
- Milestone status is marked done in this file and committed.

Likely files:

- `tests/e2e/test_aggregation.py`
- `tests/e2e/test_spec_corpus.py`
- `tests/spec_corpus/aggregation_pipeline.json`
- `README.md`
- `docs/mongodb-compatibility-uplifts-roadmap.md`
- `docs/performance-baseline.md`
- `src/bin/mongolino-bench.rs`
- `docs/aggregation-v2-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test aggregate
cargo run --bin mongolino-bench -- --profile ci --check-budget
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_aggregation.py tests/e2e/test_spec_corpus.py
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

Status note (2026-07-05):

- Extended PyMongo e2e and local spec corpus coverage for expression operators,
  computed projection, `$addFields`/`$set`, `$unset`, `$replaceRoot`,
  `$replaceWith`, computed group operands, simple `$lookup`, cursor batching,
  unsupported expression/stage forms, malformed specs, and path collisions.
- Added corpus support for `$$collection` placeholders inside aggregate
  pipelines so self-lookup cases can use the per-test collection name.
- Updated README aggregation compatibility notes and command table.
- Updated `docs/mongodb-compatibility-uplifts-roadmap.md` to mark Aggregation
  v2 complete and move the repo-local scorecard from 72% to 77%.
- Added `aggregation_expression_add_fields` and
  `aggregation_lookup_single_document` benchmark sentinels plus performance
  baseline notes.
- Verification commands run:
  - `cargo fmt -- --check` - passed.
  - `cargo test aggregate` - passed, 22 main tests and 22 bench-target tests.
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check` - passed.
  - `cargo run --bin mongolino-bench -- --profile ci --check-budget` - passed;
    benchmark budget passed for profile `ci`. New rows:
    `aggregation_expression_add_fields` 254.35 ms total / 8.478 ms latency,
    `aggregation_lookup_single_document` 469.31 ms total / 15.644 ms latency.
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev` -
    passed.
  - `cargo build` - passed; rebuilt `target/debug/mongolino` for PyMongo e2e.
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest
    tests/e2e/test_aggregation.py tests/e2e/test_spec_corpus.py` - passed
    unsandboxed, 54 passed.
  - `cargo test` - passed, 179 main tests and 181 bench-target tests.
- Commit: generated by this milestone commit.

## Milestone 5: Final Verification And Handoff

Problem:

- The final state must be verified as a whole, not only milestone-by-milestone.

Desired behavior:

- Run the full project verification suite and record exact results.

Acceptance criteria:

- Full verification passes:

```bash
cargo fmt -- --check
cargo test
cargo build
cargo run --bin mongolino-bench -- --profile ci --check-budget
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

- If sandboxed PyMongo e2e is blocked by localhost binding, record the sandbox
  failure and rerun the exact command unsandboxed.
- This file contains a final status note with:
  - exact commands run;
  - Rust test counts;
  - PyMongo e2e pass count;
  - benchmark result summary;
  - commit hashes for every milestone;
  - residual unsupported aggregation behavior.
- Milestone status is marked done in this file and committed.

Likely files:

- `docs/aggregation-v2-goal-loop.md`

Commit requirement:

- Commit after marking this milestone done and adding the status note.

Status note (2026-07-05):

- Exact commands run:
  - `cargo fmt -- --check` - passed.
  - `cargo test` - passed; Rust test counts were 179 unit/integration tests in
    `src/main.rs` target coverage and 181 tests in the benchmark target build.
  - `cargo build` - passed with existing `dead_code` warnings for planner
    diagnostics helpers.
  - `cargo run --bin mongolino-bench -- --profile ci --check-budget` - passed
    the CI performance budget.
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check` - passed.
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev` -
    passed.
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e`
    - sandboxed run failed before server startup because the sandbox could not
    bind `127.0.0.1` (`PermissionError: [Errno 1] Operation not permitted`).
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e`
    - rerun unsandboxed and passed: 204 PyMongo e2e tests passed in 106.12s.
- Benchmark summary:
  - `aggregation_expression_add_fields`: 254.23 ms total, 8.474 ms mean
    latency.
  - `aggregation_lookup_single_document`: 469.66 ms total, 15.655 ms mean
    latency.
  - Full CI profile budget passed with aggregation, indexed find/count, update,
    collation, and cursor rows.
- Milestone commits:
  - Milestone 0: `2e68da5` Add aggregation expression evaluator v2.
  - Milestone 1: `1046999` Add computed aggregation shaping stages.
  - Milestone 2: `789fb7a` Add aggregation root replacement and computed group
    operands.
  - Milestone 3: `1598417` Add simple aggregation lookup.
  - Milestone 4: `8b5ca9a` Document and benchmark aggregation v2.
  - Milestone 5: final status note commit created after this note.
- Residual unsupported aggregation behavior:
  - `$lookup` remains limited to same-database equality joins using `from`,
    `localField`, `foreignField`, and `as`; `pipeline`, `let`, and cross-db
    forms return explicit command errors.
  - Advanced stages such as `$facet`, `$bucket`, `$sortByCount`, `$graphLookup`,
    `$out`, `$merge`, `$geoNear`, `$redact`, window stages, and JavaScript
    execution are still unsupported.
  - Command options such as aggregation `allowDiskUse`, `hint`, `explain`,
    `maxTimeMS`, command-level `let`, read concern, and write concern are still
    explicit unsupported behavior.
  - Expression support is intentionally bounded and does not claim full MongoDB
    numeric promotion, conversion, or ICU/collation parity.
  - Aggregation execution remains in Rust over materialized documents; lookup
    and computed expressions are not pushed down into SQLite query plans.

## Final Response Requirements

When the goal is complete, report:

- the final compatibility scorecard movement;
- every commit hash created for this goal;
- milestone checklist status;
- exact verification commands run and results;
- PyMongo e2e pass count;
- benchmark result summary;
- files changed;
- any known residual aggregation gaps or intentionally unsupported MongoDB
  behavior.

Do not call the goal complete if any milestone remains unchecked, verification
has not run, or README/roadmap docs still describe all Aggregation v2 behavior
as unsupported.
