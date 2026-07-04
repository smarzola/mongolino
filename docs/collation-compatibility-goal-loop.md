# Goal: Collation Compatibility Uplift

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to deliver uplift 4 of 7 in the aggressive MongoDB
compatibility sequence: add a practical, explicit collation subset for common
driver workflows without pretending to implement full ICU/MongoDB collation
parity. This uplift must make collation observable through real PyMongo
commands on reads, write target selection, `findAndModify`, aggregation
`$match`/`$sort`, distinct de-duplication, and safe index metadata/enforcement.

This uplift starts after TTL Index Compatibility moved the repo-local
compatibility scorecard in `docs/mongodb-compatibility-uplifts-roadmap.md` from
**62% to 68%**. This uplift must move the scorecard from **68% to at least
72%** without weakening explicit errors for unsupported collation shapes.

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
- Do not implement broad locale/ICU behavior by accident. If a collation option
  is not part of the documented subset, reject it explicitly.
- Do not use an index when collation semantics could make the index unsafe.
- Do not revert unrelated user or agent changes.
- You are not alone in the codebase. Other agents may have touched nearby
  files; work with current state and do not revert unrelated edits.
- At the end of each milestone, run verification, mark the milestone done in
  this file, commit the completed milestone, and report the commit hash before
  continuing.

## Target State

Add a conservative collation subset:

- Parse a `collation` document on supported command paths:
  - `find`;
  - `count`;
  - `distinct`;
  - `aggregate`;
  - update entries;
  - delete entries;
  - findAndModify;
  - `createIndexes`/`listIndexes` metadata.
- Supported collation shapes:
  - `{locale: "simple"}` as exact existing binary behavior.
  - `{locale: "en", strength: 2}` and `{locale: "en_US", strength: 2}` as a
    documented case-insensitive string subset.
  - Optionally accept omitted `strength` for `locale: "simple"` only.
  - All unsupported options must return explicit command/write errors:
    `caseLevel`, `caseFirst`, `numericOrdering`, `alternate`, `maxVariable`,
    `backwards`, `normalization`, unsupported `locale`, unsupported
    `strength`, non-document collation, empty non-simple collation, and unknown
    fields.
- The non-simple supported subset is case-insensitive for Unicode strings using
  Rust's deterministic lowercase mapping. It is not full ICU:
  - no locale-specific tailoring;
  - no diacritic folding unless the chosen implementation explicitly documents
    and tests it;
  - no numeric ordering;
  - no alternate/shifted behavior.
- Command matching uses the selected collation for string equality semantics:
  direct equality, `$eq`, `$ne`, `$in`, `$nin`, `$all`, `$elemMatch`, logical
  nesting, update/delete/findAndModify target matching, aggregate `$match`, and
  count/distinct filters.
- Sorting uses the selected collation for string ordering on `find`,
  findAndModify sort, aggregate `$sort`, and final distinct value ordering.
- Distinct de-duplicates string values under the selected collation.
- Non-string BSON values keep existing comparison/equality semantics.
- String range predicates (`$gt`, `$gte`, `$lt`, `$lte`) under non-simple
  collation must either be implemented with clear semantics and adversarial
  tests or rejected/fall back safely. Do not let binary index ordering answer a
  non-simple collation range query.
- Index metadata supports supported `collation` documents:
  - `createIndexes` accepts and persists supported collation metadata.
  - `listIndexes` returns the stored collation for collation-aware indexes.
  - duplicate index spec comparison includes collation metadata.
  - unsupported index collation documents return explicit command errors.
- Unique index enforcement respects supported case-insensitive collation for at
  least safe single-field string indexes. Broaden to compound indexes only if
  semantics are proven and tested.
- Planner use of indexes is collation-safe:
  - simple/binary queries can keep existing index behavior.
  - non-simple collation queries may use a matching collation-aware index only
    for proven-safe exact equality/count/update/delete/findAndModify target
    shapes.
  - non-matching or unsupported index collation must fall back or error for
    hints without mutating documents.
  - sort pushdown under non-simple collation should remain disabled unless a
    matching collation index order is proven and tested.

## Compatibility Target

Move `docs/mongodb-compatibility-uplifts-roadmap.md` from **68% to at least
72%**:

- Index lifecycle/TTL/collation behavior: `11% -> at least 13%`.
- Query predicate compatibility: `17% -> at least 18%` through collation-aware
  equality.
- Aggregation compatibility: `9% -> at least 10%` through collation-aware
  `$match` and `$sort`.
- Explicit unsupported behavior remains `5%`.

Do not claim full ICU collation, locale-specific sort orders, diacritic
folding, numeric ordering, text search collation behavior, geospatial
collation, collation-aware range index planning, collation-aware broad sort
pushdown, or full MongoDB index collation parity unless actually implemented
and verified.

## Current State

Relevant current implementation:

- `collation` is rejected on read/write/aggregate paths as an unsupported
  command or entry key.
- `bson_values_equal`, `compare_bson`, `compare_bson_order`,
  `sort_documents`, `matches_filter`, aggregate `$match`, update/delete target
  selection, and findAndModify all use binary/current BSON semantics.
- Index metadata stores `name`, `key`, `unique`, `sparse`, `partial_filter`,
  and TTL metadata, but no collation metadata.
- Planner and unique enforcement encode string index keys with binary semantics.
- The TTL uplift added preflight validation before TTL sweeps; preserve this
  no-mutation-on-error behavior for invalid collation documents and invalid
  collation-aware hints.
- README currently says collation is unsupported on `find`, `count`,
  `distinct`, `aggregate`, `update`, `delete`, findAndModify, indexes, create,
  and collMod.

Known constraints:

- Existing binary semantics must not regress when no collation is provided.
- A non-simple collation must not accidentally use binary string indexes.
- Invalid collation documents must not trigger TTL sweeps or write mutations.
- Unique index enforcement must not weaken existing duplicate-key guarantees.
- Do not add a large dependency unless it is clearly justified and documented.

## Definition Of Done

The goal is complete only when:

1. A small internal `Collation` model exists with explicit parsing, validation,
   equality, sort, and stable serialization behavior.
2. Unsupported collation documents return explicit command/write errors before
   TTL sweeps or write mutations.
3. Existing no-collation behavior and `{locale: "simple"}` behavior match
   current binary semantics.
4. `find`, `count`, `distinct`, `aggregate`, update, delete, and findAndModify
   accept the supported collation subset.
5. Collation-aware string equality works for direct equality, `$eq`, `$ne`,
   `$in`, `$nin`, `$all`, `$elemMatch`, logical filters, aggregate `$match`,
   and write target selection.
6. Collation-aware string sorting works for `find`, findAndModify sort, and
   aggregate `$sort` without breaking deterministic `_id` tie-breaking.
7. Distinct de-duplicates and orders string values according to the supported
   collation subset.
8. `createIndexes` persists and `listIndexes` returns supported collation
   metadata; duplicate index comparison includes collation.
9. Unique index enforcement respects supported case-insensitive collation for
   the implemented safe index subset.
10. Planner, hint, count pushdown, and write target selection are
    collation-safe. Unsafe collation/index combinations fall back or return
    explicit hint errors without mutation.
11. Rust tests and PyMongo e2e cover happy paths, non-happy paths, and
    adversarial paths.
12. README and `docs/mongodb-compatibility-uplifts-roadmap.md` accurately
    document newly supported and still unsupported collation behavior.
13. Verification commands pass locally.
14. Milestone checkboxes in this file are marked `[x]` as work completes.
15. Each completed milestone has a focused commit.

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

- [x] Milestone 0: Collation parser and comparison model
- [x] Milestone 1: Collation-aware read matching, sorting, and distinct
- [x] Milestone 2: Collation-aware write target selection and no-mutation errors
- [ ] Milestone 3: Index metadata, unique enforcement, and safe planning
- [ ] Milestone 4: PyMongo/spec-corpus adversarial coverage
- [ ] Milestone 5: Benchmarks, docs, scorecard, final verification

## Milestone 0: Collation Parser And Comparison Model

Problem:

- Current equality and ordering helpers have no collation context.

Desired behavior:

- Add a small `Collation` type with variants for binary/simple and supported
  English case-insensitive strength-2 behavior.
- Parse supported `collation` BSON documents consistently for commands and
  indexes.
- Provide helper functions for collation-aware equality, string key folding,
  and ordering while keeping non-string BSON behavior unchanged.

Acceptance criteria:

- Rust tests cover parsing accepted shapes and rejecting malformed/unsupported
  shapes.
- Rust tests cover binary equality/order parity, case-insensitive equality,
  case-insensitive ordering, arrays with scalar matching, and non-string BSON
  comparisons.
- Existing matcher/sort tests pass without collation.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `docs/collation-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test collation
cargo test matcher
cargo test sort
```

Status 2026-07-04: Complete. Added the internal collation parser/model,
case-insensitive equality/order helpers, matcher/sort threading, and
collation-aware index key metadata scaffolding. Verified with
`cargo fmt -- --check`, `cargo test collation`, `cargo test matcher`, and
`cargo test sort`. Commit: `8e49905`.

## Milestone 1: Collation-Aware Read Matching, Sorting, And Distinct

Problem:

- Read commands reject collation and always use binary string semantics.

Desired behavior:

- Accept supported command-level `collation` for `find`, `count`, `distinct`,
  and `aggregate`.
- Thread collation through filter matching, projection-independent shaping,
  sort, aggregate `$match`, aggregate `$sort`, count fallback/count pushdown
  decisions, and distinct de-duplication.
- Disable or fall back from unsafe binary index pushdown when non-simple
  collation is active unless a matching collation index is available later in
  the goal.
- Preserve explicit errors before TTL sweeps for invalid collation documents.

Acceptance criteria:

- Rust tests cover `find` equality, `$in`, `$ne`, logical predicates,
  collation-aware sort, `count`, aggregate `$match`, aggregate `$sort`, and
  `distinct`.
- Tests prove `{locale: "simple"}` keeps binary behavior.
- Tests prove unsupported collation documents return command errors and do not
  trigger TTL deletion.
- Existing no-collation read tests pass.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_metadata.py`
- `docs/collation-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test collation
cargo test find
cargo test count
cargo test aggregate
cargo test distinct
```

Status 2026-07-04: Complete. `find`, `count`, `distinct`, and `aggregate`
accept the supported collation subset, use it for matching/sorting/distinct
deduplication, preserve `{locale: "simple"}` binary behavior, and reject
invalid collation before TTL sweeps. Verified with `cargo fmt -- --check`,
`cargo test collation`, `cargo test find`, `cargo test count`,
`cargo test aggregate`, and `cargo test distinct`. Commit: `19108aa`.

## Milestone 2: Collation-Aware Write Target Selection And No-Mutation Errors

Problem:

- update/delete/findAndModify currently reject collation and target documents
  with binary matching/sorting only.

Desired behavior:

- Accept supported per-entry collation for update and delete entries.
- Accept supported command-level collation for findAndModify.
- Apply collation to query matching and sort used for target selection.
- Reject unsupported collation before TTL sweeps or mutations.
- Preserve ordered/unordered batch semantics.

Acceptance criteria:

- Rust tests cover update-one, update-many, delete-one, delete-many, and
  findAndModify target selection with case-insensitive collation.
- Tests prove invalid collation documents do not mutate documents or trigger
  TTL deletion.
- Tests prove existing no-collation write behavior is unchanged.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_crud.py`
- `tests/e2e/test_find_and_modify.py`
- `docs/collation-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test collation
cargo test update
cargo test delete
cargo test find_and_modify
cargo test ttl
```

Status 2026-07-04: Complete. Update/delete entry collation and findAndModify
command collation now drive write target matching and findAndModify sort
selection, while invalid collation documents return errors before TTL sweeps or
mutations. Verified with `cargo fmt -- --check`, `cargo test collation`,
`cargo test update`, `cargo test delete`, `cargo test find_and_modify`, and
`cargo test ttl`. Commit: pending.

## Milestone 3: Index Metadata, Unique Enforcement, And Safe Planning

Problem:

- Index metadata has no collation, and binary index entries are unsafe for
  non-simple collation queries.

Desired behavior:

- Add optional collation metadata to `IndexSpec` and SQLite persistence.
- `createIndexes` accepts supported `collation` documents; `listIndexes`
  returns them.
- Existing indexes without collation load as simple/binary.
- Duplicate index comparison includes collation metadata.
- Unique index enforcement respects the supported case-insensitive collation
  for a safe documented subset, at minimum single-field string indexes.
- Planner/hint behavior is collation-safe:
  - binary queries use binary indexes as before;
  - non-simple queries only use matching collation-aware indexes when safe;
  - incompatible hints return explicit errors and do not mutate writes;
  - unsafe count pushdown falls back to matcher-based counting.

Acceptance criteria:

- Rust tests cover migration-safe loading, create/list roundtrip, duplicate
  spec conflict/idempotency, invalid index collation rejection, unique
  case-insensitive duplicate enforcement, safe matching index use, and unsafe
  hint errors.
- Tests prove sparse/partial/TTL interactions are either supported with proof
  or explicitly rejected.
- Existing index, unique, planner, TTL, and hint tests pass.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_indexes.py`
- `docs/collation-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test collation
cargo test index
cargo test unique
cargo test planner
cargo test hint
cargo test ttl
```

## Milestone 4: PyMongo/Spec-Corpus Adversarial Coverage

Problem:

- Collation is primarily a driver-visible behavior and must be validated with
  real PyMongo command shapes.

Desired behavior:

- Add PyMongo e2e tests for supported collation on:
  - `find`;
  - `count_documents`;
  - `distinct`;
  - `aggregate`;
  - `update_one`/`update_many`;
  - `delete_one`/`delete_many`;
  - `find_one_and_update`/delete;
  - `create_index` and `list_indexes`;
  - unique case-insensitive index enforcement.
- Add or extend spec-corpus cases for supported and unsupported collation
  shapes.
- Include adversarial no-mutation tests for invalid collation on read and write
  paths with expired TTL documents present.

Acceptance criteria:

- PyMongo e2e covers happy paths and explicit unsupported errors for unsupported
  collation options.
- Spec corpus covers case-insensitive equality/sort/distinct and invalid
  option rejection.
- Full e2e subset for affected tests passes outside sandbox if localhost bind
  requires it.
- Milestone status is marked done in this file and committed.

Likely files:

- `tests/e2e/test_crud.py`
- `tests/e2e/test_metadata.py`
- `tests/e2e/test_aggregation.py`
- `tests/e2e/test_find_and_modify.py`
- `tests/e2e/test_indexes.py`
- `tests/e2e/test_spec_corpus.py`
- `tests/spec_corpus/*.json`
- `docs/collation-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test collation
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_crud.py tests/e2e/test_metadata.py tests/e2e/test_aggregation.py tests/e2e/test_find_and_modify.py tests/e2e/test_indexes.py tests/e2e/test_spec_corpus.py
```

## Milestone 5: Benchmarks, Docs, Scorecard, Final Verification

Problem:

- Collation changes shared matcher/sort/index hot paths and must be documented
  precisely.

Desired behavior:

- Add benchmark coverage for representative collation equality/sort and
  case-insensitive unique enforcement, or document why the existing CI budget
  plus focused tests are sufficient.
- Update README compatibility rows for `find`, `count`, `aggregate`,
  `distinct`, update, delete, findAndModify, BSON storage/indexes, and any
  remaining unsupported collation behavior.
- Update `docs/mongodb-compatibility-uplifts-roadmap.md`:
  - mark uplift 4 complete;
  - move scorecard from **68%** to at least **72%**;
  - keep full ICU/unsupported collation options visible;
  - point the next prompt to Aggregation v2.
- Run the full verification suite.

Acceptance criteria:

- README accurately describes supported simple and case-insensitive collation
  subset and residual unsupported behavior.
- Roadmap scorecard reaches at least 72%.
- Benchmark budget passes.
- Full Rust and PyMongo e2e suites pass.
- Milestone status is marked done in this file and committed.

Likely files:

- `README.md`
- `docs/mongodb-compatibility-uplifts-roadmap.md`
- `docs/performance-baseline.md`
- `src/bin/mongolino-bench.rs`
- `docs/collation-compatibility-goal-loop.md`

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

## Final Response Requirements

When the goal is complete, report:

- the final compatibility scorecard movement;
- every commit hash created for this goal;
- milestone checklist status;
- exact verification commands run and results;
- PyMongo e2e pass count;
- benchmark result summary;
- files changed;
- any known residual collation gaps or intentionally unsupported MongoDB
  behavior.

Do not call the goal complete if any milestone remains unchecked, verification
has not run, or README/roadmap docs still describe all collation behavior as
unsupported.
