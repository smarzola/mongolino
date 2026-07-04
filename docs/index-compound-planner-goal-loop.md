# Goal: Compound Index Planner And Benchmark Uplift

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to deliver the first large index uplift in the index
compatibility and performance sequence: make compound btree-style indexes
behaviorally useful for safe equality planning, count pushdown, and write target
selection. The current implementation stores compound index metadata but only
maintains single-field planner entries, so compound indexes do not provide
meaningful query performance.

This is index uplift 1 of 3. It must move the repo-local index compatibility
scorecard from **43% to at least 55%** and establish benchmark evidence for
compound index performance.

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

Compound indexes with numeric `1` or `-1` key directions should be maintained in
SQLite and used when behavior is equivalent to the Rust matcher:

- Full compound equality filters can use a maintained compound `index_entries`
  key.
- Compound equality `find` can narrow candidates through SQLite, then still run
  the Rust matcher before returning documents.
- Compound equality `count` can use SQLite count pushdown for safe non-numeric
  scalar values.
- update/delete/findAndModify target selection can use transaction-local
  compound candidate narrowing, then still run the Rust matcher before mutation.
- Unique compound checks may use pushdown only if all key parts are present,
  non-null, non-numeric, non-array scalar values; otherwise they must fall back.
- Existing single-field index behavior must not regress.

## Compatibility Target

Move the scorecard in `docs/index-uplifts-roadmap.md` from **43% to at least
55%**:

- Basic btree key specs and metadata: `8% -> 11%`, because compound key specs
  are now represented in maintained planner entries rather than metadata only.
- Planner use for reads and writes: `8% -> 15%`, because compound equality can
  accelerate `find`, `count`, update/delete, and findAndModify target selection.
- Explicit unsupported behavior remains at `4%` or better.

Do not claim support for range scans, compound prefix scans, sort-only compound
planning, collation, multikey, sparse, or partial indexes in this uplift unless
you implement and verify them.

## Performance Target

Add benchmark cases to `mongolino-bench` and record local before/after evidence.

Required benchmark targets on the local profile:

- `find_compound_equality`: at least **10x faster** than
  `find_collection_scan` and below **3 ms/op**.
- `count_compound_equality`: below **0.25 ms/op** for safe fully covered
  compound equality.
- `update_compound_target`: below **2 ms/op** for selective compound-indexed
  update target selection.

CI budget thresholds may be looser, but they must catch gross regressions.

## Current State

Relevant current implementation:

- `IndexSpec` stores `name`, `key`, and `unique`.
- `parse_index_spec` accepts numeric `1` and `-1` directions and rejects
  unsupported index options.
- `planner_key_for_document` returns a key only for `single_field_index_name`.
- `index_entries(namespace, index_name, key_value, id_key)` stores maintained
  lookup entries.
- Read planning uses `indexed_candidate_documents`.
- Count planning uses `plan_count` and `pushed_down_count`.
- Write target planning uses `plan_transaction_candidates` and
  `transaction_candidate_documents`.
- Unique pushdown uses `unique_conflict_check_with_index_entries_tx` only for
  safe single-field non-numeric scalar values.

Known constraints:

- Candidate narrowing must never exclude a document that the Rust matcher would
  accept.
- Numeric equality is cross-type in the Rust matcher, while `index_entries`
  values are type-tagged. Numeric compound planner pushdown must fall back
  unless you implement a provably equivalent encoding.
- Arrays and multikey semantics are out of scope for this uplift.
- Prefix scans, range scans, and sort pushdown are out of scope unless they are
  added with complete tests and no semantic risk.

## Definition Of Done

The goal is complete only when:

1. Compound index entries are maintained for safe full-key scalar compound
   indexes.
2. Compound entries are rebuilt on `createIndexes` and removed on `dropIndexes`,
   `drop`, and `dropDatabase`.
3. Compound entries stay fresh after insert, update, upsert, delete, and
   findAndModify.
4. `find` uses compound entries for safe full-key equality filters and still
   validates candidates with the Rust matcher.
5. `count` uses compound entries for safe full-key non-numeric scalar equality
   filters and falls back for numeric, null, missing, array, document, and
   unsupported operator cases.
6. update/delete/findAndModify target selection uses transaction-local compound
   entries for safe full-key equality filters and still validates candidates
   with the Rust matcher.
7. Unique compound checks use index entries only for safe non-numeric scalar
   full-key checks, and fall back for numeric/null/missing/array/document cases.
8. Existing single-field index fast paths and unique behavior do not regress.
9. Benchmarks include `find_compound_equality`, `count_compound_equality`, and
   `update_compound_target` with local before/after evidence.
10. `docs/performance-baseline.md` and `docs/index-uplifts-roadmap.md` are
    updated with benchmark results and scorecard movement.
11. PyMongo e2e tests cover compound index catalog behavior, compound indexed
    find/count, write target freshness, and compound unique enforcement.
12. `cargo fmt -- --check`, `cargo test`, `cargo build`,
    `cargo run --bin mongolino-bench -- --profile ci --check-budget`,
    `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`,
    `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`, and
    targeted PyMongo e2e pass locally. Use unsandboxed execution for localhost
    binding if needed.
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

- [x] Milestone 0: Compound key encoding and planner classification
- [x] Milestone 1: Compound index entry maintenance
- [x] Milestone 2: Compound read and count pushdown
- [x] Milestone 3: Compound write target and unique pushdown
- [x] Milestone 4: Benchmarks, docs, and final verification

## Milestone 0: Compound Key Encoding And Planner Classification

Problem:

- `planner_key_for_document`, read planning, count planning, and transaction
  candidate planning all assume single-field indexes.

Desired behavior:

- Add helpers that can encode a full compound key for safe scalar values.
- Add helpers that can classify a filter as a full compound equality match for
  a given index.
- Treat field order according to the index key document order.
- Reject/fallback for numeric values, null/missing, arrays, document values,
  logical operators, multi-operator predicates, range operators, `$in`, `$nin`,
  `$ne`, `$exists`, `$not`, and partial field coverage.

Acceptance criteria:

- Rust tests cover compound key encoding order.
- Rust tests cover full-key equality planner classification.
- Rust tests cover fallback for numeric, array, missing, partial, and operator
  filters.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `docs/index-compound-planner-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test planner
cargo test index
```

Status:

- 2026-07-04: Added conservative compound key encoding and full-key equality
  filter classification. Compound planner keys use index key order and reject
  numeric, null, missing, array, document, partial, extra-field, logical, and
  unsupported operator filters. Verified with `cargo fmt -- --check`;
  `cargo test planner`; `cargo test index`. Commit hash: `37587a2`.

## Milestone 1: Compound Index Entry Maintenance

Problem:

- Compound indexes are stored in metadata but do not get maintained
  `index_entries`.

Desired behavior:

- Rebuild and refresh compound index entries for safe documents.
- Drop compound entries together with index metadata.
- Preserve existing single-field entry behavior.

Acceptance criteria:

- Creating a compound index backfills entries for existing matching documents.
- Insert/update/upsert/delete/findAndModify refresh compound entries.
- Documents with unsafe compound key parts are omitted from planner entries but
  still participate in Rust fallback behavior.
- Dropping one index, all indexes, a collection, or a database removes compound
  entries.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_indexes.py`
- `tests/e2e/test_find_and_modify.py`
- `docs/index-compound-planner-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test index
cargo test planner
cargo test find_and_modify
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_indexes.py tests/e2e/test_find_and_modify.py
```

Status:

- 2026-07-04: Compound indexes now rebuild and refresh maintained
  `index_entries` through the existing create, insert, update, delete, upsert,
  and findAndModify hooks. Unsafe compound key parts are omitted from planner
  entries and remain matcher fallback behavior. Verified with `cargo fmt --
  --check`; `cargo test index`; `cargo test planner`; `cargo test
  find_and_modify`; sandboxed PyMongo e2e failed at localhost bind with
  `PermissionError: [Errno 1] Operation not permitted`; unsandboxed
  `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest
  tests/e2e/test_indexes.py tests/e2e/test_find_and_modify.py` passed. Commit
  hash: pending.

## Milestone 2: Compound Read And Count Pushdown

Problem:

- Full compound equality filters currently fall back to collection scans or
  single-field candidates even when a compound index is available.

Desired behavior:

- Use compound entries for safe full-key equality `find`.
- Use compound entries for safe full-key non-numeric scalar equality `count`.
- Use the same safe count path for exact aggregation `$match` + `$count` if the
  filter is compound-pushdown-safe.
- Always run the Rust matcher for `find` candidates before returning results.

Acceptance criteria:

- PyMongo e2e covers compound indexed `find` and `count_documents`.
- Rust tests prove fallback for numeric and partial compound filters.
- Existing single-field count and find tests still pass.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_indexes.py`
- `tests/e2e/test_metadata.py`
- `tests/e2e/test_aggregation.py`
- `docs/index-compound-planner-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test count
cargo test find
cargo test aggregate_match_count
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_indexes.py tests/e2e/test_metadata.py tests/e2e/test_aggregation.py
cargo test
```

Status:

- 2026-07-04: Full-key safe compound equality now narrows `find` candidates
  through maintained entries while still applying the Rust matcher, and
  `count` plus aggregation `$match` + `$count` use the same safe count path.
  Numeric, partial, extra-field, array, document, null, and operator shapes
  fall back. Verified with `cargo fmt -- --check`; `cargo test count`;
  `cargo test find`; `cargo test aggregate_match_count`; `cargo test`;
  sandboxed PyMongo e2e failed at localhost bind with `PermissionError: [Errno
  1] Operation not permitted`; unsandboxed `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache
  uv run --locked pytest tests/e2e/test_indexes.py tests/e2e/test_metadata.py
  tests/e2e/test_aggregation.py` passed. Commit hash: pending.

## Milestone 3: Compound Write Target And Unique Pushdown

Problem:

- update/delete/findAndModify target selection and compound unique checks scan
  namespaces even when a safe compound equality index can narrow the work.

Desired behavior:

- Use compound entries for safe transaction-local target narrowing.
- Keep Rust matcher validation before every mutation.
- Use compound unique pushdown only for safe non-numeric scalar full-key checks.
- Fall back for unsupported unique shapes.

Acceptance criteria:

- update/delete/findAndModify use compound candidates for safe full-key
  equality filters.
- Duplicate-key behavior is unchanged for compound unique indexes.
- Numeric unique compound values use fallback and preserve cross-type numeric
  conflict behavior.
- PyMongo e2e covers update, delete, findAndModify, and unique compound paths.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_crud.py`
- `tests/e2e/test_indexes.py`
- `tests/e2e/test_find_and_modify.py`
- `tests/e2e/test_update_operators.py`
- `docs/index-compound-planner-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test update
cargo test delete
cargo test find_and_modify
cargo test unique
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_crud.py tests/e2e/test_indexes.py tests/e2e/test_find_and_modify.py tests/e2e/test_update_operators.py
cargo test
```

Status:

- 2026-07-04: Transaction-local candidate planning now covers safe full-key
  compound equality for update, delete, and findAndModify, with matcher
  validation retained before mutation. Compound unique checks use index-entry
  pushdown for safe non-numeric scalar full keys and fall back for numeric and
  unsafe shapes. Verified with `cargo fmt -- --check`; `cargo test update`;
  `cargo test delete`; `cargo test find_and_modify`; `cargo test unique`;
  `cargo test`; sandboxed PyMongo e2e failed at localhost bind with
  `PermissionError: [Errno 1] Operation not permitted`; unsandboxed
  `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest
  tests/e2e/test_crud.py tests/e2e/test_indexes.py
  tests/e2e/test_find_and_modify.py tests/e2e/test_update_operators.py`
  passed. Commit hash: pending.

## Milestone 4: Benchmarks, Docs, And Final Verification

Problem:

- The benchmark suite does not yet measure compound index performance.

Desired behavior:

- Add benchmark cases for compound equality find, compound equality count, and
  compound update target selection.
- Record before/after local numbers.
- Update the scorecard and roadmap.

Acceptance criteria:

- `find_compound_equality`, `count_compound_equality`, and
  `update_compound_target` exist in `mongolino-bench`.
- Local benchmark evidence meets the performance targets in this prompt.
- CI budget includes thresholds for the new benchmarks.
- `docs/performance-baseline.md` and `docs/index-uplifts-roadmap.md` are
  updated.
- Full verification passes.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/bin/mongolino-bench.rs`
- `.github/workflows/ci.yml` if needed
- `docs/performance-baseline.md`
- `docs/index-uplifts-roadmap.md`
- `docs/index-compound-planner-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test
cargo build
cargo run --bin mongolino-bench -- --profile smoke --json /tmp/mongolino-bench-index-compound-smoke.json
cargo run --bin mongolino-bench -- --profile local --json /tmp/mongolino-bench-index-compound-local.json
cargo run --bin mongolino-bench -- --profile ci --check-budget
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

Use unsandboxed execution for PyMongo e2e if the sandbox blocks localhost
binding.

Commit requirement:

- Commit after marking this milestone done and adding the status note.

Status:

- 2026-07-04: Added `find_compound_equality`,
  `count_compound_equality`, and `update_compound_target` benchmarks, CI budget
  thresholds, local before/after evidence, and scorecard documentation. Final
  local results: `find_compound_equality` `2.122 ms/op` versus
  `find_collection_scan` `30.807 ms/op` (`14.5x` faster);
  `count_compound_equality` `0.030 ms/op`; `update_compound_target`
  `1.336 ms/op`. Verified with `cargo fmt -- --check`; `cargo test`;
  `cargo build`; `cargo run --bin mongolino-bench -- --profile smoke --json
  /tmp/mongolino-bench-index-compound-smoke.json`; `cargo run --bin
  mongolino-bench -- --profile local --json
  /tmp/mongolino-bench-index-compound-local.json`; `cargo run --bin
  mongolino-bench -- --profile ci --check-budget`;
  `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`;
  `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`;
  sandboxed full e2e failed at localhost bind with `PermissionError: [Errno 1]
  Operation not permitted`; unsandboxed `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache
  uv run --locked pytest tests/e2e` passed with 136 passed and 1 skipped.
  Commit hash: pending.
- 2026-07-04 adversarial follow-up: Added maintained multikey omission
  sentinels so single-field and full-key compound scalar planners fall back
  when indexed array paths would make `index_entries` incomplete. This preserves
  scalar compound pushdown on clean datasets but does not implement full
  multikey index entries; scalar array-element indexing remains in the later
  multikey uplift.

## Final Response Requirements

When complete, report:

- commits made;
- files changed;
- compatibility scorecard movement;
- pushed-down compound cases;
- fallback cases that intentionally remain Rust-side;
- benchmark before/after headline numbers;
- final verification commands and outcomes;
- known residual risks.
