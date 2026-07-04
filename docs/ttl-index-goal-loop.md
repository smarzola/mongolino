# Goal: TTL Index Compatibility Uplift

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to deliver uplift 3 of 7 in the aggressive MongoDB
compatibility sequence: add practical TTL index compatibility on top of the
existing SQLite-backed index catalog. This means `expireAfterSeconds` metadata
must be parsed, persisted, listed, dropped, updated through a conservative
`collMod` subset, and used by deterministic expiration behavior that is visible
through real client reads and writes.

This uplift starts after Index Planner v2 moved the repo-local compatibility
scorecard in `docs/mongodb-compatibility-uplifts-roadmap.md` from **55% to
62%**. This uplift must move the scorecard from **62% to at least 68%**,
primarily through the Index lifecycle/TTL/collation behavior area. Do not claim
collation support in this uplift.

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
- Do not optimize by weakening matcher, validator, unique-index, or planner
  semantics.
- Do not add a background thread unless it is strictly necessary. Prefer
  deterministic sweep points that can be tested without sleeps.
- Do not revert unrelated user or agent changes.
- You are not alone in the codebase. Other agents may have touched nearby
  files; work with current state and do not revert unrelated edits.
- At the end of each milestone, run verification, mark the milestone done in
  this file, commit the completed milestone, and report the commit hash before
  continuing.

## Target State

Add a conservative, observable TTL subset:

- `createIndexes` accepts `expireAfterSeconds` for supported single-field date
  TTL indexes and persists it with index metadata.
- `listIndexes` returns `expireAfterSeconds` for TTL indexes.
- `dropIndexes`, `drop`, and `dropDatabase` remove TTL metadata and maintained
  index entries consistently with existing index lifecycle behavior.
- TTL indexes can coexist with ordinary indexes and with multiple TTL indexes
  on the same collection. A document expires when any TTL index proves it is
  expired.
- Expiration applies only when the indexed field resolves to a BSON `DateTime`
  older than or equal to the TTL cutoff:
  - `expireAfterSeconds: 0` expires documents whose indexed date is at or
    before the sweep time.
  - Positive `expireAfterSeconds` expires documents whose indexed date is at or
    before `sweep_time - expireAfterSeconds`.
  - Missing fields, `null`, non-date scalar values, arrays, documents, and
    unsupported dotted-path shapes must not expire unless a behavior is
    explicitly proven and tested.
- Expiration deletes the document and all maintained index entries for that
  document atomically.
- Deterministic expiration runs at safe command boundaries so real clients can
  observe expired documents disappearing without waiting for a background
  monitor. At minimum, trigger sweeps before collection reads and before
  document-producing writes in the affected namespace.
- TTL deletion must never mutate data when `createIndexes` or `collMod`
  validation fails.
- `collMod` supports the narrow MongoDB-compatible TTL update shape:
  `{"collMod": <collection>, "index": {"name": <indexName>, "expireAfterSeconds": <seconds>}}`.
  Unsupported `collMod` index shapes return explicit command errors and do not
  weaken existing validator `collMod` behavior.
- Unsupported index features remain explicit errors: compound TTL indexes,
  `_id` TTL, text/geospatial/hashed/wildcard indexes, hidden indexes,
  collation-aware indexes, background semantics, and unsupported option types.

## Compatibility Target

Move `docs/mongodb-compatibility-uplifts-roadmap.md` from **62% to at least
68%**:

- Index lifecycle/TTL/collation behavior: `5% -> at least 11%`.
- Explicit unsupported behavior remains `5%`.
- Other scorecard areas should not be inflated by this uplift.

Do not claim support for background TTL monitor timing, TTL on compound indexes,
TTL on non-date values, clustered/time-series behavior, TTL deletes with
MongoDB's exact production cadence, TTL index conversion beyond the narrow
`collMod` shape above, or collation-aware indexes.

## Current State

Relevant current implementation:

- `IndexSpec` stores `name`, `key`, `unique`, `sparse`, and
  `partial_filter`.
- `parse_index_spec` rejects every index option except `key`, `name`, `unique`,
  `v`, `sparse`, and `partialFilterExpression`.
- `listIndexes` lists `_id_` plus persisted user index metadata, but does not
  expose TTL options.
- `collMod` currently supports only validator metadata and rejects
  `expireAfterSeconds`.
- `indexes` and `index_entries` are durable SQLite tables. Existing index entry
  cleanup already runs for normal document deletion, index drop, collection
  drop, and database drop paths.
- The read path uses helpers such as `documents_for_namespace`,
  `candidate_documents_with_hint`, `pushed_down_count`, and
  `distinct_command`.
- The write path enforces validators and unique indexes, then maintains
  `index_entries` after document changes.
- README currently lists TTL indexes as unsupported.

Known constraints:

- Candidate narrowing and count pushdown must not see expired documents after a
  TTL sweep point.
- TTL must not bypass validators or unique indexes by mutating documents other
  than deletion of expired documents.
- Real MongoDB uses a periodic TTL monitor, but this repo should prefer a
  deterministic sweep that tests reliably and avoids sleeps.
- Existing benchmarks must not regress grossly. TTL sweeps should be bounded to
  namespaces with TTL indexes and avoid scanning every collection on unrelated
  commands.

## Definition Of Done

The goal is complete only when:

1. `IndexSpec` and SQLite persistence represent optional TTL metadata.
2. Existing indexes created before this change still load correctly.
3. `createIndexes` accepts `expireAfterSeconds` only for valid TTL specs.
4. Invalid TTL specs return explicit command errors and do not create metadata,
   index entries, or mutate documents.
5. `listIndexes` includes exact TTL metadata for TTL indexes and omits it for
   non-TTL indexes.
6. `dropIndexes`, `drop`, and `dropDatabase` remove TTL metadata and entries
   without leaving orphaned index state.
7. Deterministic TTL sweeps remove expired documents and all maintained index
   entries atomically.
8. TTL sweeps run from real command paths before observable reads and before
   document-producing writes for the relevant namespace.
9. Documents with future dates, missing TTL fields, `null`, non-date values,
   arrays, documents, and unsupported dotted-path shapes are not expired.
10. Multiple TTL indexes on one collection are supported conservatively: a
    document expires if any TTL index proves expiration.
11. `collMod` can update `expireAfterSeconds` for an existing TTL-capable index
    by name, and rejects unknown indexes, non-TTL indexes if unsupported,
    invalid values, malformed `index` documents, and ambiguous/unsupported
    shapes without mutating state.
12. Expiration behavior is covered by Rust unit/integration tests and PyMongo
    e2e tests using real client handshakes.
13. Adversarial tests cover malformed TTL options, compound TTL attempts,
    unsupported value types, no-mutation-on-error, stale index-entry cleanup,
    and planner/count/read paths after expiration.
14. Benchmarks either add a representative TTL sweep budget or explicitly prove
    that existing CI benchmark budget still catches regressions.
15. README and `docs/mongodb-compatibility-uplifts-roadmap.md` accurately
    describe newly supported and still unsupported TTL behavior.
16. Verification commands pass locally.
17. Milestone checkboxes in this file are marked `[x]` as work completes.
18. Each completed milestone has a focused commit.

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

- [x] Milestone 0: TTL metadata model and migration-safe persistence
- [x] Milestone 1: `createIndexes` TTL validation and listing
- [x] Milestone 2: Deterministic TTL sweeper core
- [x] Milestone 3: Wire-visible read/write sweep integration
- [ ] Milestone 4: `collMod` TTL update subset
- [ ] Milestone 5: Adversarial PyMongo/spec-corpus coverage
- [ ] Milestone 6: Benchmarks, docs, scorecard, final verification

## Milestone 0: TTL Metadata Model And Migration-Safe Persistence

Problem:

- The existing index model has no place to store `expireAfterSeconds`.

Desired behavior:

- Extend `IndexSpec` with optional TTL metadata.
- Persist TTL metadata in SQLite in a way that works for existing databases.
- Ensure all existing index lifecycle helpers still load, compare, insert,
  list, drop, and rebuild indexes correctly.

Acceptance criteria:

- Existing tests pass without behavior changes for non-TTL indexes.
- Existing index comparison treats TTL metadata as part of the index
  specification, so conflicting duplicate index definitions are rejected.
- Existing persisted indexes without TTL metadata still deserialize as
  non-TTL.
- Milestone status is marked done in this file and committed.

Status:

- 2026-07-04: Completed TTL metadata model and migration-safe persistence.
  Verification passed: `cargo fmt -- --check`; `cargo test index`;
  `cargo test create_indexes`. Commit: `f331358`.

Likely files:

- `src/main.rs`
- `docs/ttl-index-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test index
cargo test create_indexes
```

## Milestone 1: `createIndexes` TTL Validation And Listing

Problem:

- `createIndexes` rejects `expireAfterSeconds`, and `listIndexes` cannot expose
  TTL metadata.

Desired behavior:

- Parse and validate `expireAfterSeconds` from index specs.
- Accept non-negative finite integer TTL seconds that fit the chosen internal
  representation. Accept BSON integer types; reject bools, strings, doubles
  unless a safe MongoDB-compatible numeric handling is explicitly tested,
  negative values, overflow values, arrays, documents, and null.
- Allow TTL only on one-field non-`_id` indexes with ordinary ascending or
  descending key direction.
- Decide explicitly whether TTL can combine with `sparse` and
  `partialFilterExpression`. If supported, prove behavior with tests; otherwise
  reject explicitly.
- Reject compound TTL indexes and unsupported key types.
- Include `expireAfterSeconds` in `listIndexes` only for TTL indexes.

Acceptance criteria:

- Rust tests cover valid TTL creation, autogenerated and explicit index names,
  list output, duplicate identical spec idempotency, duplicate conflicting TTL
  spec rejection, invalid value types, negative values, overflow values,
  compound keys, `_id`, and unsupported key directions.
- No invalid TTL creation leaves persisted index metadata or index entries.
- Milestone status is marked done in this file and committed.

Status:

- 2026-07-04: Completed TTL `createIndexes` parsing, validation, durable
  listing, duplicate-spec checks, and PyMongo metadata/error coverage.
  Verification passed: `cargo fmt -- --check`; `cargo test ttl`;
  `cargo test list_indexes`; `cargo test create_indexes`;
  `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest
  tests/e2e/test_indexes.py` (30 passed, outside sandbox due localhost bind).
  Commit: `c335a16`.

Likely files:

- `src/main.rs`
- `tests/e2e/test_indexes.py`
- `docs/ttl-index-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test ttl
cargo test list_indexes
cargo test create_indexes
```

## Milestone 2: Deterministic TTL Sweeper Core

Problem:

- TTL indexes need observable expiration, but a background thread would make
  tests flaky and complicate local execution.

Desired behavior:

- Add a deterministic TTL sweep helper scoped to one namespace.
- Use a testable clock boundary so Rust tests can assert behavior without
  sleeping. Production command paths can use current UTC time.
- For each TTL index in the namespace, identify documents whose indexed field
  has a BSON `DateTime` at or before the cutoff.
- Delete expired documents and all related `index_entries` and
  `index_multikey_omissions` atomically.
- Treat malformed/non-date field values as non-expiring.

Acceptance criteria:

- Rust tests cover:
  - expired date is deleted;
  - future date remains;
  - `expireAfterSeconds: 0` expiration boundary;
  - missing, null, string, number, array, and document values remain;
  - multiple TTL indexes expire a document if either TTL predicate matches;
  - TTL deletion removes all maintained index entries;
  - repeated sweeps are idempotent;
  - no TTL indexes means no document mutation.
- Sweep code is namespace-bounded and does not scan unrelated collections.
- Milestone status is marked done in this file and committed.

Status:

- 2026-07-04: Completed deterministic namespace-scoped TTL sweeper core with a
  testable clock, atomic document/index-entry deletion, repeated-sweep
  idempotency, multiple TTL index handling, and non-date/non-expiring safety.
  Verification passed: `cargo fmt -- --check`; `cargo test ttl`;
  `cargo test index_entries`. Commit: `f750870`.

Likely files:

- `src/main.rs`
- `docs/ttl-index-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test ttl
cargo test index_entries
```

## Milestone 3: Wire-Visible Read/Write Sweep Integration

Problem:

- A sweeper helper is not enough; real MongoDB clients need to observe expired
  documents disappearing through normal commands.

Desired behavior:

- Trigger namespace-scoped TTL sweeps before observable reads:
  - `find`;
  - `count`;
  - `aggregate`;
  - `distinct`;
  - index-hinted reads and planner pushdown reads.
- Trigger namespace-scoped TTL sweeps before document-producing writes that can
  observe or conflict with expired documents:
  - `insert`;
  - `update`;
  - `delete`;
  - findAndModify.
- Ensure a TTL sweep cannot partially delete data if the surrounding command
  later returns a validation, hint, or parse error.
- Keep command errors explicit for unsupported shapes.

Acceptance criteria:

- Rust tests prove expired documents do not appear through collection scans,
  index-narrowed `find`, pushed-down `count`, `aggregate` count shapes,
  `distinct`, updates, deletes, and findAndModify.
- Tests prove unique indexes can be reused after TTL removes the expired
  conflicting document.
- Tests prove hint errors and malformed commands do not run unintended
  unrelated sweeps.
- Milestone status is marked done in this file and committed.

Status:

- 2026-07-04: Completed read/write command-boundary TTL sweep integration for
  find, count, aggregate, distinct, insert, update, delete, and findAndModify,
  including planner/count paths, unique-index reuse after expiration, and no
  sweep on invalid read hints. Verification passed: `cargo fmt -- --check`;
  `cargo test ttl`; `cargo test count`; `cargo test hint`;
  `cargo test find_and_modify`; `cargo test update`; `cargo test delete`.
  Commit: pending.

Likely files:

- `src/main.rs`
- `docs/ttl-index-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test ttl
cargo test count
cargo test hint
cargo test find_and_modify
cargo test update
cargo test delete
```

## Milestone 4: `collMod` TTL Update Subset

Problem:

- Many MongoDB applications update TTL durations using `collMod`.
  `mongolino` currently rejects all index-related `collMod` options.

Desired behavior:

- Preserve existing validator `collMod` behavior exactly.
- Add support for:
  - `{"collMod": <collection>, "index": {"name": <indexName>, "expireAfterSeconds": <seconds>}}`
- Validate that the collection exists and the index exists.
- Validate the new TTL seconds with the same rules as `createIndexes`.
- Update only TTL metadata, not the key pattern or other index properties.
- Return a MongoDB-shaped success response with `ok: 1.0` and any small stable
  metadata that is useful and tested.
- Reject unsupported `collMod` index shapes explicitly:
  - missing `name`;
  - non-string or empty `name`;
  - missing `expireAfterSeconds`;
  - invalid TTL value;
  - unknown index;
  - `_id_` index;
  - key-pattern based updates unless fully implemented and tested;
  - attempts to set unsupported index options;
  - attempts to combine malformed validator and index updates that could leave
    partial state.

Acceptance criteria:

- Rust tests cover validator-only `collMod`, TTL-only `collMod`, combined valid
  validator plus TTL update if supported, and no-mutation behavior when any
  part of a combined command is invalid.
- PyMongo e2e tests cover TTL update by name and invalid update errors.
- Existing validation e2e tests are updated from "TTL collMod is unsupported"
  to the new narrower behavior.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_validation.py`
- `tests/e2e/test_indexes.py`
- `docs/ttl-index-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test coll_mod
cargo test ttl
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_validation.py tests/e2e/test_indexes.py
```

## Milestone 5: Adversarial PyMongo/Spec-Corpus Coverage

Problem:

- TTL compatibility is observable driver behavior, and unit-only coverage will
  miss wire, BSON, and PyMongo API details.

Desired behavior:

- Add e2e PyMongo tests that create TTL indexes through normal driver APIs,
  inspect `list_indexes`, insert expired and non-expired documents, and observe
  deterministic expiration through normal commands.
- Add or extend spec-corpus cases for TTL index creation, invalid TTL specs,
  `collMod` TTL updates, and unsupported index classes.
- Keep tests deterministic by using dates far enough in the past/future and by
  relying on command-path sweeps, not sleeps.

Acceptance criteria:

- PyMongo e2e covers:
  - `create_index("createdAt", expireAfterSeconds=N)`;
  - `list_indexes()` includes TTL metadata;
  - expired document disappears from `find`;
  - future document remains;
  - missing and non-date fields remain;
  - `expireAfterSeconds=0` boundary behavior;
  - `collMod` TTL update by index name;
  - invalid compound TTL and malformed TTL options return driver-visible
    command errors.
- Spec-corpus or Rust table tests cover unsupported text/geospatial/hashed/
  wildcard TTL combinations as explicit errors.
- Milestone status is marked done in this file and committed.

Likely files:

- `tests/e2e/test_indexes.py`
- `tests/e2e/test_spec_corpus.py`
- `tests/spec_corpus/*.json`
- `src/main.rs`
- `docs/ttl-index-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test ttl
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_indexes.py tests/e2e/test_spec_corpus.py
```

## Milestone 6: Benchmarks, Docs, Scorecard, Final Verification

Problem:

- The compatibility table and roadmap must describe the new TTL surface
  accurately, and the performance budget should guard against accidental broad
  scans.

Desired behavior:

- Update README compatibility rows for `collMod`, `find`/read paths if needed,
  `BSON storage`, and `Indexes`.
- Update `docs/mongodb-compatibility-uplifts-roadmap.md`:
  - mark uplift 3 complete;
  - move scorecard from **62%** to at least **68%**;
  - keep unsupported collation and broader index classes visible.
- Add or update benchmark coverage for TTL sweep overhead if practical. If no
  dedicated TTL benchmark is added, document why existing CI benchmark coverage
  is sufficient and ensure the full benchmark budget still passes.
- Run the full verification suite.

Acceptance criteria:

- README clearly says what TTL supports and what remains unsupported.
- Roadmap shows TTL uplift complete and next prompt target is Collation Support.
- Benchmark budget passes.
- Full Rust and PyMongo e2e suites pass.
- Milestone status is marked done in this file and committed.

Likely files:

- `README.md`
- `docs/mongodb-compatibility-uplifts-roadmap.md`
- `docs/performance-baseline.md`
- `src/bin/mongolino-bench.rs`
- `tests/e2e/test_indexes.py`
- `docs/ttl-index-goal-loop.md`

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
- any known residual TTL gaps or intentionally unsupported MongoDB behavior.

Do not call the goal complete if any milestone remains unchecked, verification
has not run, or README/roadmap docs still describe TTL as fully unsupported.
