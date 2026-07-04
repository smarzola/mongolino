# Goal: Application Compatibility Uplift

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to move `mongolino` from a CRUD-compatible prototype into a small MongoDB-like server that can support normal application setup and iteration workflows through real drivers. This is a substantial compatibility uplift, but it must be delivered as atomic, independently verifiable milestones with checkpoint commits.

Focus on the behavior that makes simple applications and test suites feel natural: real server-side cursors, collection/database lifecycle commands, metadata commands, index API compatibility, simple unique index enforcement, and PyMongo/spec-corpus coverage. Keep the server in Rust, keep durable state in SQLite, and keep unsupported behavior explicit.

## Repository Rules

Follow `AGENTS.md`.

Important reminders:

- Keep the server implementation in Rust and the durable storage layer in SQLite.
- Treat MongoDB wire compatibility as observable behavior: validate changes with real client handshakes when possible.
- Run `cargo fmt` and `cargo test` before handing off code changes.
- Keep unsupported MongoDB commands explicit by returning command errors instead of silently accepting behavior.
- Use the existing PyMongo e2e suite for real driver verification.
- Do not use Docker or external MongoDB services for this goal.
- Use `uv` for Python tooling.
- Do not revert unrelated user changes.
- Prefer the repo's existing patterns and docs style, but split `src/main.rs` into modules if the implementation becomes hard to reason about.
- If e2e tests expose a real product gap in the documented subset, fix the product rather than weakening the test.
- At the end of each milestone, run verification, mark the milestone done in this file, commit the completed milestone, and report the commit hash before continuing.

## Target State

By the end, `mongolino` should support enough application-level MongoDB behavior that a simple PyMongo app can:

- create and list collections;
- insert, query, update, delete, count, and distinct values;
- iterate result sets across multiple batches with `getMore`;
- explicitly close cursors with `killCursors` or normal exhaustion;
- drop collections and databases for test cleanup;
- create, list, and drop basic indexes;
- enforce simple unique indexes durably across insert, update, and upsert;
- run the expanded PyMongo e2e suite and local spec corpus in CI.

The implementation should remain honest:

- Cursors can be in-memory and per connection for this goal.
- Cursor results can be snapshot-at-find-time.
- Indexes can support only simple ascending/descending single-field and compound metadata initially, with query acceleration limited to simple cases.
- Unsupported index options, cursor options, collection options, and commands must return explicit errors.
- No auth, transactions, retryable writes, replica sets, aggregation, geospatial/text indexes, collation, or server-side JavaScript in this goal.

## Current State

The repo currently has:

- Rust server implementation in `src/main.rs`.
- SQLite-backed BSON document storage keyed by `(namespace, id_key)`.
- MongoDB wire handling for `OP_MSG` and limited `OP_QUERY`.
- Handshake commands: `hello`, `isMaster`, `ping`, `buildInfo`, `listDatabases`, `endSessions`.
- CRUD commands: `insert`, `find`, `update`, `delete`.
- `find` matching for documented operators and projections/sort/skip/limit.
- PyMongo/pytest e2e tests under `tests/e2e`.
- Local JSON spec-inspired corpus under `tests/spec_corpus`.
- GitHub Actions CI running Rust checks and PyMongo e2e.

Important gaps:

- `find` always returns cursor id `0` and closes immediately.
- There is no `getMore` or `killCursors` command.
- `batchSize` smaller than remaining matches drops the rest instead of allowing iteration.
- Empty collections are not tracked because database and collection names are derived from stored document namespaces.
- There is no `create`, `drop`, `dropDatabase`, or `listCollections`.
- There is no `count`, `countDocuments` command path, or `distinct`.
- `createIndexes`, `listIndexes`, and `dropIndexes` are unsupported.
- No unique index beyond `_id` is enforced.
- No query planner uses secondary indexes.

## Definition Of Done

The goal is complete only when:

1. `find` returns nonzero cursor ids when additional documents remain and `getMore` returns subsequent `nextBatch` results.
2. PyMongo cursor iteration with small `batch_size` returns all expected documents.
3. `killCursors` explicitly closes live cursors and reports killed/not-found cursor ids.
4. Invalid cursor ids, namespace mismatches, exhausted cursors, malformed `getMore`, and malformed `killCursors` return documented explicit errors or MongoDB-like responses.
5. Collections and databases have durable catalog state independent of whether documents exist.
6. `create`, `listCollections`, `drop`, and `dropDatabase` work through PyMongo where possible.
7. `listDatabases` includes empty databases/collections where catalog state requires it.
8. `count` or the command path used by PyMongo count helpers works for the documented filter subset.
9. `distinct` works for scalar, dotted path, and array values in the documented matcher subset.
10. `createIndexes`, `listIndexes`, and `dropIndexes` work for the documented simple index subset.
11. Unique indexes on simple supported fields are enforced on insert, update, and upsert, including preexisting data validation during index creation.
12. Simple index metadata is stored durably in SQLite.
13. Query execution uses supported index metadata where practical for `_id` and simple equality/range filters, with regression tests proving correctness before performance claims.
14. README compatibility tables and notes are updated accurately.
15. PyMongo e2e tests and local spec corpus cover every new supported command and adversarial path.
16. `cargo fmt -- --check`, `cargo test`, `cargo build`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`, and `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e` pass locally. Use unsandboxed execution for localhost binding if needed.
17. Milestone checkboxes in this file are marked `[x]` as work completes.
18. Each completed milestone has a focused commit.
19. Final verification commands pass or any unrelated/environmental failures are documented with evidence.

## Milestone Checklist

When a milestone is complete:

1. Run the milestone's verification commands.
2. Update this checklist by changing the milestone from `[ ]` to `[x]`.
3. Add a short status note under that milestone with the date, exact commands run, and commit hash if available.
4. Commit the code, tests, docs, and checklist/status update with a focused commit message.
5. Report the commit hash in the goal-loop status before starting the next milestone.

- [x] Milestone 0: Baseline and architecture preparation
  - Status 2026-07-04: Baseline verification before edits: `cargo fmt -- --check` passed; `cargo test` passed (39 tests); `cargo build` passed; sandboxed `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e` failed at localhost bind with `PermissionError: [Errno 1] Operation not permitted`; rerun outside the sandbox passed (40 passed, 1 skipped). Added per-client command context scaffolding, migration helper scaffolding, and tests pinning the previous closed-cursor batch behavior. Milestone verification after edits: `cargo fmt -- --check` passed; `cargo test` passed (41 tests); `cargo build` passed with one existing-style dead-code warning for the test-only `handle_command` wrapper; unsandboxed `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e` passed (40 passed, 1 skipped). Commit hash reported after commit.
- [x] Milestone 1: Server-side cursor state and `getMore`
  - Status 2026-07-04: Implemented per-client in-memory cursor state, positive cursor ids for remaining `find` batches, `getMore` with `nextBatch`, exhaustion cleanup, Rust cursor/getMore tests, PyMongo batch iteration/getMore tests, local cursor corpus coverage, and README cursor notes. Verification: `cargo fmt -- --check` passed; `cargo test cursor` passed (1 test); `cargo test get_more` passed (2 tests); `cargo test` passed (43 tests); `cargo build` passed with dead-code warnings for test wrappers; unsandboxed `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e` passed (42 passed, 1 skipped). Commit hash reported after commit.
- [ ] Milestone 2: Cursor lifecycle hardening and `killCursors`
- [ ] Milestone 3: Durable collection catalog and lifecycle commands
- [ ] Milestone 4: Count, distinct, and metadata commands
- [ ] Milestone 5: Index catalog and index command compatibility
- [ ] Milestone 6: Unique index enforcement
- [ ] Milestone 7: Simple index-aware query execution
- [ ] Milestone 8: PyMongo/spec corpus expansion and docs

## Milestone 0: Baseline and Architecture Preparation

Problem:

- The current implementation is concentrated in `src/main.rs`.
- Cursors require per-client mutable state, while current command handling mostly takes only a SQLite connection and a command document.
- Later catalog and index work needs schema migrations and helpers before behavior changes become safe.

Desired behavior:

- Establish a clear baseline and make the smallest architecture changes needed to support later milestones.
- Keep behavior unchanged except for internal structure and additional tests.

Acceptance criteria:

- Confirm current baseline with:
  - `cargo fmt -- --check`;
  - `cargo test`;
  - `cargo build`;
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e`.
- Add or update tests that pin current closed-cursor behavior before changing it.
- Introduce a per-client command context if needed, for example a `ClientState` that owns cursor state and is passed through `serve_client`.
- Keep current command behavior unchanged in this milestone.
- Add database initialization migration helpers for future catalog/index tables without changing current storage semantics.
- If splitting files, keep modules small and obvious, such as wire, commands, storage, matcher, cursor, and tests. Do not perform a broad style rewrite.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- Optional new `src/*.rs` modules
- `tests/e2e/test_crud.py`
- `tests/e2e/test_spec_corpus.py`
- `docs/application-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test
cargo build
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 1: Server-Side Cursor State and `getMore`

Problem:

- `find` currently returns all selected results in `firstBatch` up to `batchSize` and closes with cursor id `0`.
- Real drivers expect `getMore` when more results remain.

Desired behavior:

- `find` snapshots matched and shaped results, returns `firstBatch`, and stores remaining documents under a nonzero cursor id when more remain.
- `getMore` returns `nextBatch` for a live cursor and closes the cursor when exhausted.
- PyMongo iteration over a cursor with small `batch_size` returns all documents.

Acceptance criteria:

- Implement in-memory per-client cursor registry with monotonically generated positive cursor ids.
- Store cursor namespace, remaining BSON documents, and current position or queue.
- `find` respects `batchSize`, `limit`, `skip`, sort, and projection when determining first and remaining batches.
- `find` returns cursor id `0` when no results remain after `firstBatch`.
- `find` returns nonzero cursor id when remaining results exist.
- Implement `getMore` command shape used by PyMongo:
  - `{ getMore: <cursor_id>, collection: <collection>, batchSize: <int optional>, "$db": <db> }`.
- `getMore` response uses `cursor.id`, `cursor.ns`, and `cursor.nextBatch`.
- `getMore` validates cursor id type, collection name, namespace, and batch size.
- Negative `batchSize` and unsupported `getMore` options return explicit command errors.
- Cursor ids are not reused within a client connection during tests.
- Add Rust tests for cursor registry and command responses.
- Add PyMongo e2e tests:
  - `list(collection.find({}).sort("_id", 1).batch_size(1))` returns all documents;
  - `limit` smaller than total closes after the limit;
  - `batch_size` larger than remaining closes after next batch.
- Update local spec corpus with a cursor iteration case.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- Optional `src/cursor.rs`
- `tests/e2e/test_crud.py`
- `tests/spec_corpus/*.json`
- `README.md`
- `docs/application-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test cursor
cargo test get_more
cargo test
cargo build
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 2: Cursor Lifecycle Hardening and `killCursors`

Problem:

- Cursor state needs explicit close behavior and adversarial coverage.
- Drivers may issue `killCursors`, and tests need to prove resources are not left live indefinitely.

Desired behavior:

- Exhausted cursors are removed.
- `killCursors` removes live cursors and reports killed or not found ids.
- Bad cursor operations are explicit and do not corrupt state.

Acceptance criteria:

- Implement `killCursors` command:
  - `{ killCursors: <collection>, cursors: [<cursor_ids>], "$db": <db> }`.
- Return MongoDB-like fields for the documented subset:
  - `cursorsKilled`;
  - `cursorsNotFound`;
  - `cursorsAlive`;
  - `cursorsUnknown`;
  - `ok`.
- Validate malformed `cursors`, non-integer ids, wrong collection names, and unsupported options.
- Decide and document behavior for `getMore` after exhaustion and after kill. Prefer explicit command errors for invalid live cursor access unless PyMongo requires a not-found cursor response shape.
- Add tests for:
  - manual `killCursors`;
  - repeated kill;
  - bogus cursor id;
  - namespace mismatch;
  - getMore after kill;
  - getMore after exhaustion.
- Add PyMongo e2e tests that explicitly close a cursor and ensure subsequent behavior is sane.
- Keep per-client cursor scope clear. If cross-connection `getMore` is unsupported, return an explicit error and document it.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- Optional `src/cursor.rs`
- `tests/e2e/test_crud.py`
- `tests/e2e/test_errors.py`
- `tests/spec_corpus/*.json`
- `README.md`
- `docs/application-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test cursor
cargo test kill
cargo test
cargo build
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 3: Durable Collection Catalog and Lifecycle Commands

Problem:

- Collections and databases are currently inferred only from documents.
- Empty collections cannot exist.
- PyMongo tests cannot use normal cleanup workflows such as `drop`.

Desired behavior:

- SQLite stores durable catalog metadata for databases and collections.
- `create`, `listCollections`, `drop`, and `dropDatabase` work for the documented subset.
- Existing document namespaces are migrated or surfaced consistently through the catalog.

Acceptance criteria:

- Add catalog tables, for example:
  - `databases` or derive databases from collections;
  - `collections(namespace, db, name, created_at, options_json_or_bson)`.
- On insert/update/upsert into a namespace, ensure collection catalog entry exists.
- Existing document-only namespaces appear in `listCollections` and `listDatabases`.
- Implement `create` command for basic collection creation.
- Implement `listCollections` command with a cursor response that PyMongo can consume.
- Implement `drop` command for a collection:
  - removes documents;
  - removes collection catalog entry;
  - removes indexes for that collection in later milestones or prepares the table for it.
- Implement `dropDatabase`:
  - removes collections and documents for the database;
  - leaves other databases untouched.
- Preserve `_id` primary-key behavior.
- Reject unsupported collection options explicitly, such as validators, capped collections, timeseries, clustered indexes, or collation.
- Add Rust tests for catalog creation, migration from document namespace, drop, and dropDatabase.
- Add PyMongo e2e tests for:
  - `database.create_collection`;
  - `database.list_collection_names`;
  - empty collection appears in list;
  - `collection.drop`;
  - `database.drop_collection`;
  - `client.drop_database` or command path.
- Update local spec corpus with lifecycle cases.
- Update README compatibility table.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- Optional `src/catalog.rs` or `src/storage.rs`
- `tests/e2e/test_handshake.py`
- `tests/e2e/test_crud.py`
- New `tests/e2e/test_lifecycle.py`
- `tests/spec_corpus/*.json`
- `README.md`
- `docs/application-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test catalog
cargo test drop
cargo test
cargo build
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 4: Count, Distinct, and Metadata Commands

Problem:

- Applications commonly use count, distinct, and metadata helpers.
- Without these commands, simple admin and assertion flows need custom find logic.

Desired behavior:

- PyMongo helpers for counting and distinct values work against the documented matcher subset.
- Metadata commands return useful, honest responses.

Acceptance criteria:

- Determine the command shapes PyMongo sends for:
  - `collection.count_documents(filter)`;
  - `collection.estimated_document_count()` if feasible;
  - `collection.distinct(field, filter)`.
- Implement the command paths needed for the supported helpers.
- `count` or aggregate fallback support should be explicit. Do not implement broad aggregation in this milestone unless PyMongo requires a tiny command shape that can be safely supported and documented.
- `count` respects the documented filter matcher and namespace.
- If supporting skip/limit for count, validate and test it; otherwise reject unsupported options.
- `distinct` supports:
  - top-level fields;
  - dotted paths;
  - array value expansion consistent with existing matcher behavior;
  - optional query filter.
- `distinct` returns unique values with deterministic ordering if practical; document ordering if not MongoDB-equivalent.
- Reject unsupported options such as collation, readConcern, hint, maxTimeMS, or comment unless harmless and documented.
- Improve `listDatabases` and `listCollections` size/count placeholders only if it can be done cheaply and honestly.
- Add PyMongo e2e tests for count and distinct happy paths plus unsupported options.
- Add local spec corpus cases for count and distinct.
- Update README.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- Optional `src/commands.rs`
- `tests/e2e/test_lifecycle.py`
- `tests/e2e/test_errors.py`
- `tests/spec_corpus/*.json`
- `README.md`
- `docs/application-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test count
cargo test distinct
cargo test
cargo build
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 5: Index Catalog and Index Command Compatibility

Problem:

- `createIndexes`, `listIndexes`, and `dropIndexes` are currently unsupported.
- Many applications create indexes during startup even before relying on query planner behavior.

Desired behavior:

- Basic index commands work and persist index metadata in SQLite.
- `_id_` index is always listed.
- Unsupported index features are rejected explicitly.

Supported subset:

- Index key specs with one or more fields and direction `1` or `-1`.
- Name auto-generation or explicit `name`.
- `unique: true` metadata, with enforcement completed in Milestone 6.
- `createIndexes`, `listIndexes`, `dropIndexes`.

Unsupported for this milestone:

- Text indexes.
- Geospatial indexes.
- Hashed indexes.
- Wildcard indexes.
- Partial indexes.
- Sparse indexes unless explicitly implemented and tested.
- Collation.
- TTL expiration behavior.
- Hidden indexes.
- Background build semantics.

Acceptance criteria:

- Add durable index catalog table, for example:
  - namespace;
  - index name;
  - key spec encoded as BSON or JSON;
  - unique flag;
  - created_at.
- Ensure every collection has virtual or cataloged `_id_` index.
- Implement `createIndexes`:
  - validates collection and indexes array;
  - creates collection catalog entry if needed, if MongoDB-compatible enough;
  - rejects duplicate index name conflicts;
  - rejects conflicting existing index specs;
  - validates key directions;
  - rejects unsupported options explicitly.
- Implement `listIndexes` with cursor response consumable by PyMongo.
- Implement `dropIndexes` for one named index and all non-`_id_` indexes where supported.
- Never allow dropping `_id_`.
- Add Rust tests and PyMongo e2e tests for:
  - creating an index;
  - listing `_id_` and created indexes;
  - duplicate index creation idempotence or conflict behavior;
  - dropping an index;
  - unsupported index options returning explicit errors.
- Add local spec corpus index cases.
- Update README compatibility table.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- Optional `src/indexes.rs`
- `tests/e2e/test_indexes.py`
- `tests/e2e/test_errors.py`
- `tests/spec_corpus/*.json`
- `README.md`
- `docs/application-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test index
cargo test
cargo build
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 6: Unique Index Enforcement

Problem:

- Index command compatibility without unique enforcement can mislead applications.
- Unique indexes are one of the most user-visible index semantics after `_id`.

Desired behavior:

- Unique indexes on supported simple field specs are enforced on insert, update, and upsert.
- Creating a unique index fails if existing data violates it.
- Violations return duplicate key write errors and preserve existing data.

Acceptance criteria:

- Define the supported unique key encoding for indexed values:
  - scalar values;
  - null and missing field semantics;
  - dotted paths;
  - arrays, either supported with documented semantics or explicitly rejected for unique indexes.
- On `createIndexes` with `unique: true`, scan existing documents and reject duplicates.
- Enforce unique indexes during:
  - `insert`;
  - replacement update;
  - modifier update;
  - upsert.
- Ordered/unordered bulk behavior remains correct when unique index violations occur.
- Duplicate errors include code `11000` and useful index/namespace context.
- Dropping a unique index removes its enforcement.
- Dropping a collection or database removes its indexes.
- Add Rust tests and PyMongo e2e tests for:
  - unique index creation success;
  - creation failure on duplicate existing data;
  - insert duplicate failure;
  - update duplicate failure preserving original documents;
  - upsert duplicate failure;
  - unordered bulk partial success with unique violation;
  - drop index then duplicate insert allowed if no other unique constraint blocks it.
- Add local spec corpus unique-index cases.
- Update README.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- Optional `src/indexes.rs`
- `tests/e2e/test_indexes.py`
- `tests/spec_corpus/*.json`
- `README.md`
- `docs/application-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test unique
cargo test index
cargo test
cargo build
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 7: Simple Index-Aware Query Execution

Problem:

- Index metadata improves API compatibility, but applications eventually expect indexed equality/range queries to avoid full scans.
- This milestone should make conservative planner improvements without overclaiming MongoDB planner behavior.

Desired behavior:

- Query execution uses supported index metadata when it is clearly correct.
- Behavior remains identical to matcher-based scans.

Acceptance criteria:

- Add internal index storage if needed to make lookup practical, for example a SQLite table mapping namespace/index/value/document id.
- Maintain index entries on insert, update, upsert, delete, drop, and dropDatabase.
- Use index-assisted lookup for:
  - `_id` equality, preserving the existing fast path;
  - simple equality on indexed scalar fields;
  - simple range on indexed numeric/string fields only if semantics are clear and tested.
- If compound indexes are stored but not used by the planner, document that.
- Add internal tests proving indexed and full-scan results match for supported filters.
- Add adversarial tests for stale index entries after update/delete/drop.
- Add PyMongo e2e tests that use `create_index`, mutate data, and verify correct query results.
- Do not add performance benchmarks unless they are cheap and deterministic.
- If adding any performance claim, include a small directional benchmark or avoid the claim.
- Update README to say indexes are used for a conservative subset only.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- Optional `src/indexes.rs`
- Optional `src/storage.rs`
- `tests/e2e/test_indexes.py`
- `README.md`
- `docs/application-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test planner
cargo test index
cargo test
cargo build
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 8: PyMongo/Spec Corpus Expansion and Docs

Problem:

- A substantial compatibility uplift is incomplete without real-driver regression coverage and accurate docs.
- The local corpus should track the supported application workflow, not only CRUD.

Desired behavior:

- PyMongo e2e and local spec corpus cover the entire new documented subset.
- README accurately explains what works, what remains partial, and what is unsupported.
- CI protects the new compatibility surface.

Acceptance criteria:

- Expand `tests/e2e` coverage for:
  - cursor iteration;
  - getMore command where directly useful;
  - killCursors command;
  - collection create/list/drop;
  - database drop;
  - count;
  - distinct;
  - create/list/drop indexes;
  - unique index enforcement;
  - indexed query correctness after mutation.
- Expand `tests/spec_corpus` with local JSON cases for the same features.
- Keep unsupported regex/text/geospatial/transactions/auth cases skipped or explicitly error-tested with named reasons.
- Update README compatibility table rows for:
  - `find`;
  - Cursors;
  - `getMore`;
  - `killCursors`;
  - collection lifecycle;
  - database lifecycle;
  - count/distinct;
  - indexes;
  - BSON storage/query planning.
- Add concise development notes for running targeted e2e subsets if useful.
- Run final verification.
- Milestone status is marked done in this file and committed.

Likely files:

- `tests/e2e/*.py`
- `tests/spec_corpus/*.json`
- `README.md`
- `docs/application-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test
cargo build
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Suggested Parallelization

If using sub-agents, keep write ownership disjoint:

- Cursor worker: cursor state, `getMore`, `killCursors`, cursor tests.
- Catalog worker: collection/database catalog, lifecycle commands, lifecycle tests.
- Index worker: index catalog, unique enforcement, index tests.
- E2E worker: PyMongo/spec corpus expansion after each product milestone lands.

The main thread should own integration, conflict resolution, final docs consistency, and adversarial review. Do not let multiple workers rewrite the same module at the same time unless the code has already been split into stable ownership boundaries.

## Final Verification

Before the goal is complete, run:

```bash
cargo fmt -- --check
cargo test
cargo build
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

If localhost binding fails inside a sandbox with `Operation not permitted`, rerun the PyMongo e2e command with normal/unsandboxed localhost permissions and record that exact fact in the status note.

## Final Response Required

When complete, report:

- target state achieved or not achieved;
- commits made, with hashes;
- files changed;
- exact verification commands run and results;
- CI status if available;
- known residual risks or follow-up issues.
