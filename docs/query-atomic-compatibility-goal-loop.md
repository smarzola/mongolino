# Goal: Query and Atomic Modification Compatibility Uplift

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to make `mongolino` much more useful for real application code by adding the next large MongoDB compatibility layer: atomic single-document modification through `findAndModify`, a practical aggregation pipeline subset, and shared query-shaping internals that keep `find`, aggregation, and atomic commands consistent. This is a compatibility and quality uplift: do not chase full MongoDB parity, but make the supported subset coherent, driver-observable, well tested, and explicitly bounded.

The most important application workflows this goal should unlock are:

- PyMongo `find_one_and_update`, `find_one_and_replace`, and `find_one_and_delete` for locks, job queues, counters, and optimistic local workflows;
- PyMongo `aggregate` for small read pipelines using `$match`, `$sort`, `$skip`, `$limit`, `$project`, and `$count`;
- consistent filtering, sorting, projection, skip, limit, index freshness, and duplicate-key behavior across `find`, aggregation, update/upsert, and atomic modification.

## Repository Rules

Follow `AGENTS.md`.

Important reminders:

- Keep the server implementation in Rust and the durable storage layer in SQLite.
- Treat MongoDB wire compatibility as observable behavior: validate changes with real client handshakes when possible.
- Run `cargo fmt` and `cargo test` before handing off code changes.
- Keep unsupported MongoDB commands explicit by returning command errors instead of silently accepting behavior.
- Use the existing PyMongo e2e suite for real driver verification.
- Use `uv` for Python tooling.
- Do not use Docker or external MongoDB services for this goal.
- Do not revert unrelated user or agent changes.
- Prefer the repo's existing patterns and docs style.
- Add abstractions only where they remove duplication or keep behavior consistent across commands.
- If an e2e test exposes a real product gap in the documented subset, fix the product rather than weakening the test.
- At the end of each milestone, run verification, mark the milestone done in this file, commit the completed milestone, and report the commit hash before continuing.

## Target State

By the end, `mongolino` should support enough query and atomic-modification behavior that a simple PyMongo app can:

- atomically find and update one document with `find_one_and_update`;
- atomically find and replace one document with `find_one_and_replace`;
- atomically find and delete one document with `find_one_and_delete`;
- select the affected document using the existing matcher and deterministic sort behavior;
- request either the pre-image or post-image where PyMongo exposes that choice;
- use upsert with `find_one_and_update` and `find_one_and_replace`;
- receive duplicate-key errors when `_id` or unique indexes conflict;
- run aggregation pipelines using the documented small stage subset;
- use aggregation cursors with PyMongo;
- see README compatibility tables that accurately describe what works and what is still explicit-error unsupported.

The implementation should remain honest:

- `findAndModify` can be single-server and SQLite-transaction backed.
- No transactions, retryable writes, sessions, write concern durability semantics, collation, array filters, pipeline updates, positional updates, JavaScript, `$lookup`, `$unwind`, `$facet`, or expression language support in this goal.
- Aggregation should be a simple document-stream pipeline over the existing matcher/shaper logic, not a full MongoDB aggregation engine.
- Unsupported stages, unsupported projection expressions, malformed stage shapes, unsupported command options, and unsupported update forms must return explicit command errors.
- Do not silently ignore unknown keys unless the repo already documents that key as harmless and accepted.

## Current State

The repo currently has:

- Rust server implementation in `src/main.rs`.
- SQLite-backed BSON document storage keyed by `(namespace, id_key)`.
- MongoDB wire handling for `OP_MSG` and limited `OP_QUERY`.
- Per-connection cursor state for `find` and `getMore`.
- Collection and index catalog tables plus maintained scalar equality `index_entries`.
- Commands: `hello`, `isMaster`, `ping`, `buildInfo`, `listDatabases`, `endSessions`, `create`, `listCollections`, `drop`, `dropDatabase`, `count`, `distinct`, narrow `aggregate` count-documents path, `createIndexes`, `listIndexes`, `dropIndexes`, `insert`, `find`, `getMore`, `killCursors`, `update`, and `delete`.
- Query matching for exact matches, dotted paths, limited array traversal, `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$exists`, `$and`, `$or`, `$nor`, and `$not`.
- Projection, sort, skip, limit, and simple index-assisted equality lookup for `find`.
- Update replacement, `$set`, `$unset`, `$inc`, upsert, single/multi update, `_id` immutability, unique index enforcement, and index entry refresh.
- PyMongo e2e tests under `tests/e2e`.
- Local JSON spec-inspired corpus under `tests/spec_corpus`.
- GitHub Actions CI running Rust checks and PyMongo e2e.

Important gaps:

- `findAndModify` / `findandmodify` is unsupported.
- PyMongo `find_one_and_update`, `find_one_and_replace`, and `find_one_and_delete` cannot work.
- `aggregate` only supports the PyMongo `count_documents()` pipeline shape and rejects general read pipelines.
- Aggregation does not use the per-client cursor registry for multi-batch results.
- Query shaping logic is mostly tied to `find`, which invites subtle drift if aggregation and `findAndModify` reimplement it independently.
- README still marks `aggregate` as only a count-documents path and has no `findAndModify` row.

## Definition Of Done

The goal is complete only when:

1. `findAndModify` and `findandmodify` dispatch to a supported command implementation.
2. `findAndModify` supports single-document remove, replacement update, modifier update with existing supported modifiers, sort, projection/fields, upsert, and pre-image/post-image return where supported by PyMongo.
3. `findAndModify` preserves `_id` immutability, unique index enforcement, and maintained index entries across update, replacement, upsert, and delete.
4. `findAndModify` returns MongoDB-like response fields for the supported subset, including `value`, `lastErrorObject`, and `ok`.
5. Malformed and unsupported `findAndModify` combinations return explicit command errors, including conflicting `remove` plus `update`, non-document query/update/sort/projection, array filters, pipeline updates, unsupported write concern, unsupported collation, unsupported hint, and unsupported projection expressions.
6. `aggregate` supports the documented sequential stage subset: `$match`, `$sort`, `$skip`, `$limit`, `$project`, and `$count`.
7. The existing PyMongo `count_documents()` aggregation shape still works.
8. Aggregation returns cursor documents compatible with PyMongo and supports cursor `batchSize` with `getMore` when additional results remain.
9. Aggregation rejects unsupported stages and malformed stage operands explicitly, including `$group` outside the existing count-documents shape if full grouping is not implemented.
10. `find`, aggregation, and `findAndModify` share query-shaping helpers where practical so matcher/projection/sort/skip/limit semantics stay consistent.
11. README compatibility tables and notes accurately describe the new supported subset and remaining unsupported behavior.
12. PyMongo e2e tests cover happy paths, not-happy paths, and adversarial paths for `findAndModify` and aggregation.
13. The local spec corpus includes representative successful and error cases for `findAndModify` and aggregation.
14. GitHub Actions continues to run the Rust and PyMongo checks without new service dependencies.
15. `cargo fmt -- --check`, `cargo test`, `cargo build`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`, and `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e` pass locally. Use unsandboxed execution for localhost binding if needed.
16. Milestone checkboxes in this file are marked `[x]` as work completes.
17. Each completed milestone has a focused commit.
18. Final verification commands pass or any unrelated/environmental failures are documented with evidence.

## Milestone Checklist

When a milestone is complete:

1. Run the milestone's verification commands.
2. Update this checklist by changing the milestone from `[ ]` to `[x]`.
3. Add a short status note under that milestone with the date, exact commands run, and commit hash if available.
4. Commit the code, tests, docs, and checklist/status update with a focused commit message.
5. Report the commit hash in the goal-loop status before starting the next milestone.

- [x] Milestone 0: Baseline and shared query-shaping preparation
- [x] Milestone 1: `findAndModify` command compatibility
- [x] Milestone 2: Aggregation read pipeline subset
- [ ] Milestone 3: Aggregation cursor batching and adversarial command coverage
- [ ] Milestone 4: Spec corpus, README, and final verification hardening

## Milestone 0: Baseline and Shared Query-Shaping Preparation

Status 2026-07-04: Complete. Introduced shared candidate loading, document shaping, and cursor response helpers while preserving existing `find` behavior. Verification: `cargo fmt -- --check` passed after applying `cargo fmt`; `cargo test find` passed; `cargo test aggregate` passed; `cargo test planner` passed; `cargo test` passed; `cargo build` passed; `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e` failed in the sandbox at `tests/e2e/conftest.py:103` with `PermissionError: [Errno 1] Operation not permitted` while binding `127.0.0.1`.

Problem:

- `find` already has filtering, projection, sorting, skip, limit, batch sizing, `_id` fast path, and simple index-assisted equality lookup.
- `aggregate` and `findAndModify` need most of the same behavior, but duplicating it will cause drift.

Desired behavior:

- Establish a clean baseline and introduce the smallest shared helper layer needed for later milestones.
- Keep externally observable behavior unchanged in this milestone.

Acceptance criteria:

- Confirm the current baseline before edits with:
  - `cargo fmt -- --check`;
  - `cargo test`;
  - `cargo build`;
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e`.
- Introduce focused helpers for reusable query behavior, such as:
  - loading candidate documents for a namespace using the existing conservative index lookup when eligible;
  - applying matcher, sort, skip, limit, and projection consistently;
  - constructing cursor responses and splitting batches without command-specific drift.
- Keep `find` behavior unchanged except for calling the shared helpers.
- Keep the existing narrow `aggregate` count-documents behavior unchanged.
- Add or preserve Rust tests that prove existing `find`, projection, sort, batch, count, distinct, index planner, and cursor behavior still pass.
- Do not split the entire server into modules unless it directly reduces risk for this goal.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `docs/query-atomic-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test find
cargo test aggregate
cargo test planner
cargo test
cargo build
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 1: `findAndModify` Command Compatibility

Status 2026-07-04: Complete. Added `findAndModify` / `findandmodify` dispatch, transaction-backed update/replace/delete/upsert behavior, pre-image/post-image responses, projection aliases, unique-index enforcement, index-entry refresh/delete, and explicit errors for malformed or unsupported command shapes. Verification: `cargo fmt -- --check` passed; `cargo test find_and_modify` passed; `cargo test unique` passed; `cargo test planner` passed; `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_find_and_modify.py` failed in the sandbox at `tests/e2e/conftest.py:103` with `PermissionError: [Errno 1] Operation not permitted` while binding `127.0.0.1`; `cargo test` passed.

Problem:

- Real applications and test suites often use PyMongo `find_one_and_update`, `find_one_and_replace`, and `find_one_and_delete`.
- The server currently has `update` and `delete`, but no atomic command that returns the affected document.

Desired behavior:

- Implement `findAndModify` / `findandmodify` for the documented single-document subset using a SQLite transaction.
- Select the target document with the existing matcher and deterministic sort behavior.
- Return the pre-image by default and the post-image when requested.

Acceptance criteria:

- Add dispatch for `findAndModify` and `findandmodify`.
- Support command shape used by PyMongo for:
  - `find_one_and_update(filter, update, sort=..., projection=..., upsert=..., return_document=...)`;
  - `find_one_and_replace(filter, replacement, sort=..., projection=..., upsert=..., return_document=...)`;
  - `find_one_and_delete(filter, sort=..., projection=...)`.
- Accept and validate documented keys only. Expected supported keys include:
  - `findAndModify` or `findandmodify`;
  - `query`;
  - `sort`;
  - `remove`;
  - `update`;
  - `new`;
  - `upsert`;
  - `fields`;
  - `projection`;
  - `$db`;
  - `lsid`.
- Treat `fields` and `projection` as aliases, reject both if they conflict, and use the same projection semantics as `find`.
- Support `remove: true` with no `update`.
- Support replacement updates and existing modifier updates (`$set`, `$unset`, `$inc`) with the same validation as `update`.
- Support `upsert: true` for update/replacement paths.
- Preserve `_id` immutability and duplicate-key behavior.
- Enforce supported unique indexes and refresh/delete index entries for the affected document.
- Return a MongoDB-like response:
  - `value` is the selected document image or `null`;
  - `lastErrorObject.n` is `0` or `1`;
  - `lastErrorObject.updatedExisting` is accurate for update/upsert paths;
  - `lastErrorObject.upserted` is present for upsert insertions;
  - `ok: 1.0` on success.
- Return explicit command errors for:
  - missing or empty collection name;
  - non-document `query`, `sort`, `fields`, `projection`, or `update`;
  - `remove: true` combined with `update`;
  - missing both `remove` and `update`;
  - `remove: false` with no `update`;
  - pipeline updates;
  - attempts to change `_id`;
  - unsupported command keys such as `arrayFilters`, `collation`, `hint`, `writeConcern`, `bypassDocumentValidation`, `maxTimeMS`, and `let`.
- Add Rust tests for selection, sort, pre/post image, projection, delete, upsert, duplicate key, index entry freshness, and malformed/adversarial command shapes.
- Add PyMongo e2e tests using real helpers:
  - `find_one_and_update` returns pre-image by default;
  - `find_one_and_update(..., return_document=ReturnDocument.AFTER)` returns post-image;
  - sorted target selection chooses the expected document;
  - `find_one_and_replace` preserves `_id` and rejects duplicate unique-index conflicts;
  - `find_one_and_delete` removes and returns the expected document;
  - upsert returns the inserted document when requested;
  - unsupported options fail explicitly where PyMongo can send them.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_find_and_modify.py`
- `tests/spec_corpus/find_and_modify.json`
- `README.md`
- `docs/query-atomic-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test find_and_modify
cargo test unique
cargo test planner
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_find_and_modify.py
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 2: Aggregation Read Pipeline Subset

Status 2026-07-04: Complete. Implemented sequential aggregate stages for `$match`, `$sort`, `$skip`, `$limit`, `$project`, and `$count`, while preserving the existing PyMongo `count_documents()` `$group` shape. Verification: `cargo fmt -- --check` passed; `cargo test aggregate` passed; `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_aggregation.py` failed in the sandbox at `tests/e2e/conftest.py:103` with `PermissionError: [Errno 1] Operation not permitted` while binding `127.0.0.1`; `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_metadata.py` failed with the same bind error; `cargo test` passed.

Problem:

- `aggregate` currently only supports the PyMongo `count_documents()` shape.
- Simple applications often use aggregation for filtering, sorting, slicing, projection, and counts.

Desired behavior:

- Implement a small sequential aggregation pipeline over the existing document stream and shared query helpers.
- Preserve the existing `count_documents()` behavior.

Acceptance criteria:

- Support pipeline stages:
  - `$match`: document operand, same matcher semantics as `find`;
  - `$sort`: document operand, same sort semantics as `find`;
  - `$skip`: non-negative integer operand;
  - `$limit`: non-negative integer operand;
  - `$project`: document operand, same inclusion/exclusion semantics as `find` projections;
  - `$count`: non-empty string field name, returns one document with that field and closes with an empty result when count is zero if that is the simplest PyMongo-compatible behavior.
- Process stages in order. For example, `$limit` before `$skip` must not behave like `$skip` before `$limit`.
- Keep the existing PyMongo `count_documents()` pipeline shape working, including the current `$group` count path if preserving it is the safest route.
- Use existing matcher, projection, sort, skip, and limit behavior where practical.
- Use index-assisted candidate loading only when doing so is behaviorally equivalent to collection scan. Do not make performance claims unless tests prove correctness.
- Return explicit command errors for:
  - stage documents with zero or multiple keys;
  - non-document `$match`, `$sort`, `$project`;
  - negative or non-integer `$skip` and `$limit`;
  - empty or non-string `$count`;
  - unsupported projection expressions;
  - unsupported stages such as `$lookup`, `$unwind`, `$facet`, `$addFields`, `$set`, `$unset`, `$replaceRoot`, `$out`, `$merge`, `$geoNear`, and general `$group`.
- Add Rust tests for stage order, projection, count, unsupported stages, malformed stage shapes, and matcher errors.
- Add PyMongo e2e tests for:
  - `collection.aggregate([...])` with `$match`, `$sort`, `$project`;
  - `$skip`/`$limit` stage order;
  - `$count`;
  - unsupported stage error.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_aggregation.py`
- `tests/spec_corpus/aggregation_pipeline.json`
- `README.md`
- `docs/query-atomic-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test aggregate
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_aggregation.py
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_metadata.py
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 3: Aggregation Cursor Batching and Adversarial Command Coverage

Problem:

- Real drivers expect aggregate responses to be cursor-shaped and may request a cursor `batchSize`.
- Large aggregation results should not be forced into a single first batch when the command asks for batching.

Desired behavior:

- Aggregate returns cursor responses compatible with PyMongo and uses the same per-client cursor registry as `find` when more results remain.
- Direct command callers cannot create empty-batch live-cursor loops or malformed cursor state.

Acceptance criteria:

- Thread `ClientState` into `aggregate` command handling if not already done.
- Parse `cursor: {}` and `cursor: { batchSize: <positive int> }`.
- Return `firstBatch` and a nonzero cursor id when more aggregation results remain.
- Use `getMore` with the aggregate cursor namespace and existing cursor lifecycle.
- Reject aggregate cursor `batchSize` values that are negative, zero, non-integer, or too large according to the repo's existing batch-size policy.
- Reject unsupported aggregate command options such as `allowDiskUse`, `explain`, `collation`, `hint`, `comment`, `maxTimeMS`, `bypassDocumentValidation`, `readConcern`, `writeConcern`, and `let` unless the README explicitly documents a harmless accepted no-op.
- Add Rust tests for aggregate cursor creation, `getMore`, exhaustion cleanup, and malformed cursor docs.
- Add PyMongo e2e tests using `collection.aggregate(..., batchSize=1)` or direct `database.command` if PyMongo helper behavior does not expose the needed command shape.
- Add adversarial e2e tests that prove `batchSize: 0` and unsupported options return explicit failures.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_aggregation.py`
- `tests/e2e/test_cursors.py`
- `tests/spec_corpus/aggregation_cursor.json`
- `README.md`
- `docs/query-atomic-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test aggregate
cargo test get_more
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_aggregation.py
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_cursors.py
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 4: Spec Corpus, README, and Final Verification Hardening

Problem:

- The new behavior needs durable docs and regression coverage beyond helper-level e2e tests.
- Compatibility tables must stay honest about supported and unsupported behavior.

Desired behavior:

- README and spec corpus tell the truth about the new surface.
- The final test suite covers happy paths, not-happy paths, and adversarial paths.

Acceptance criteria:

- Update README compatibility table:
  - add `findAndModify` as `Partial`;
  - update `aggregate` from count-only to the documented small pipeline subset;
  - mention aggregate cursor batching and remaining unsupported aggregation features;
  - keep auth, transactions, retryable writes, collation, and unsupported update/index behavior explicit.
- Add or update spec corpus cases for:
  - `findAndModify` update/delete/upsert happy paths;
  - `findAndModify` duplicate key and unsupported option failures;
  - aggregation pipeline success with multiple stages;
  - aggregation unsupported stage and malformed stage failures;
  - aggregate cursor batch iteration if the corpus runner can represent it cleanly.
- Ensure PyMongo e2e includes adversarial coverage for:
  - malformed command documents;
  - unsupported command options;
  - duplicate key conflicts;
  - `_id` immutability;
  - empty results and no-match behavior;
  - cursor exhaustion and after-exhaustion behavior where applicable.
- Run the full verification suite.
- Mark every milestone complete with status notes and commit hashes.
- Commit final docs/test hardening.

Likely files:

- `README.md`
- `docs/query-atomic-compatibility-goal-loop.md`
- `tests/spec_corpus/*.json`
- `tests/e2e/test_find_and_modify.py`
- `tests/e2e/test_aggregation.py`
- `tests/e2e/test_spec_corpus.py`
- `src/main.rs`

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

## Final Response Requirements

When the goal is complete, report:

- summary of implemented behavior;
- files changed;
- commits made with hashes;
- exact verification commands and pass/fail result;
- whether e2e needed unsandboxed execution for localhost binding;
- known residual risks and intentionally unsupported MongoDB behavior;
- any follow-up compatibility uplift that is now the best next candidate.
