# Goal: CRUD Compatibility Uplift

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to turn `mongolino` from a narrow MongoDB wire-protocol proof of concept into a small, testable CRUD-compatible server for the documented subset. Keep the server in Rust, keep durable storage in SQLite, and make MongoDB compatibility observable through command responses, persisted BSON behavior, and real client checks when possible.

Focus this goal on the next massive uplift: query matching for normal fields and common operators, correct insert error behavior, `update`, `delete`, and compatibility documentation. Do not try to implement authentication, transactions, secondary indexes, `getMore`, server-side cursor storage, replica-set behavior, compression, or the full MongoDB query language in this goal.

## Repository Rules

Follow `AGENTS.md`.

Important reminders:

- Keep the server implementation in Rust and the durable storage layer in SQLite.
- Treat MongoDB wire compatibility as observable behavior. Validate changes with real client handshakes when possible.
- Run `cargo fmt` and `cargo test` before handing off code changes.
- Keep unsupported MongoDB commands explicit by returning command errors instead of silently accepting behavior.
- Keep changes scoped to CRUD compatibility. Do not refactor unrelated command handling, storage, or CLI behavior unless the milestone requires it.
- Do not revert unrelated user changes.
- Prefer the repo's existing patterns, tests, and automation.
- If tests expose a real product gap, fix the product rather than weakening the test.
- At the end of each milestone, run verification, mark the milestone done in this file, commit the completed milestone, and report the commit hash before continuing.

## Target State

By the end, `mongolino` should support the core single-server CRUD path that common MongoDB clients expect for simple applications.

The repo should have:

- `insert` behavior that preserves existing documents on duplicate `_id` errors and returns MongoDB-style write error information for the documented subset.
- `find` behavior that can match normal BSON fields, dotted paths, and a limited but explicit set of query operators.
- `find` result shaping for the documented subset: projection, skip, limit, sort, and batch size behavior without server-side cursors.
- `update` command support for replacement updates, `$set`, `$unset`, `$inc`, upsert, single-update, and multi-update behavior.
- `delete` command support for single and multi delete behavior.
- Explicit command errors for unsupported operators, malformed command bodies, ambiguous update documents, unsupported write options, and unsupported commands.
- Unit and integration coverage for happy paths, not-happy paths, and adversarial paths.
- README compatibility table updates that accurately describe the new supported subset and remaining gaps.

## Current State

The current implementation is intentionally narrow:

- `README.md` says the server accepts MongoDB `OP_MSG` handshakes and supports `hello`, `isMaster`, `ping`, `buildInfo`, `listDatabases`, basic `insert`, and basic `find`.
- `src/main.rs` dispatches only `hello`, `isMaster`, `ping`, `buildInfo`, `listDatabases`, `endSessions`, `insert`, and `find`.
- `insert_documents` currently uses `INSERT OR REPLACE`, which can silently overwrite an existing `_id`. MongoDB insert should report duplicate key errors and preserve the existing document.
- `find_documents` supports exact `_id` lookup and full namespace scans limited by `batchSize`.
- `find` does not support field filters, query operators, projections, sort, skip, limit semantics, collation, read concern, or server-side cursors.
- There is no `update` command and no `delete` command.
- BSON is stored as original BSON blobs in SQLite with `(namespace, id_key)` as the durable primary key.
- The repo currently has unit tests in `src/main.rs` only.

## Definition Of Done

The goal is complete only when:

1. Existing handshake, `ping`, `buildInfo`, `listDatabases`, `insert`, and `_id` find behavior still passes regression tests.
2. Duplicate `_id` inserts return a command response with write error detail and do not mutate the existing stored document.
3. `find` supports exact field matching, dotted paths, array traversal where documented, and the operators selected in this prompt.
4. Unsupported query operators and malformed filter documents return explicit command errors.
5. `find` supports the documented projection, sort, skip, limit, and batch size behavior or explicitly errors for unsupported combinations.
6. `update` supports the documented replacement and modifier subset, including upsert behavior, correct counters, and duplicate-key protection.
7. `delete` supports the documented single and multi delete subset with correct counters.
8. Not-happy-path and adversarial tests cover malformed BSON-like command shapes, type mismatches, invalid update documents, unsupported operators, duplicate keys, empty names, oversized limits, and command injection-shaped field names.
9. Real client compatibility is validated with `mongosh` or another MongoDB client when available. If no client is available, record that in the milestone status note and keep raw command/unit coverage.
10. `README.md` accurately documents supported behavior and remaining gaps.
11. Milestone checkboxes in this file are marked `[x]` as work completes.
12. Each completed milestone has a focused commit.
13. Final verification commands pass or any unrelated/environmental failures are documented with evidence.

## Milestone Checklist

When a milestone is complete:

1. Run the milestone's verification commands.
2. Update this checklist by changing the milestone from `[ ]` to `[x]`.
3. Add a short status note under that milestone with the date, exact commands run, and commit hash if available.
4. Commit the code, tests, docs, and checklist/status update with a focused commit message.
5. Report the commit hash in the goal-loop status before starting the next milestone.

- [x] Milestone 0: Baseline and test harness
- [x] Milestone 1: Insert correctness and write error contracts
- [x] Milestone 2: Query matcher for `find`
- [ ] Milestone 3: `find` result shaping
- [ ] Milestone 4: `update` command subset
- [ ] Milestone 5: `delete` command subset
- [ ] Milestone 6: Real client verification and compatibility docs

## Milestone 0: Baseline and Test Harness

Status note:

- 2026-07-03: Added in-memory SQLite command helpers, cursor batch extraction, command-error assertions, and baseline coverage for insert/find/listDatabases/unknown commands/malformed commands/OP_MSG section rejection. Verification: `cargo fmt`; `cargo test`. Commit hash: reported after checkpoint commit.

Problem:

- The current tests are useful but too narrow for a CRUD compatibility uplift.
- Most command functions are embedded in `src/main.rs`, so broad behavior changes need a clear regression harness before implementation begins.
- Future milestones need adversarial tests without relying only on manual `mongosh` checks.

Desired behavior:

- You can run a focused automated suite that exercises command documents directly against an in-memory SQLite connection.
- Tests clearly separate command response shape, storage side effects, query matching, and wire parsing behavior.
- Baseline tests pin the current supported behavior before broad CRUD changes begin.

Acceptance criteria:

- Add or restructure tests so `insert`, `_id` find, collection scans, `listDatabases`, unknown commands, malformed inserts, and `OP_MSG` parsing are covered.
- Add helpers for opening an in-memory initialized SQLite connection and extracting cursor batches from command responses.
- Add a helper that asserts command errors include `ok: 0.0`, `code`, and `errmsg`.
- Add tests for not-happy baseline paths:
  - empty command document;
  - unknown command;
  - unsupported `OP_MSG` section kind;
  - malformed `insert` without `documents`;
  - malformed `find` without a collection name.
- Keep tests passing without adding future behavior as failing tests.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `README.md` if test instructions need clarification
- `Cargo.toml` only if a test-only dependency is genuinely needed

Verification:

```bash
cargo fmt
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 1: Insert Correctness and Write Error Contracts

Status note:

- 2026-07-03: Replaced replace-on-conflict insert behavior with SQLite uniqueness-preserving inserts, duplicate key writeErrors, ordered/unordered batch handling, generated `_id` storage, and malformed insert validation. Verification: `cargo fmt`; `cargo test insert`; `cargo test`. Commit hash: reported after checkpoint commit.

Problem:

- `insert_documents` uses `INSERT OR REPLACE`, which silently overwrites existing documents. MongoDB insert must fail duplicate `_id` writes instead of replacing the document.
- The current insert path does not model ordered vs unordered bulk behavior, write errors, or partial success.
- Without correct insert semantics, update/delete tests can start from corrupted fixtures.

Desired behavior:

- Insert creates documents when `_id` is unique.
- Missing `_id` still gets a generated `ObjectId`.
- Duplicate `_id` returns a write error and preserves the original document.
- Ordered inserts stop at the first write error.
- Unordered inserts attempt later documents after a write error.
- Unsupported write options are either ignored only when harmless and documented, or rejected explicitly when accepting them would imply false compatibility.

Acceptance criteria:

- Replace `INSERT OR REPLACE` with insert behavior that reports duplicate key errors for `(namespace, id_key)` conflicts.
- Response documents include `ok: 1.0` for command-level success with `writeErrors` when appropriate, unless the command body itself is malformed.
- Duplicate key write errors include stable code and message fields suitable for client debugging.
- `n` reflects only documents actually inserted.
- Ordered duplicate-key insert stops before later documents.
- Unordered duplicate-key insert continues and records all encountered write errors.
- Generated `_id` values are present in stored documents and can be found afterward.
- Empty `documents` arrays return a documented response or explicit command error. Choose the behavior closest to MongoDB and cover it.
- Not-happy and adversarial tests include:
  - duplicate `_id` in an existing collection;
  - duplicate `_id` twice in the same batch;
  - unordered batch with success, failure, success;
  - non-document entries in `documents`;
  - missing or non-string `insert` collection name;
  - invalid `ordered` type;
  - field names shaped like operators in inserted documents, such as `"$set"` and `"a.$bad"`, verifying they are stored as data.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `README.md`

Verification:

```bash
cargo fmt
cargo test insert
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 2: Query Matcher for `find`

Status note:

- 2026-07-03: Added an in-process BSON matcher for field equality, dotted paths, array traversal, numeric comparisons across `Int32`/`Int64`/`Double`, `$eq`/`$ne`/`$gt`/`$gte`/`$lt`/`$lte`/`$in`/`$nin`/`$exists`, `$and`/`$or`/`$nor`, and `$not`, with explicit command errors for unsupported or malformed operators. Verification: `cargo fmt`; `cargo test find`; `cargo test matcher`; `cargo test`. Commit hash: reported after checkpoint commit.

Problem:

- `find` currently supports exact `_id` lookup and otherwise scans all documents.
- Applications need basic field filters before `mongolino` feels like a usable document database.
- Query operator support must be explicit and well tested because silently treating unsupported operators as literal fields would produce dangerous false positives.

Desired behavior:

- `find` evaluates a documented in-process matcher against decoded BSON documents.
- `_id` equality still uses the SQLite primary-key fast path when possible.
- Other filters scan the namespace, decode documents, and include only matching documents.
- Unsupported operators return command errors rather than being silently ignored.

Supported matcher subset for this milestone:

- Empty filter matches all documents.
- Exact equality for scalar values, embedded documents, arrays, `null`, and booleans.
- Dotted path lookup for embedded documents, such as `{ "profile.city": "Rome" }`.
- `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$exists`.
- Logical `$and`, `$or`, `$nor`.
- `$not` for one nested supported field predicate.

Scope controls:

- Do not implement regex, JavaScript, `$where`, geospatial operators, text search, collation, `$elemMatch`, or aggregation.
- If array traversal semantics are implemented, document the exact subset and test it. If not implemented, keep array matching explicit and conservative.
- Do not add secondary indexes in this milestone.

Acceptance criteria:

- Add matcher functions with focused unit tests. Prefer small pure functions for BSON path lookup, value comparison, and operator evaluation.
- Exact `_id` lookup behavior remains compatible and covered.
- Field equality filters work for strings, integers, doubles where comparable, booleans, null, embedded documents, and arrays.
- Dotted path filters work for nested documents.
- Numeric comparisons define and test cross-type behavior for `Int32`, `Int64`, and `Double`.
- `$in` and `$nin` validate that their operand is an array.
- `$exists` validates that its operand is a boolean.
- `$and`, `$or`, and `$nor` validate that their operands are non-empty arrays of documents.
- `$not` validates that its operand is a document.
- Unsupported operators such as `$regex`, `$where`, `$elemMatch`, `$all`, and typo-shaped operators return explicit command errors.
- Adversarial tests include:
  - malformed operator operands;
  - unknown top-level operator;
  - unknown field-level operator;
  - deeply nested dotted paths that do not exist;
  - dotted paths crossing scalar values;
  - mixed numeric types;
  - `NaN` or non-normal floating comparison behavior if BSON decoding can represent it;
  - filters with field names beginning with `$` where those fields are data, not operators, when nested appropriately;
  - large `$in` arrays within reasonable test limits;
  - logical operators containing non-document entries.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- Optional new module files under `src/` if splitting matcher code materially improves readability
- `README.md`

Verification:

```bash
cargo fmt
cargo test find
cargo test matcher
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 3: `find` Result Shaping

Problem:

- Even with matching, common clients expect `projection`, `sort`, `skip`, and `limit` to behave predictably.
- Current `batchSize` handling clamps values and returns a closed cursor, but does not model `limit` and skip interactions.
- Result shaping touches user-visible behavior and should reject unsupported combinations explicitly.

Desired behavior:

- `find` can shape matched documents for the documented subset while still returning a cursor with `id: 0`.
- The implementation remains honest: unsupported or ambiguous projection and sort semantics return explicit errors.

Supported result-shaping subset:

- `skip` with non-negative integer values.
- `limit` with integer values. Treat negative limit according to a documented MongoDB-compatible subset or explicitly reject it.
- `batchSize` with integer values. Continue returning `id: 0`; document that server-side cursors and `getMore` are still unsupported.
- Inclusion projection, such as `{ name: 1, age: 1 }`.
- Exclusion projection, such as `{ password: 0 }`.
- `_id` inclusion/exclusion override.
- Single-mode projection validation: reject mixed inclusion and exclusion except for `_id`.
- Sort by one or more top-level or dotted fields with `1` or `-1`.

Acceptance criteria:

- Apply matcher before result shaping.
- Apply sort before skip and limit.
- Projection returns documents with expected fields and preserves `_id` unless explicitly excluded.
- Projection supports nested dotted paths if practical. If not, reject dotted projection paths explicitly and document the limitation.
- Sort handles missing fields deterministically and documents the chosen ordering.
- Sort validates that each direction is `1` or `-1`.
- `skip`, `limit`, and `batchSize` reject invalid types and unreasonable negative values.
- `firstBatch` length respects the documented interaction among `limit` and `batchSize` while keeping cursor id `0`.
- Not-happy and adversarial tests include:
  - mixed inclusion/exclusion projection;
  - non-integer projection values;
  - invalid sort direction;
  - sort on fields with mixed BSON types;
  - negative skip;
  - very large skip;
  - zero limit;
  - negative limit;
  - `batchSize` zero, negative, and very large values;
  - projection paths that try to collide with parent and child fields, such as `{ a: 1, "a.b": 1 }`;
  - projection field names beginning with `$`.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- Optional new module files under `src/`
- `README.md`

Verification:

```bash
cargo fmt
cargo test projection
cargo test sort
cargo test find
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 4: `update` Command Subset

Problem:

- Without `update`, applications cannot modify persisted documents.
- Update semantics are riskier than insert/find because malformed update documents can corrupt stored BSON or falsely claim compatibility.
- Upsert behavior depends on matcher and insert correctness from earlier milestones.

Desired behavior:

- `handle_command` dispatches `update` to a documented subset.
- Updates operate inside a SQLite transaction.
- The command returns MongoDB-style counters for matched and modified documents in the supported subset.
- Unsupported update operators and ambiguous documents return explicit command errors.

Supported update subset:

- Command shape: `{ update: <collection>, updates: [ ... ], ordered: <bool optional>, "$db": <db> }`.
- Each update entry supports `q`, `u`, `upsert`, and `multi`.
- Replacement update documents without operator keys.
- Modifier update documents with `$set`, `$unset`, and `$inc`.
- Upsert for replacement and modifier updates.
- Single update when `multi` is false or omitted.
- Multi update when `multi` is true.

Scope controls:

- Do not implement array filters, positional operators, pipeline updates, collation, hints, write concern, bypass document validation, retryable writes, or sessions in this milestone.
- Reject update documents that mix replacement fields and update operators.
- Reject unsupported update operators explicitly.

Acceptance criteria:

- Add update command parsing with validation for collection name and `updates` array.
- Apply query matcher from Milestone 2 to select target documents.
- Replacement updates preserve or validate `_id` according to MongoDB-like behavior. Do not allow replacement to change `_id`.
- `$set` creates or replaces fields by path in the documented subset.
- `$unset` removes fields by path in the documented subset.
- `$inc` validates numeric existing values and numeric operands.
- Upsert inserts a new document when no documents match and `upsert: true`.
- Upsert builds the new document from equality predicates where safe, plus replacement or modifier update content. Keep this conservative and documented.
- Duplicate key conflicts during update or upsert return write errors and preserve existing documents.
- Ordered update batches stop after the first write error.
- Unordered update batches continue after write errors.
- Counters distinguish matched and modified documents for common cases.
- Not-happy and adversarial tests include:
  - missing `updates`;
  - non-array `updates`;
  - update entry missing `q` or `u`;
  - `q` not a document;
  - `u` not a document;
  - empty modifier update;
  - mixed replacement and modifier update;
  - unsupported operator such as `$rename`, `$push`, or `$pull`;
  - attempt to change `_id`;
  - `$inc` on a string or document field;
  - `$inc` with non-numeric operand;
  - dotted path update through a scalar parent;
  - conflicting paths in one update, such as `$set: { a: 1, "a.b": 2 }`;
  - upsert that would create duplicate `_id`;
  - ordered and unordered multi-entry batches with partial failure.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- Optional new module files under `src/`
- `README.md`

Verification:

```bash
cargo fmt
cargo test update
cargo test insert
cargo test find
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 5: `delete` Command Subset

Problem:

- Without `delete`, applications cannot remove persisted documents.
- Delete should reuse matcher semantics and preserve explicit behavior for malformed requests.

Desired behavior:

- `handle_command` dispatches `delete` to a documented subset.
- Deletes run in a SQLite transaction.
- Delete responses report the number of removed documents.
- Single delete and multi delete behavior are distinct and tested.

Supported delete subset:

- Command shape: `{ delete: <collection>, deletes: [ ... ], ordered: <bool optional>, "$db": <db> }`.
- Each delete entry supports `q` and `limit`.
- `limit: 1` deletes one matching document.
- `limit: 0` deletes all matching documents.

Scope controls:

- Do not implement collation, hints, write concern, retryable writes, sessions, or explain behavior.
- Reject unsupported delete options explicitly.

Acceptance criteria:

- Add delete command parsing with validation for collection name and `deletes` array.
- Reuse matcher behavior from Milestone 2.
- Delete by `_id` can use the SQLite primary-key path when safe.
- Delete-many removes all matched documents for `limit: 0`.
- Delete-one removes a deterministic single matched document for `limit: 1`.
- `n` reflects only documents actually removed.
- Ordered delete batches stop after the first write error.
- Unordered delete batches continue after write errors.
- Not-happy and adversarial tests include:
  - missing `deletes`;
  - non-array `deletes`;
  - delete entry missing `q`;
  - `q` not a document;
  - missing `limit`;
  - invalid limit values other than `0` or `1`;
  - unsupported delete options;
  - unsupported operators inside `q`;
  - deleting from an empty collection;
  - deleting one of many matches;
  - unordered batch with failure then success;
  - repeated delete for the same `_id`.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- Optional new module files under `src/`
- `README.md`

Verification:

```bash
cargo fmt
cargo test delete
cargo test find
cargo test update
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 6: Real Client Verification and Compatibility Docs

Problem:

- Unit tests prove internal behavior, but MongoDB compatibility is observable through real client command flow.
- The README compatibility table must stay accurate after the CRUD uplift.
- The final handoff needs concrete evidence, not just implementation claims.

Desired behavior:

- A real client can connect, handshake, insert, find, update, delete, and observe errors for unsupported behavior where the local environment supports it.
- README documents the supported CRUD subset and remaining gaps clearly.
- Final verification covers both automated tests and client-level smoke checks.

Acceptance criteria:

- Update `README.md` interface table for:
  - `insert`;
  - `find`;
  - `update`;
  - `delete`;
  - BSON storage;
  - indexes;
  - cursors;
  - unsupported commands.
- Add concise examples for insert, find with field filter, update, and delete.
- Add a compatibility notes section that names unsupported query/update/delete features rather than implying broad MongoDB compatibility.
- Run `cargo fmt` and `cargo test`.
- Run a real client smoke test when `mongosh` or another client is available.
- The real client smoke test should cover:
  - `db.runCommand({ ping: 1 })`;
  - `insertOne`;
  - duplicate key failure;
  - `findOne` by `_id`;
  - `find` by non-`_id` field and supported operator;
  - projection;
  - sort/skip/limit;
  - `updateOne` with `$set`;
  - `updateMany` with `$inc` where supported;
  - `deleteOne`;
  - unsupported operator failure.
- If no real client is installed, record the attempted command and exact failure in the milestone status note. Do not install tooling without user approval.
- Not-happy and adversarial manual/client checks include:
  - unsupported operator returns an error to the client;
  - malformed update returns an error to the client;
  - duplicate key failure does not replace the original document;
  - delete with invalid limit returns an error;
  - unsupported command remains explicit.
- Milestone status is marked done in this file and committed.

Likely files:

- `README.md`
- `src/main.rs`
- Optional `tests/` files if integration tests have been introduced

Verification:

```bash
cargo fmt
cargo test
```

Optional real client smoke test:

```bash
cargo run -- --addr 127.0.0.1:27018 --db /tmp/mongolino-crud-smoke.sqlite3
```

In another terminal, if `mongosh` is available:

```bash
mongosh mongodb://127.0.0.1:27018 --quiet
```

Then run a compact script like:

```javascript
use crud_smoke
db.users.drop()
db.users.insertOne({ _id: "u1", name: "Ada", age: 37, profile: { city: "Rome" }, score: 1 })
db.users.insertOne({ _id: "u2", name: "Grace", age: 39, profile: { city: "London" }, score: 2 })
db.users.findOne({ _id: "u1" })
db.users.find({ age: { $gte: 38 } }).toArray()
db.users.find({}, { name: 1, _id: 0 }).sort({ age: -1 }).limit(1).toArray()
db.users.updateOne({ _id: "u1" }, { $set: { name: "Ada Lovelace" }, $inc: { score: 1 } })
db.users.updateMany({ age: { $gte: 37 } }, { $inc: { score: 1 } })
db.users.deleteOne({ _id: "u2" })
db.users.find({}).sort({ _id: 1 }).toArray()
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Final Verification

Before the goal is complete, run:

```bash
cargo fmt
cargo test
```

Also run a real client smoke test when available:

```bash
cargo run -- --addr 127.0.0.1:27018 --db /tmp/mongolino-crud-final.sqlite3
mongosh mongodb://127.0.0.1:27018 --quiet
```

If a local environment prevents binding to `127.0.0.1` or no MongoDB client is installed, document the exact failure and explain which automated tests cover the same behavior.

## Final Response Required

When complete, report:

- target state achieved or not achieved;
- milestone commits made, with hashes;
- files changed;
- exact verification commands run and results;
- real client smoke test result or exact reason it was skipped;
- known residual risks or follow-up issues.
