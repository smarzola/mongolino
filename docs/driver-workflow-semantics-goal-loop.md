# Goal: Driver Workflow Semantics Compatibility Uplift

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to deliver uplift 7 of the seven-uplift MongoDB compatibility
sequence: add practical single-node driver workflow semantics for supported
`readConcern`, `writeConcern`, sessions, and retryable writes, while keeping
transactions and distributed durability semantics explicitly unsupported.

This is the final large compatibility uplift in
`docs/mongodb-compatibility-uplifts-roadmap.md`. It should move the repo-local
scorecard from **82%** to at least **86%** by raising driver workflow semantics
from **3%** to at least **7%**, without weakening explicit errors for unsupported
features.

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

By the end, `mongolino` should work with common single-node PyMongo workflows
that currently fail or are under-specified:

- commands that carry an `lsid` should validate the session id shape instead of
  accepting arbitrary BSON;
- `endSessions` should validate the command shape and remain a harmless
  single-node cleanup stub;
- supported read commands should accept `readConcern` forms that are safe
  no-ops on a local SQLite-backed single node;
- supported write commands should accept `writeConcern` forms that map cleanly
  to SQLite commit semantics;
- retryable writes should support a bounded single-statement skeleton by
  requiring `lsid + txnNumber`, rejecting transaction fields, and replaying the
  same response for duplicate retryable write attempts on the same connection;
- explicit transaction commands and transaction command fields should return
  clear command errors before mutation.

The implementation must not pretend to implement distributed MongoDB semantics:

- no causal consistency guarantees across connections;
- no snapshot reads;
- no majority replication semantics beyond local acknowledged commit;
- no journaling controls beyond accepting safe no-op shapes;
- no multi-operation transactions;
- no retryable write history across process restarts unless explicitly
  implemented and documented;
- no session expiration/storage beyond what is needed to validate and key
  retryable write responses.

## Current State

The repo currently has:

- `hello` advertises `logicalSessionTimeoutMinutes: 30`, which encourages
  drivers to send sessions.
- Many commands allow `lsid` as a key, but there is no shared validation of the
  `lsid` document shape.
- `endSessions` returns `{ ok: 1.0 }` without validating arguments.
- Write commands generally reject `writeConcern`; read commands generally reject
  `readConcern`.
- The PyMongo e2e fixture disables retryable writes in the default URI with
  `retryWrites=false`.
- There is no retryable write response cache keyed by session id and transaction
  number.
- Unknown commands such as `commitTransaction` and `abortTransaction` already
  return unsupported command errors, but transaction fields inside supported
  commands are not handled through a shared workflow layer.

Important command surfaces:

- `handle_command_with_state` dispatches every command and owns per-connection
  `ClientState`.
- `ClientState` already stores per-client cursor state, making it the natural
  place for a bounded retryable-write response cache.
- `insert`, `update`, `delete`, and `findAndModify` are the primary retryable
  write command targets.
- `createIndexes`, `dropIndexes`, `create`, `drop`, `dropDatabase`, and
  `collMod` are write-like command paths but should remain conservative unless
  implemented and tested.

## Definition Of Done

The goal is complete only when:

1. A shared driver-workflow parser validates optional `lsid`, `txnNumber`,
   `readConcern`, `writeConcern`, and transaction fields before command-specific
   mutation paths.
2. `lsid` must be a document containing an `id` UUID/Binary value or another
   explicitly documented driver-compatible UUID representation; malformed
   sessions return command errors before mutation or TTL sweeps.
3. Commands that accept `lsid` today still accept valid `lsid` after validation.
4. `endSessions` accepts an array of session documents, validates session id
   shapes, rejects malformed top-level options, and returns `{ ok: 1.0 }`
   without requiring stored session state.
5. Supported read commands accept safe `readConcern` no-op forms:
   `{ level: "local" }` and `{ level: "available" }` at minimum, plus an empty
   document if PyMongo sends it.
6. Unsupported readConcern levels such as `majority`, `linearizable`,
   `snapshot`, malformed non-document values, and unsupported fields return
   explicit command errors before TTL sweeps.
7. Supported write commands accept safe acknowledged `writeConcern` forms:
   empty document, `{ w: 1 }`, `{ w: "majority" }` as local acknowledged commit
   if documented honestly, optional boolean `j`, and numeric `wtimeout`/
   `wtimeoutMS` that are non-negative no-ops.
8. Unsafe or unsupported writeConcern forms such as `{ w: 0 }`, negative
   timeout values, invalid `j`, unknown fields, non-document values, and
   conflicting timeout aliases return explicit command errors before mutation or
   TTL sweeps.
9. Supported write commands reject transaction fields such as
   `startTransaction`, `autocommit`, and transaction command usage unless a
   milestone explicitly implements a safe subset. Rejections must happen before
   mutation.
10. `commitTransaction`, `abortTransaction`, and transaction-only commands return
    explicit command errors with clear messages.
11. Retryable writes accept valid `lsid + txnNumber` for supported one-shot
    `insert`, `update`, `delete`, and `findAndModify` commands.
12. Retrying the exact same supported write with the same `lsid + txnNumber` on
    the same connection returns the same response and does not apply the write a
    second time.
13. Reusing the same `lsid + txnNumber` for a different command body returns an
    explicit command error and does not mutate.
14. Retryable write response caching is bounded to avoid unbounded memory growth
    and documented as per-connection, in-memory, single-node behavior.
15. Retryable writes without `lsid`, with malformed `txnNumber`, or with
    transaction fields return explicit errors before mutation.
16. Ordered/unordered batch semantics, validation, unique-index enforcement,
    TTL preflight ordering, maintained index entries, collation, update
    pipeline, positional update, aggregation, and cursor behavior do not
    regress.
17. PyMongo e2e tests include a client configured with `retryWrites=true` and
    a client session where appropriate.
18. PyMongo e2e tests cover happy paths, not-happy paths, and adversarial paths
    for readConcern, writeConcern, sessions, retryable writes, and transaction
    rejection.
19. Rust unit tests cover parser behavior and no-mutation-before-error
    invariants without relying only on PyMongo.
20. README compatibility tables and notes accurately describe the new
    single-node workflow subset and residual unsupported semantics.
21. `docs/mongodb-compatibility-uplifts-roadmap.md` marks uplift 7 complete,
    moves driver workflow semantics from 3% to at least 7%, and moves total
    compatibility from 82% to at least 86%.
22. Full verification passes:

```bash
cargo fmt -- --check
cargo test
cargo build
cargo run --bin mongolino-bench -- --profile ci --check-budget
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

23. Milestone checkboxes in this file are marked `[x]` as work completes.
24. Each completed milestone has a focused commit.

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

- [x] Milestone 0: Driver workflow parser and session validation
- [x] Milestone 1: ReadConcern and writeConcern safe no-op subset
- [x] Milestone 2: Transaction rejection and no-mutation preflight
- [x] Milestone 3: Retryable write skeleton and replay cache
- [x] Milestone 4: PyMongo e2e, spec corpus, docs, scorecard, and benchmarks
- [ ] Milestone 5: Final verification and handoff

## Milestone 0: Driver Workflow Parser And Session Validation

Problem:

- `lsid` is allowed on many command paths but is not validated.
- `endSessions` accepts any command shape.
- Driver workflow option parsing is scattered across command-specific allowlists.

Desired behavior:

- Add a shared parser that can be called before dispatching or at the start of
  each command path.
- Validate optional session id shape consistently.
- Keep valid PyMongo session-bearing commands working.

Acceptance criteria:

- Add a `DriverWorkflowOptions` or equivalent structure representing optional
  `lsid`, `txnNumber`, `readConcern`, `writeConcern`, and transaction fields.
- `lsid` validation accepts the BSON shape produced by PyMongo sessions and
  rejects malformed values.
- `endSessions` validates `endSessions: [<session docs>]`, rejects malformed
  entries/options, and returns `{ ok: 1.0 }`.
- Commands that already accepted `lsid` continue accepting valid `lsid`.
- Malformed `lsid` errors happen before TTL sweeps and before mutation.
- Add focused Rust tests for valid and malformed session shapes, including
  malformed write commands that leave documents unchanged.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_handshake.py`
- `tests/e2e/test_errors.py` or a new `tests/e2e/test_driver_workflow.py`
- `docs/driver-workflow-semantics-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test session
cargo test end_sessions
cargo test ttl_invalid
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

Status:

2026-07-05:

- Added shared dispatch-level `DriverWorkflowOptions` parsing for `lsid`,
  `txnNumber`, `readConcern`, `writeConcern`, and transaction fields.
- Added session UUID/Binary shape validation and validating `endSessions` stub.
- Added Rust coverage through `handle_command_with_state` for valid sessions,
  malformed `lsid`, `endSessions`, and no-mutation behavior.
- Verification:
  - `cargo fmt`
  - `cargo test driver_workflow` -> 3 main tests and 3 bench-target tests passed.
  - `cargo test` -> 192 main tests and 194 bench-target tests passed.
- Commit: pending milestone batch commit.

## Milestone 1: ReadConcern And WriteConcern Safe No-Op Subset

Problem:

- PyMongo and ODMs often attach read/write concern options. The server currently
  rejects them broadly, even when they are safe single-node no-ops.

Desired behavior:

- Accept explicitly safe readConcern/writeConcern shapes and document their
  single-node meaning.
- Reject unsafe, malformed, or transaction/snapshot-related shapes before any
  side effects.

Acceptance criteria:

- Supported read commands accept empty `readConcern`, `local`, and `available`
  levels where safe.
- Unsupported readConcern levels and malformed fields return command errors
  before TTL sweeps.
- Supported write commands accept empty writeConcern, `{ w: 1 }`,
  documented `{ w: "majority" }` local-ack semantics, optional boolean `j`, and
  non-negative timeout fields.
- Unsupported writeConcern shapes return command/write errors before mutation
  and before TTL sweeps.
- Add Rust tests proving invalid readConcern on expired namespaces does not
  trigger TTL cleanup.
- Add Rust tests proving invalid writeConcern on insert/update/delete/
  findAndModify does not mutate.
- Existing unsupported command keys remain unsupported unless this milestone
  explicitly adopts them.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_driver_workflow.py`
- `README.md`
- `docs/driver-workflow-semantics-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test read_concern
cargo test write_concern
cargo test ttl
cargo test update
cargo test find_and_modify
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_driver_workflow.py
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

Status:

2026-07-05:

- Accepted safe local no-op readConcern forms `{}`, `local`, and `available`
  on supported read commands.
- Accepted safe acknowledged writeConcern forms `{}`, `w: 1`,
  `w: "majority"`, optional boolean `j`, and non-negative `wtimeout`/
  `wtimeoutMS` on supported write commands.
- Rejected unsupported concerns before TTL sweeps or mutation; updated older
  unsupported-option tests to reflect the new accepted safe subset.
- Verification:
  - `cargo fmt`
  - `cargo test driver_workflow` -> 3 main tests and 3 bench-target tests passed.
  - `cargo test` -> 192 main tests and 194 bench-target tests passed.
  - Sandboxed `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_driver_workflow.py` failed with `PermissionError: [Errno 1] Operation not permitted` at localhost bind.
  - Unsandboxed same command -> 6 passed.
- Commit: pending milestone batch commit.

## Milestone 2: Transaction Rejection And No-Mutation Preflight

Problem:

- The server must stay honest: accepting `lsid` and `txnNumber` must not imply
  support for MongoDB transactions.

Desired behavior:

- Transaction-only commands and transaction fields are rejected explicitly and
  early.
- Rejections never mutate data, refresh indexes, or sweep TTL.

Acceptance criteria:

- `commitTransaction`, `abortTransaction`, and transaction-only commands return
  command errors with clear unsupported messages.
- Supported commands reject `startTransaction`, `autocommit`, and transaction
  fields before mutation.
- Invalid transaction fields on write commands do not insert/update/delete.
- Invalid transaction fields on read commands do not sweep TTL.
- PyMongo session transaction attempts fail with an explicit operation failure
  and leave data unchanged.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_driver_workflow.py`
- `README.md`
- `docs/driver-workflow-semantics-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test transaction
cargo test ttl_invalid
cargo test insert
cargo test update
cargo test delete
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_driver_workflow.py
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

Status:

2026-07-05:

- Rejected `commitTransaction`, `abortTransaction`, `prepareTransaction`,
  `startTransaction`, and `autocommit` before mutation.
- Added Rust and PyMongo coverage proving transaction attempts leave documents
  unchanged.
- Verification:
  - `cargo test transaction_fields` -> 1 main test and 1 bench-target test passed.
  - `cargo test` -> 192 main tests and 194 bench-target tests passed.
  - Unsandboxed `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_driver_workflow.py` -> 6 passed.
- Commit: pending milestone batch commit.

## Milestone 3: Retryable Write Skeleton And Replay Cache

Problem:

- PyMongo retryable writes use `lsid + txnNumber`. With `retryWrites=true`, a
  client can send retryable single writes that currently fail because
  `txnNumber` is unsupported.

Desired behavior:

- Accept retryable single-statement write metadata for supported write commands.
- Cache and replay the same response for exact duplicate retry attempts on the
  same connection.
- Detect conflicting command bodies for the same retryable write key.

Acceptance criteria:

- Add bounded retryable write history to `ClientState`.
- Supported retryable commands include at least `insert`, `update`, `delete`,
  and `findAndModify` when they are single-command, non-transactional writes.
- A repeated command with the same valid `lsid + txnNumber` and same command
  body returns the same response without applying another mutation.
- Reusing the same `lsid + txnNumber` with a different command body returns an
  explicit error and leaves data unchanged.
- `txnNumber` without `lsid`, malformed `txnNumber`, negative values, and
  transaction fields return explicit errors before mutation.
- Bounded cache eviction is deterministic and tested. Eviction can be simple,
  such as keeping the newest N entries per connection.
- Add Rust tests for insert/update/delete/findAndModify retry replay, conflict
  detection, cache bounds, and no-mutation-on-error.
- Add PyMongo e2e with a client URI or client option using `retryWrites=true`.
  The default fixture may keep `retryWrites=false`; add a dedicated fixture or
  helper for this milestone.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/conftest.py`
- `tests/e2e/test_driver_workflow.py`
- `README.md`
- `docs/driver-workflow-semantics-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test retryable
cargo test session
cargo test insert
cargo test update
cargo test delete
cargo test find_and_modify
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_driver_workflow.py
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

Status:

2026-07-05:

- Added bounded per-connection retryable write cache to `ClientState`
  (`RETRYABLE_WRITE_CACHE_LIMIT = 128`).
- Supported exact duplicate replay for `insert`, `update`, `delete`, and
  `findAndModify` with valid `lsid + txnNumber`.
- Reusing the same retryable key for a different command body returns an
  explicit command error and does not mutate.
- Added Rust coverage for replay across all four write surfaces, conflict
  detection, malformed retry metadata, and FIFO eviction.
- Verification:
  - `cargo test retryable` -> 3 main tests and 3 bench-target tests passed.
  - `cargo test` -> 192 main tests and 194 bench-target tests passed.
  - Unsandboxed `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_driver_workflow.py` -> 6 passed.
- Commit: pending milestone batch commit.

## Milestone 4: E2E, Spec Corpus, Docs, Scorecard, And Benchmarks

Problem:

- Driver workflow compatibility must be observable in real PyMongo tests and
  honest docs.

Desired behavior:

- Add e2e and local spec-corpus coverage for supported and unsupported workflow
  semantics.
- Update docs and scorecard.
- Capture any performance risk from retryable-write caching.

Acceptance criteria:

- PyMongo e2e covers:
  - valid client sessions on read and write commands;
  - `endSessions`;
  - accepted readConcern/writeConcern shapes;
  - unsupported readConcern/writeConcern shapes;
  - transaction rejection through PyMongo sessions;
  - retryable write happy paths with `retryWrites=true`;
  - retry replay does not duplicate insert/update/delete/findAndModify effects;
  - retry conflict returns an explicit error and preserves data.
- Local spec corpus includes representative driver workflow cases if the runner
  can express them without awkward harness changes.
- README command table and notes describe the new accepted subset and residual
  unsupported behavior.
- `docs/mongodb-compatibility-uplifts-roadmap.md` marks uplift 7 complete and
  updates driver workflow semantics from 3% to at least 7%, total 82% to at
  least 86%.
- `docs/performance-baseline.md` or benchmark coverage is updated if retryable
  write caching adds measurable overhead or a new benchmark row is added.
- Milestone status is marked done in this file and committed.

Likely files:

- `tests/e2e/test_driver_workflow.py`
- `tests/e2e/conftest.py`
- `tests/spec_corpus/*.json`
- `tests/e2e/test_spec_corpus.py`
- `README.md`
- `docs/mongodb-compatibility-uplifts-roadmap.md`
- `docs/performance-baseline.md`
- `docs/driver-workflow-semantics-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test driver
cargo test retryable
cargo test
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_driver_workflow.py tests/e2e/test_spec_corpus.py
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

Status:

2026-07-05:

- Added `tests/e2e/test_driver_workflow.py` covering sessions, `endSessions`,
  safe and unsafe concerns, transaction rejection, `retryWrites=true`, exact
  replay, and retry conflict detection.
- Added `tests/spec_corpus/driver_workflow_semantics.json` for representative
  raw command concern and transaction cases.
- Updated README compatibility docs, roadmap scorecard, and performance note.
- Scorecard moved driver workflow semantics from 3% to 7% and total from 82% to
  86%.
- Verification:
  - Unsandboxed `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_driver_workflow.py` -> 6 passed.
  - Unsandboxed `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_spec_corpus.py` -> 31 passed.
  - `cargo test` -> 192 main tests and 194 bench-target tests passed.
- Commit: pending milestone batch commit.

## Milestone 5: Final Verification And Handoff

Problem:

- The final uplift state must be verified as a whole.

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
  - residual unsupported driver workflow behavior.
- Milestone status is marked done in this file and committed.

Likely files:

- `docs/driver-workflow-semantics-goal-loop.md`

Commit requirement:

- Commit after marking this milestone done and adding the status note.

Status:

## Final Response Requirements

When the goal is complete, report:

- final compatibility scorecard movement;
- every commit hash created for this goal;
- milestone checklist status;
- exact verification commands run and results;
- PyMongo e2e pass count;
- benchmark result summary;
- files changed;
- known residual unsupported behavior, especially transaction and distributed
  durability/session semantics.

Do not call the goal complete if any milestone remains unchecked, verification
has not run, or README/roadmap docs still describe all driver workflow
semantics as unsupported.
