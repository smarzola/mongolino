# Goal: Aggregation v2 Adversarial TTL And Preflight Fix Loop

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to fix the parent adversarial review findings from the
Aggregation v2 uplift. The implementation is broad and mostly verified, but two
TTL/preflight consistency gaps remain:

1. `$lookup` reads a foreign namespace without running that foreign namespace's
   deterministic TTL sweep, so expired foreign documents can leak through lookup
   results even though direct reads of that collection would hide/delete them.
2. Some constant expression errors are detected only during aggregation
   execution after the source namespace TTL sweep has already run. For example,
   `{ "$addFields": { "ratio": { "$divide": [10, 0] } } }` returns a command
   error but can still sweep expired source documents first.

This fix is intentionally narrow. Do not broaden Aggregation v2 beyond the
documented subset.

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

## Parent Review Findings

### Finding 1: `$lookup` foreign reads bypass TTL

`aggregate_command_with_state` sweeps only the source namespace before executing
the pipeline. `apply_aggregate_lookup_stage` then loads the foreign collection
with `documents_for_namespace` without sweeping the foreign namespace first.

Expected behavior:

- A valid `$lookup` that reads a foreign collection must apply the same
  deterministic namespace-scoped TTL visibility rule to the foreign namespace
  before loading foreign documents.
- A malformed or unsupported pipeline must still return a command error before
  sweeping either source or foreign namespaces.
- Self-lookup should not double-sweep in a way that changes behavior; it is fine
  to sweep idempotently after validation.

### Finding 2: constant expression runtime errors occur after source TTL sweep

Pipeline shape validation currently parses expressions but does not catch
constant expression errors that are knowable before reading data. Execution then
runs after `sweep_ttl_namespace`, so a command such as constant division by zero
can delete expired source documents even though the command returns an error.

Expected behavior:

- Constant-only expression errors should be reported during preflight validation,
  before any TTL sweep.
- At minimum cover:
  - `$divide` with a literal zero divisor;
  - numeric operators with constant nonnumeric operands;
  - string operators with constant nonstring operands;
  - `$replaceRoot`/`$replaceWith` with a constant non-document result.
- Data-dependent runtime errors may remain runtime errors, but tests and docs
  should make the boundary clear if needed.
- Do not reject valid expressions merely because they reference missing fields
  in an empty synthetic document. Only evaluate or statically validate
  expressions that are fully constant and have no field paths or variables.

## Definition Of Done

The fix is complete only when:

1. `$lookup` sweeps the foreign namespace before loading foreign documents.
2. Expired foreign documents are not returned through valid `$lookup`.
3. Malformed `$lookup` pipelines do not sweep source or foreign namespaces.
4. Constant-only expression errors are rejected before the source namespace TTL
   sweep.
5. Valid field-dependent expressions still execute normally.
6. Existing Aggregation v2 functionality remains intact.
7. Rust tests cover both findings, including no-sweep-on-error assertions.
8. PyMongo e2e tests cover driver-visible `$lookup` foreign TTL behavior and at
   least one static expression no-sweep error path.
9. Full verification passes:

```bash
cargo fmt -- --check
cargo test aggregate
cargo test ttl
cargo test collation
cargo test
cargo build
cargo run --bin mongolino-bench -- --profile ci --check-budget
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

10. This file has completed milestone status notes with exact commands and
    commit hashes.
11. The fix is committed with focused commits.

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

- [x] Milestone 0: Fix `$lookup` foreign TTL visibility
- [x] Milestone 1: Preflight constant expression runtime errors
- [x] Milestone 2: PyMongo e2e and final verification

## Milestone 0: Fix `$lookup` Foreign TTL Visibility

Problem:

- `$lookup` foreign reads can return expired documents because the foreign
  namespace is loaded without a TTL sweep.

Desired behavior:

- Valid `$lookup` sweeps the foreign namespace before loading it.
- Invalid `$lookup` remains a preflight error and does not sweep source or
  foreign namespaces.

Acceptance criteria:

- `apply_aggregate_lookup_stage` or its caller sweeps the foreign namespace
  after the entire pipeline has been shape-validated and before
  `documents_for_namespace` loads foreign documents.
- Tests prove expired foreign documents are absent from lookup results and live
  foreign documents still join.
- Tests prove an unsupported `$lookup` form does not sweep expired source or
  foreign documents.
- Existing lookup null/missing, array, collation, self-lookup, and cursor tests
  still pass.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_aggregation.py`
- `docs/aggregation-v2-adversarial-fix-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test lookup
cargo test aggregate
cargo test ttl
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

Status 2026-07-05:

- Implemented a `$lookup` foreign namespace TTL sweep before loading foreign
  documents, after aggregate command and pipeline shape/collation preflight.
- Added Rust coverage proving valid `$lookup` hides/deletes expired foreign
  documents while malformed `$lookup` leaves both source and foreign TTL
  namespaces unswept.
- Verification passed:
  - `cargo fmt -- --check`
  - `cargo test lookup`
  - `cargo test aggregate`
  - `cargo test ttl`
- Commit: `0be8fd8` (`Fix aggregation TTL preflight adversarial gaps`).

## Milestone 1: Preflight Constant Expression Runtime Errors

Problem:

- Constant-only expression errors are currently discovered during execution
  after the source namespace TTL sweep.

Desired behavior:

- Expressions that can be proven invalid without reading documents are rejected
  during `validate_aggregate_pipeline_shape`.

Acceptance criteria:

- Add a static expression validation path that only evaluates or deeply checks
  fully constant expressions with no field paths and no `$$ROOT`/`$$CURRENT`
  variables.
- Preflight catches at least:
  - `$divide: [10, 0]`;
  - `$add: [1, "bad"]`;
  - `$multiply: [2, "bad"]`;
  - `$toLower: 1`;
  - `$concat: ["ok", 1]`;
  - `$replaceRoot: { newRoot: 1 }`;
  - `$replaceWith: "literal"`.
- Valid data-dependent expressions such as `$divide: ["$score", 2]`,
  `$toLower: "$name"`, and `$replaceRoot: "$profile"` continue to parse and
  run.
- Rust tests prove static errors do not sweep expired source documents.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_aggregation.py`
- `docs/aggregation-v2-adversarial-fix-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test aggregation_expression
cargo test aggregate_shaping
cargo test aggregate
cargo test ttl
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

Status 2026-07-05:

- Added conservative static expression validation during aggregate pipeline
  preflight: only expressions proven constant, with no field paths or
  `$$ROOT`/`$$CURRENT`, are evaluated before TTL sweep.
- Covered constant `$divide` by zero, numeric nonnumeric operands, string
  nonstring operands, and constant non-document `$replaceRoot`/`$replaceWith`.
- Preserved data-dependent runtime behavior for field-dependent expressions.
- Verification passed:
  - `cargo fmt -- --check`
  - `cargo test aggregation_expression`
  - `cargo test aggregate_shaping`
  - `cargo test aggregate`
  - `cargo test ttl`
- Commit: `0be8fd8` (`Fix aggregation TTL preflight adversarial gaps`).

## Milestone 2: PyMongo E2E And Final Verification

Problem:

- The fixes affect observable driver behavior and shared aggregation validation.

Desired behavior:

- Add driver-level regression coverage and run full verification.

Acceptance criteria:

- PyMongo e2e proves `$lookup` does not return expired foreign documents.
- PyMongo e2e proves a static expression error does not sweep expired source
  documents.
- Existing e2e aggregation tests still pass.
- Full verification passes:

```bash
cargo fmt -- --check
cargo test aggregate
cargo test ttl
cargo test collation
cargo test
cargo build
cargo run --bin mongolino-bench -- --profile ci --check-budget
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

- If sandboxed PyMongo e2e is blocked by localhost binding, record the sandbox
  failure and rerun the exact command unsandboxed.
- Milestone status is marked done in this file and committed.

Likely files:

- `tests/e2e/test_aggregation.py`
- `docs/aggregation-v2-adversarial-fix-goal-loop.md`

Commit requirement:

- Commit after marking this milestone done and adding the status note.

Status 2026-07-05:

- Added PyMongo e2e coverage proving `$lookup` sweeps an expired foreign TTL
  document before joining and proving static expression errors leave expired
  source TTL documents in raw storage until a later valid read.
- Sandboxed full e2e command was blocked by localhost binding:
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e`
  - Result: collected 206 tests; failed with `PermissionError: [Errno 1]
    Operation not permitted` in `allocate_local_port`; summary was 2 failed, 6
    passed, 198 errors due to the sandbox bind restriction.
- Focused unsandboxed e2e verification passed:
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_aggregation.py`
  - Result: 26 passed in 13.84s.
- Full verification passed:
  - `cargo fmt -- --check`
  - `cargo test aggregate`
  - `cargo test ttl`
  - `cargo test collation`
  - `cargo test` (180 main tests passed; 182 bench-target tests passed)
  - `cargo build` (passed with existing dead-code warnings)
  - `cargo run --bin mongolino-bench -- --profile ci --check-budget`
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`
  - `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e`
  - Result: 206 passed in 107.16s unsandboxed.
- Commit: `ffa583d` (`Record aggregation adversarial final verification`).

## Final Response Requirements

When complete, report:

- every commit hash;
- exact tests and verification commands run;
- PyMongo e2e pass count;
- files changed;
- residual risk or intentionally unsupported behavior.
