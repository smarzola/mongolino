# Goal: Write Concern SQLite Durability Mapping

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to turn accepted MongoDB `writeConcern` durability metadata
into an observable SQLite durability setting without claiming distributed
MongoDB semantics. `mongolino` already runs SQLite in WAL mode and accepts a
safe single-node writeConcern subset. This goal makes `j: true` request a
local journal durability upgrade by executing that write with
`PRAGMA synchronous = FULL`, while the normal local acknowledged path uses
`PRAGMA synchronous = NORMAL`.

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
- Do not weaken existing write validation, retryable write, transaction
  rejection, TTL preflight, or duplicate-key behavior.
- Do not revert unrelated user or agent changes.
- You are not alone in the codebase. Other agents may have touched nearby
  files; work with current state and do not revert unrelated edits.

## Target State

SQLite connection setup uses WAL mode, foreign keys, and a documented
single-node default of `synchronous = NORMAL`. Supported write commands with
`writeConcern: { j: true }` run with `synchronous = FULL` for that command and
restore the prior/default synchronous setting afterward. Supported
`writeConcern` forms without `j: true`, including `{}`, `{ w: 1 }`, and the
documented local-ack interpretation of `{ w: "majority" }`, keep the normal
setting.

The server must remain honest:

- `w: "majority"` is still a local acknowledged write, not replica majority
  durability.
- `j: true` maps only to SQLite local journal/fsync strength.
- `w: 0`, unsupported `w` values, invalid `j`, invalid timeout fields, and
  transaction fields are rejected before mutation.

## Current State

Source inspection on 2026-07-05 shows:

- `init_connection` sets `journal_mode = WAL` and `foreign_keys = ON`.
- `validate_write_concern` accepts optional boolean `j`, but returns only
  validation success/failure.
- `DriverWorkflowOptions` carries only retryable-write metadata.
- `handle_command_with_state` dispatches all commands after workflow
  validation and retryable-write replay checks.
- README currently describes boolean `j` as an accepted local SQLite no-op.

Likely files:

- `src/main.rs`
- `tests/e2e/test_driver_workflow.py`
- `README.md`
- `docs/performance-baseline.md`
- this goal file

## Definition Of Done

The goal is complete only when:

1. `init_connection` explicitly sets `journal_mode = WAL`,
   `synchronous = NORMAL`, and `foreign_keys = ON`.
2. Driver workflow parsing records whether the command requested
   `writeConcern.j == true`.
3. Only commands that support `writeConcern` can request the journaled path.
4. Write commands with `j: true` execute with SQLite `synchronous = FULL`.
5. The connection synchronous setting is restored to the previous/default value
   after command completion, including command-error responses and Rust errors.
6. Retryable-write replay of an already recorded command does not unnecessarily
   toggle SQLite pragmas or mutate state.
7. Invalid writeConcern and transaction fields still fail before mutation and
   before TTL sweeps.
8. Unit tests prove parsing, pragma application, restoration, and invalid-path
   non-mutation.
9. PyMongo e2e coverage proves `WriteConcern(j=True)` still works through a
   real driver and unsupported writeConcern remains explicit.
10. Docs state the new durability mapping and the remaining non-goals.
11. Benchmark or performance notes explain expected cost and how to measure it.
12. `cargo fmt -- --check`, `cargo test`, `cargo build`, and the relevant
    PyMongo e2e subset pass.
13. The milestone checklist below is updated with status notes.

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

- [x] Milestone 0: Design the local durability contract
- [x] Milestone 1: Carry writeConcern durability through workflow parsing
- [x] Milestone 2: Apply and restore SQLite synchronous mode around writes
- [x] Milestone 3: Tests, docs, and targeted verification

Status 2026-07-05: Completed all milestones in the current working tree. The
internal contract is `WriteConcernOptions { journaled: bool }`, where only
validated `writeConcern.j == true` requests local SQLite `synchronous=FULL`.
Connection initialization now sets `journal_mode=WAL`, `synchronous=NORMAL`,
and `foreign_keys=ON`. Journaled write commands restore the prior synchronous
mode after successful responses, command-error documents, and Rust errors.
Retryable-write replay returns before command dispatch, so replay does not
mutate or enter the journaled pragma wrapper. Docs now describe local
acknowledgement, `w: "majority"` non-replica semantics, and the expected
normal-vs-journaled benchmark method. Verification run:
`cargo fmt -- --check`, `cargo test write_concern`, `cargo test synchronous`,
`cargo test driver_workflow`, `cargo test`,
`cargo build`, and
`UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_driver_workflow.py`.
The first sandboxed PyMongo e2e attempt failed because the sandbox could not
bind `127.0.0.1`; the same command passed outside the sandbox. Commit hash:
pending final local commit.

## Milestone 0: Design The Local Durability Contract

Problem:

- The current parser validates `j` but does not expose whether local durability
  should be upgraded.

Desired behavior:

- Decide the minimal internal representation needed to distinguish normal local
  acknowledged writes from journaled local writes.

Acceptance criteria:

- `w: "majority"` remains documented as local acknowledged commit.
- `j: true` is the only supported writeConcern field that changes SQLite
  durability behavior.
- `j: false`, omitted `j`, `{}`, `{ w: 1 }`, and `{ w: "majority" }` keep
  normal synchronous mode.
- The design avoids global mutable state and works per SQLite connection.

Verification:

```bash
cargo test write_concern
```

Status 2026-07-05: Done. `cargo test write_concern` passed. Commit hash:
pending final local commit.

## Milestone 1: Carry WriteConcern Durability Through Workflow Parsing

Problem:

- `validate_write_concern` currently returns `()` and `DriverWorkflowOptions`
  carries only retryable-write metadata.

Desired behavior:

- Parse validated writeConcern into a small result, and carry a boolean such as
  `journaled_write` or a local durability enum through `DriverWorkflowOptions`.

Acceptance criteria:

- Supported write commands can observe whether `j: true` was requested.
- Unsupported commands still reject writeConcern before dispatch.
- Retryable-write replay checks still happen before command execution.
- Tests cover `j: true`, `j: false`, omitted `j`, invalid `j`, and
  unsupported writeConcern command placement.

Likely files:

- `src/main.rs`

Verification:

```bash
cargo fmt -- --check
cargo test write_concern
cargo test driver_workflow
```

Status 2026-07-05: Done. `cargo fmt -- --check`, `cargo test write_concern`,
and `cargo test driver_workflow` passed. Commit hash: pending final local
commit.

## Milestone 2: Apply And Restore SQLite Synchronous Mode Around Writes

Problem:

- SQLite `synchronous` is currently not configured, and writeConcern does not
  change local durability.

Desired behavior:

- Set the default connection synchronous mode to `NORMAL` after WAL setup.
- When a write command requests `j: true`, set `synchronous = FULL` for the
  command and restore the previous/default mode afterward.

Acceptance criteria:

- Applies only after driver workflow validation succeeds.
- Does not run for read commands or replayed retryable writes.
- Restores after successful writes, command-error documents, and Rust errors.
- Does not mask the original command result or error.
- Does not weaken transaction rejection, invalid writeConcern preflight, TTL
  preflight, unique checks, or validator failures.
- Unit tests can observe current `PRAGMA synchronous` before, during if
  feasible, and after command execution. If direct during-command observation is
  impractical, use a small helper/fault path or a narrow test hook that proves
  restoration after an error.

Likely files:

- `src/main.rs`

Verification:

```bash
cargo fmt -- --check
cargo test synchronous
cargo test write_concern
cargo test invalid_driver_workflow_options_do_not_mutate_or_sweep_ttl
```

Status 2026-07-05: Done. `cargo fmt -- --check`, `cargo test synchronous`,
`cargo test write_concern`, and the driver-workflow test containing
`invalid_driver_workflow_options_do_not_mutate_or_sweep_ttl` passed. Commit
hash: pending final local commit.

## Milestone 3: Tests, Docs, And Targeted Verification

Problem:

- The public docs currently describe boolean `j` as an accepted local no-op.
  Benchmarks do not tell users how to compare normal vs journaled local writes.

Desired behavior:

- Update README and performance notes to describe `WAL + synchronous=NORMAL`
  default and `j: true -> synchronous=FULL` local durability.
- Add or update e2e coverage for a PyMongo `WriteConcern(j=True)` write path
  and unsupported writeConcern rejection.
- Add benchmark instructions or a small benchmark note for measuring normal vs
  journaled write overhead. Do not invent headline numbers unless you actually
  run and record the command.

Verification:

```bash
cargo fmt -- --check
cargo test
cargo build
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_driver_workflow.py
```

Status 2026-07-05: Done. `cargo fmt -- --check`, `cargo test`, and
`cargo build` passed. The PyMongo driver workflow command first failed in the
sandbox with `PermissionError: [Errno 1] Operation not permitted` while binding
`127.0.0.1`, then passed outside the sandbox. Commit hash: pending final local
commit.

## Final Response Requirements

Report:

- files changed;
- commits made, if any;
- exact verification commands and results;
- durability behavior implemented;
- known residual risks, especially local-vs-distributed durability boundaries.
