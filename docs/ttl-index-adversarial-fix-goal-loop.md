# Goal: Fix TTL Sweep No-Mutation-On-Error Gaps

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to harden the TTL Index Compatibility uplift after parent
adversarial review. The implemented TTL behavior is broadly complete, but the
review found a blocking semantic gap: some command paths run deterministic TTL
sweeps before all command-specific validation has completed. That can mutate
stored data when a command later returns an error, and in all-expired
collections it can even mask an invalid query by deleting the only candidate
documents before the matcher sees the unsupported predicate.

## Repository Rules

Follow `AGENTS.md`.

Important reminders:

- Keep the server implementation in Rust and durable storage in SQLite.
- Treat MongoDB wire compatibility as observable behavior.
- Unsupported MongoDB behavior must return explicit command errors.
- Do not weaken matcher, validator, planner, unique-index, or TTL semantics.
- Do not revert unrelated user or agent changes.
- You are not alone in the codebase; work with current state.
- Use focused commits and update this file as milestones complete.

## Parent Review Finding

The parent review inspected the TTL implementation after commits through
`024ce5c` and found this issue:

- `find`, `count`, `distinct`, and `aggregate` perform a TTL sweep after
  top-level command parsing but before every predicate/pipeline shape is fully
  validated. An unsupported query operator or unsupported aggregate stage can
  therefore delete expired documents before returning an error. If all
  documents are expired, the invalid predicate can be masked because no
  document remains to run through `matches_filter`.
- `insert`, `update`, and `delete` perform TTL sweeps before all per-entry
  validation has completed. Malformed entries or validation failures can return
  write errors after TTL has already mutated the collection.
- `findAndModify` parses more eagerly, but still needs a regression test proving
  invalid hints, invalid updates, and validation failures do not cause
  unintended TTL deletion before the command error path is returned.

This violates the TTL goal prompt requirement that invalid TTL-adjacent command
paths do not partially mutate data when a surrounding command later returns a
validation, hint, or parse error.

## Target State

TTL sweeps still run at deterministic command boundaries for successful,
well-formed commands, and still release unique conflicts before valid writes.
However, commands that fail validation before their normal read/write behavior
must not delete expired documents as a side effect.

Practical target behavior:

- All read paths validate command-specific query/pipeline/projection/sort/hint
  shape before calling `sweep_ttl_namespace`:
  - `find`;
  - `count`;
  - `distinct`;
  - `aggregate`.
- Write paths validate all non-storage-mutating entry shape before calling
  `sweep_ttl_namespace_at_tx`:
  - `insert`;
  - `update`;
  - `delete`;
  - findAndModify.
- Valid writes still sweep before unique checks and target selection, so expired
  documents do not cause false duplicate-key conflicts or stale matches.
- Unsupported filters return errors independent of how many documents are in
  the collection.
- Invalid commands do not mutate expired or unexpired documents, index entries,
  or TTL metadata.

## Definition Of Done

The fix is complete only when:

1. There is an explicit filter preflight validator for the supported query
   subset used by `find`, `count`, `distinct`, aggregate `$match`, update
   query, delete query, and findAndModify query paths.
2. The filter validator catches unsupported top-level operators and unsupported
   field operators without relying on document data.
3. Aggregate pipeline validation happens before TTL sweep and catches malformed
   stages, unsupported stages, invalid `$match`, invalid `$sort`, invalid
   `$project`, invalid `$count`, invalid `$unwind`, and invalid `$group` shapes
   without mutating documents.
4. Insert/update/delete preflight validation catches malformed batch entries
   before TTL sweeps. Valid writes still sweep before unique checks and target
   selection.
5. New adversarial Rust tests prove no TTL deletion occurs for invalid
   `find`, `count`, `distinct`, `aggregate`, `insert`, `update`, `delete`, and
   findAndModify commands.
6. New PyMongo e2e tests cover at least invalid `find`, invalid `aggregate`,
   invalid update, and invalid insert/validation behavior with expired TTL
   documents present.
7. Existing TTL happy paths still pass: expired documents disappear through
   valid reads/writes, future/non-date values remain, and unique conflicts are
   released after valid TTL sweeps.
8. `cargo fmt -- --check`, focused TTL tests, full `cargo test`, benchmark
   budget, uv lock/sync, and full PyMongo e2e pass.
9. This file is updated with completed checkboxes, status notes, and commit
   hashes.

## Milestone Checklist

When a milestone is complete:

1. Run the milestone's verification commands.
2. Update this file by changing the milestone from `[ ]` to `[x]`.
3. Add a short status note with the date, exact commands run, and commit hash.
4. Commit the code, tests, docs, and status update with a focused commit
   message.
5. Report the commit hash in the goal-loop status before continuing.

- [x] Milestone 0: Read-path preflight validation before TTL sweeps
- [ ] Milestone 1: Write-path preflight validation before TTL sweeps
- [ ] Milestone 2: PyMongo adversarial coverage and full verification

## Milestone 0: Read-Path Preflight Validation Before TTL Sweeps

Problem:

- Read commands can sweep before invalid filters/pipelines are rejected.

Desired behavior:

- Add validation helpers that inspect filter and pipeline shape without needing
  stored documents.
- Call those helpers before TTL sweeps in `find`, `count`, `distinct`, and
  `aggregate`.
- Preserve existing command error codes/messages as much as practical.

Acceptance criteria:

- Rust tests prove expired documents remain after invalid:
  - `find` unsupported field operator;
  - `find` unsupported top-level operator;
  - `count` unsupported field operator;
  - `distinct` unsupported query operator;
  - `aggregate` unsupported stage;
  - `aggregate` invalid `$match` predicate.
- Rust tests prove invalid filters still return explicit command errors when
  all documents would otherwise be expired.
- Existing read TTL happy paths still pass.
- Milestone status is marked done in this file and committed.

Status note:

- 2026-07-04: Added query filter and aggregate pipeline preflight validation
  before read-path TTL sweeps. Verification run: `cargo fmt -- --check`,
  `cargo test ttl`, `cargo test find_rejects`, `cargo test aggregate`,
  `cargo test count`. Commit: `4a7cf87`.

Likely files:

- `src/main.rs`
- `docs/ttl-index-adversarial-fix-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test ttl
cargo test find_rejects
cargo test aggregate
cargo test count
```

## Milestone 1: Write-Path Preflight Validation Before TTL Sweeps

Problem:

- Write commands can sweep before malformed entries or validation failures are
  rejected.

Desired behavior:

- Pre-validate insert documents against stored validators before sweeping, but
  keep uniqueness checks after sweeping so valid inserts can reuse keys from
  expired documents.
- Pre-validate update/delete entry structure and query/update/hint shape before
  sweeping.
- For findAndModify, prove invalid hints, invalid update shape, and validation
  failure paths do not delete expired documents.

Acceptance criteria:

- Rust tests prove expired documents remain after invalid:
  - insert validation failure;
  - update malformed entry;
  - update unsupported query operator;
  - delete malformed entry;
  - delete unsupported query operator;
  - findAndModify invalid update;
  - findAndModify update validation failure.
- Rust tests prove valid inserts still release unique conflicts by sweeping
  expired documents before unique enforcement.
- Existing write TTL happy paths still pass.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `docs/ttl-index-adversarial-fix-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test ttl
cargo test update
cargo test delete
cargo test find_and_modify
cargo test validation
```

## Milestone 2: PyMongo Adversarial Coverage And Full Verification

Problem:

- The bug is visible through real client workflows, so Rust-only tests are not
  enough.

Desired behavior:

- Add PyMongo e2e cases that create a TTL index, insert expired documents, run
  invalid commands, assert the command fails, then assert the expired document
  is still present until a valid read/write triggers a sweep.
- Keep tests deterministic with far-past/future dates and no sleeps.
- Run full verification.

Acceptance criteria:

- PyMongo e2e covers at least invalid `find`, invalid `aggregate`, invalid
  update, and invalid insert validation/no-mutation cases.
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

- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_indexes.py`
- `tests/e2e/test_validation.py`
- `docs/ttl-index-adversarial-fix-goal-loop.md`

## Final Response Requirements

When complete, report:

- every commit hash;
- exact tests and verification commands run;
- PyMongo e2e pass count;
- files changed;
- any residual risk or intentionally unsupported behavior.
