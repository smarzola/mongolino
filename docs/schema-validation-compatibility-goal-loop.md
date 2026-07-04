# Goal: Schema Validation and Collection Metadata Compatibility Uplift

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to add a substantial validation-oriented compatibility layer: durable collection options, a small `$jsonSchema` validator subset, `collMod` updates for validators, and write-path enforcement with `bypassDocumentValidation` where supported. This should make `create_collection(..., validator=...)`, application data guards, and local test setup feel natural through PyMongo while keeping unsupported MongoDB validation semantics explicit.

This is one of three major compatibility uplifts in the current delivery sequence. Keep it independently verifiable and committable.

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
- Add abstractions only where they keep create/list/collMod/write behavior consistent.
- At the end of each milestone, run verification, mark the milestone done in this file, commit the completed milestone, and report the commit hash before continuing.

## Target State

By the end, `mongolino` should support enough schema validation behavior that a simple PyMongo app can:

- create a collection with a validator document containing a supported `$jsonSchema` subset;
- inspect durable collection options through `listCollections`;
- modify a collection validator through `collMod`;
- enforce validation on `insert`, `update`, `findAndModify`, and upsert paths;
- bypass validation when a supported write command includes `bypassDocumentValidation: true`;
- receive explicit command/write errors for invalid documents, malformed validators, unsupported validator features, and unsupported validation options.

The implementation should remain honest:

- Support only a small `$jsonSchema` subset in this goal.
- No full JSON Schema engine, `oneOf`, `anyOf`, `allOf`, `not`, arrays/items, pattern/regex, numeric min/max, string length, dependencies, encryption schema, or collation.
- `validationLevel` may support only `strict`.
- `validationAction` may support only `error`.
- `bypassDocumentValidation` is accepted only on commands where it is implemented.
- Unsupported validation features must return explicit command errors instead of being stored or ignored.

## Current State

The repo currently has:

- Rust server implementation in `src/main.rs`.
- A `collections` SQLite catalog with `namespace`, `db`, and `name`.
- `create`, `listCollections`, `drop`, and `dropDatabase`.
- Write paths for `insert`, `update`, and `findAndModify`.
- Maintained index entries and unique index enforcement.
- PyMongo e2e tests and local JSON spec corpus.

Important gaps:

- `create` rejects all collection options beyond the collection name.
- `listCollections` always reports empty `options`.
- There is no durable collection validator metadata.
- `collMod` is unsupported.
- `insert`, `update`, and `findAndModify` do not enforce schema validation.
- `bypassDocumentValidation` is currently unsupported on write commands.

## Supported Validator Subset

Support validators of exactly these forms:

```javascript
{ $jsonSchema: { bsonType: "object", required: ["field"], properties: { field: { bsonType: "string" } } } }
```

Supported `$jsonSchema` keys:

- `bsonType`: only `"object"` at the root.
- `required`: array of non-empty strings.
- `properties`: document mapping field names to property schemas.

Supported property schema keys:

- `bsonType`: one string or an array of strings.
- `required`: optional nested required array for object-valued properties.
- `properties`: optional nested property schemas for object-valued properties.

Supported `bsonType` values:

- `object`
- `array`
- `string`
- `int`
- `long`
- `double`
- `number`
- `bool`
- `objectId`
- `date`
- `null`

Behavior notes:

- Missing optional properties pass.
- Required properties must exist, including nested required properties when their parent object exists.
- `number` matches int, long, and double.
- Dotted property keys are unsupported in validator definitions; use nested `properties` instead.
- Field validation uses direct document fields, not query-style array traversal.

## Definition Of Done

The goal is complete only when:

1. The `collections` catalog durably stores supported options/validator metadata and migrates existing databases safely.
2. `create` accepts supported `validator`, `validationLevel: "strict"`, and `validationAction: "error"` options.
3. `create` rejects unsupported collection options and unsupported validator schema features explicitly.
4. `listCollections` returns durable `options` including validator metadata for non-`nameOnly` calls.
5. `collMod` supports updating or clearing validator metadata for an existing collection.
6. `collMod` rejects unsupported options, missing collections, and malformed validators explicitly.
7. `insert` enforces validation before storing documents, preserving ordered/unordered write behavior.
8. `update` enforces validation on replacement, modifier update, and upsert results.
9. `findAndModify` enforces validation on update/replacement/upsert results.
10. `delete`, `drop`, and read commands are unaffected.
11. `bypassDocumentValidation: true` is accepted and effective on `insert`, `update`, and `findAndModify`.
12. `bypassDocumentValidation` with non-boolean values returns explicit command errors.
13. Validation errors use deterministic write/command error codes and messages suitable for PyMongo assertions.
14. README compatibility tables and notes accurately describe supported validation behavior.
15. PyMongo e2e tests cover happy paths, not-happy paths, and adversarial paths.
16. Local spec corpus includes representative validation cases.
17. `cargo fmt -- --check`, `cargo test`, `cargo build`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`, `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`, and `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e` pass locally. Use unsandboxed execution for localhost binding if needed.
18. Milestone checkboxes in this file are marked `[x]` as work completes.
19. Each completed milestone has a focused commit.

## Milestone Checklist

When a milestone is complete:

1. Run the milestone's verification commands.
2. Update this checklist by changing the milestone from `[ ]` to `[x]`.
3. Add a short status note under that milestone with the date, exact commands run, and commit hash if available.
4. Commit the code, tests, docs, and checklist/status update with a focused commit message.
5. Report the commit hash in the goal-loop status before starting the next milestone.

- [ ] Milestone 0: Catalog metadata and validator parser
- [ ] Milestone 1: Create/listCollections/collMod validation metadata
- [ ] Milestone 2: Insert/update/findAndModify enforcement and bypass
- [ ] Milestone 3: PyMongo e2e, spec corpus, docs, and final hardening

## Milestone 0: Catalog Metadata and Validator Parser

Problem:

- The collection catalog currently stores only namespace, db, and name.
- Validation needs durable metadata and a parser that rejects unsupported schema shapes before writes depend on it.

Desired behavior:

- Add catalog columns or a small metadata table for durable collection options.
- Build a focused validator parser/evaluator for the supported `$jsonSchema` subset.

Acceptance criteria:

- Add a migration path for existing SQLite files.
- Store validator, validationLevel, and validationAction durably as BSON or another structured representation.
- Add helper APIs to fetch metadata by namespace from both connection and transaction contexts.
- Implement parser validation for supported `$jsonSchema` shape.
- Implement evaluator returning clear validation errors for invalid documents.
- Add Rust tests for parser/evaluator happy paths and unsupported shapes.
- Keep externally visible command behavior unchanged in this milestone unless tests explicitly cover parser helpers.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `docs/schema-validation-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test validator
cargo test
cargo build
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 1: Create, listCollections, and collMod Validation Metadata

Problem:

- Applications need to create and inspect collection validators.
- Test suites often update validators through `collMod`.

Desired behavior:

- `create`, `listCollections`, and `collMod` manage durable validation metadata for the supported subset.

Acceptance criteria:

- `create` accepts:
  - `validator`;
  - `validationLevel: "strict"`;
  - `validationAction: "error"`.
- `create` stores supported metadata and rejects unsupported options explicitly.
- `listCollections` includes `options.validator`, `options.validationLevel`, and `options.validationAction` where set.
- `nameOnly: true` remains compact.
- Add `collMod` command dispatch.
- `collMod` updates validator metadata for existing collections.
- `collMod` can clear validation by setting an empty validator document if that is the simplest explicit behavior; document the chosen behavior.
- `collMod` rejects missing collections, unsupported options, unsupported validationLevel/action values, and malformed validator shapes.
- Add Rust tests and PyMongo e2e tests for create/list/collMod metadata paths.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_validation.py`
- `README.md`
- `docs/schema-validation-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test validation
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_validation.py
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 2: Write Enforcement and bypassDocumentValidation

Problem:

- Stored validators are meaningless until write paths enforce them.
- PyMongo can send `bypassDocumentValidation` on write commands and find-and-modify helpers.

Desired behavior:

- Enforce validators consistently across all document-producing write paths.
- Support explicit bypass on implemented write commands.

Acceptance criteria:

- `insert` validates every prepared document before storage unless bypassed.
- Ordered inserts stop at the first validation failure; unordered inserts continue and report indexed write errors.
- `update` validates replacement, modifier update, and upsert results unless bypassed.
- `findAndModify` validates update/replacement/upsert results unless bypassed.
- Unique index checks and `_id` immutability remain enforced even when validation is bypassed.
- `bypassDocumentValidation: true` is accepted on `insert`, `update`, and `findAndModify`.
- `bypassDocumentValidation: false` behaves as normal validation.
- Non-boolean bypass values return explicit command errors.
- Unsupported bypass on other commands remains explicit errors.
- Add Rust tests covering ordered/unordered insert, update, upsert, findAndModify, bypass, unique index interaction, and no-op invalid existing document behavior.
- Add PyMongo e2e tests covering real helper behavior.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_validation.py`
- `tests/spec_corpus/schema_validation.json`
- `README.md`
- `docs/schema-validation-compatibility-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test validation
cargo test unique
cargo test find_and_modify
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_validation.py
cargo test
```

Commit requirement:

- Commit after marking this milestone done and adding the status note.

## Milestone 3: PyMongo e2e, Spec Corpus, Docs, and Final Hardening

Problem:

- Validation behavior needs durable documentation and adversarial coverage.

Desired behavior:

- README, e2e tests, and corpus coverage define the supported validation contract clearly.

Acceptance criteria:

- README compatibility table updates:
  - `create` mentions supported validators and strict/error validation options;
  - `listCollections` mentions validator options;
  - add `collMod`;
  - write command rows mention validation and bypass behavior;
  - storage gaps no longer say schema validation is absent.
- Add or update `tests/spec_corpus/schema_validation.json`.
- Add adversarial e2e coverage for:
  - unsupported validator operators;
  - malformed required/properties/bsonType;
  - nested object validation;
  - validation failure on insert/update/findAndModify;
  - bypass success;
  - collMod missing collection and clear-validator behavior.
- Run full verification.
- Mark every milestone complete with status notes and commit hashes.
- Commit final docs/test hardening.

Likely files:

- `README.md`
- `docs/schema-validation-compatibility-goal-loop.md`
- `tests/spec_corpus/schema_validation.json`
- `tests/e2e/test_validation.py`
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

- summary of implemented validation behavior;
- files changed;
- commits made with hashes;
- exact verification commands and pass/fail result;
- whether e2e needed unsandboxed execution for localhost binding;
- known residual risks and intentionally unsupported MongoDB validation behavior.
