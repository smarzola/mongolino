# Goal: Query Language v2 Compatibility Uplift

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to deliver uplift 1 of 7 in the aggressive MongoDB
compatibility sequence: expand the supported query predicate language for real
application and ODM workloads. This uplift adds practical support for `$regex`,
`$elemMatch`, `$type`, `$size`, and `$all`, while preserving explicit errors for
unsupported shapes and maintaining the existing Rust matcher as the source of
truth.

This uplift must move the repo-local compatibility scorecard in
`docs/mongodb-compatibility-uplifts-roadmap.md` from **49% to at least 55%**,
primarily through Query predicate compatibility.

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

Support a conservative but useful Query v2 predicate subset:

- `$regex` for string fields with string or BSON regex operands, including
  common options `i`, `m`, and `s`; unsupported options return explicit errors.
- `$elemMatch` for arrays of scalars and arrays of documents using supported
  nested predicates.
- `$type` for common BSON aliases and numeric type codes used by PyMongo/ODM
  workflows.
- `$size` for exact array length matching.
- `$all` for scalar array membership using existing BSON equality semantics and
  `$elemMatch` clauses where supported.
- Query behavior is shared consistently by `find`, `count`, `distinct`,
  aggregation `$match`, update/delete target selection, findAndModify, and
  `$pull` document predicates where those paths use the matcher.
- Existing index pushdowns must fall back when a new predicate shape is not
  safely represented by maintained index entries.

## Compatibility Target

Move the scorecard from **49% to at least 55%**:

- Query predicate compatibility: `11% -> 17%`.
- Explicit unsupported behavior remains `5%`.

Do not claim support for JavaScript `$where`, geospatial predicates, text
search, full PCRE compatibility, collation-aware regex, or arbitrary expression
language in this uplift.

## Current State

Relevant current behavior:

- `matches_filter`, `matches_field_condition`, and
  `matches_operator_document` implement equality, `$eq`, `$ne`, `$gt`, `$gte`,
  `$lt`, `$lte`, `$in`, `$nin`, `$exists`, `$not`, `$and`, `$or`, and `$nor`.
- Existing tests intentionally reject `$regex`, `$elemMatch`, and `$all`.
- `values_at_path` expands dotted paths through arrays.
- `$pull` document predicates reuse query matcher behavior for supported
  shapes.
- Index pushdowns use conservative exact equality planners and validate
  candidates through the matcher.

Known constraints:

- Regex support should use Rust dependencies already available where possible.
  If a new crate is required, add it intentionally and update lockfiles.
- Regex must not panic or enable catastrophic behavior. Reject unsupported
  options and invalid patterns with command/write errors.
- `$elemMatch` semantics differ from plain dotted traversal: one array element
  must satisfy the nested predicate.
- Numeric BSON equality is cross-type in the existing matcher; do not regress
  that behavior.

## Definition Of Done

The goal is complete only when:

1. `$regex` supports string and BSON regex operands for string candidate values.
2. Regex options `i`, `m`, and `s` work; unsupported options are explicit
   errors.
3. `$elemMatch` works for scalar arrays and document arrays with supported
   nested predicates.
4. `$type` supports common aliases and numeric codes for string, object,
   array, bool, objectId, date, null, int, long, double, and number.
5. `$size` matches exact array lengths and rejects non-integer/negative
   operands.
6. `$all` matches scalar array membership and supported `$elemMatch` clauses.
7. New predicates behave consistently through `find`, `count`, aggregation
   `$match`, update/delete target selection, findAndModify, and `$pull`
   predicates where applicable.
8. Unsupported forms return explicit command or write errors without mutating
   documents.
9. Existing index planner pushdowns fall back safely for new predicate shapes
   unless exact equality planning remains valid.
10. PyMongo e2e and spec corpus cases cover happy paths, non-happy paths, and
    adversarial paths.
11. README and roadmap docs are updated.
12. Verification commands pass locally.
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

- [x] Milestone 0: Matcher architecture and error model
- [x] Milestone 1: Regex predicate support
- [x] Milestone 2: `$type`, `$size`, and `$all`
- [x] Milestone 3: `$elemMatch` scalar and document semantics
- [x] Milestone 4: Cross-command/e2e/spec coverage and docs

## Milestone 0: Matcher Architecture And Error Model

Problem:

- Adding new predicates directly inside `matches_operator_document` can create
  inconsistent errors or accidental support gaps across command paths.

Desired behavior:

- Refactor only as needed to keep predicate parsing and error reporting clear.
- Preserve all existing supported predicate behavior.
- Ensure unsupported operators remain explicit.

Acceptance criteria:

- Existing matcher tests pass unchanged or with only intentional expectation
  updates for newly supported operators.
- Error messages are precise enough for e2e assertions.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `docs/query-language-v2-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test find_matcher
cargo test update_array
cargo test
```

Status 2026-07-04: Complete. Split field-operator predicate evaluation into a
single helper while preserving the existing matcher error model and supported
operator behavior. Verification: `cargo fmt -- --check`, `cargo test
find_matcher`, `cargo test update_array`, and `cargo test` passed. Commit:
`9e8a024`.

## Milestone 1: Regex Predicate Support

Problem:

- `$regex` is currently an explicit unsupported query operator.

Desired behavior:

- Support `{field: {$regex: "pattern"}}`, `{field: /pattern/}` if BSON regex is
  represented by the Rust BSON crate, and `$options` for `i`, `m`, and `s`.
- Match only string candidate values.
- Return explicit errors for invalid regex patterns, unsupported options,
  non-string/non-regex operands, and regex inside unsupported contexts.

Acceptance criteria:

- Rust tests cover case-sensitive, case-insensitive, multiline/dotall, invalid
  pattern, unsupported option, non-string field, and array-of-strings behavior.
- PyMongo e2e covers `find`, `count_documents`, and update/delete target
  selection using regex.
- `$pull` document predicates with regex work or explicitly reject with
  documented rationale; choose one and test it.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `Cargo.toml`
- `Cargo.lock`
- `tests/e2e/test_query_language.py` or existing e2e files
- `tests/spec_corpus/query_language_v2.json`
- `docs/query-language-v2-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test regex
cargo test find_matcher
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_errors.py tests/e2e/test_crud.py
cargo test
```

Status 2026-07-04: Complete. Added `$regex` support for string operands, BSON
regex operands, `$options` `i`/`m`/`s`, string-array traversal, invalid-pattern
errors, unsupported-option errors, and `$pull` document predicate reuse.
Verification: `cargo fmt -- --check` passed; `cargo test regex` passed; `cargo
test find_matcher` passed; sandboxed `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache
uv run --locked pytest tests/e2e/test_errors.py tests/e2e/test_crud.py` failed
at localhost bind with `PermissionError: [Errno 1] Operation not permitted`;
after `cargo build`, the same PyMongo command passed unsandboxed with 31 tests;
`cargo test` passed. Commit: `d1d8edb`.

## Milestone 2: `$type`, `$size`, And `$all`

Problem:

- Common ODM filters for BSON type, exact array size, and array membership are
  unsupported.

Desired behavior:

- `$type` supports common aliases and numeric codes.
- `$size` supports exact non-negative integer array length.
- `$all` supports scalar required values and supported `$elemMatch` clauses
  after Milestone 3; scalar `$all` can land first with clear tests.

Acceptance criteria:

- Rust tests cover aliases, numeric codes, number alias, null, arrays,
  non-array `$size`, repeated values, and malformed operands.
- PyMongo e2e covers `find` and `count_documents`.
- Unsupported `$type` aliases/codes return explicit errors.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_query_language.py`
- `tests/spec_corpus/query_language_v2.json`
- `docs/query-language-v2-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test type
cargo test size
cargo test all
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_query_language.py
cargo test
```

Status 2026-07-04: Complete. Added `$type` for common aliases and BSON numeric
codes, `$size` exact non-negative array length matching, and scalar `$all`
membership. `$all` `$elemMatch` clauses remain explicit errors until Milestone
3. Verification: `cargo fmt -- --check` passed; `cargo test type` passed;
`cargo test size` passed; `cargo test all` passed; after `cargo build`,
unsandboxed `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked
pytest tests/e2e/test_query_language.py` passed with 2 tests; `cargo fmt --
--check && cargo test` passed. Commit: `8b820bf`.

## Milestone 3: `$elemMatch` Scalar And Document Semantics

Problem:

- Existing array traversal can match values across different array elements.
  `$elemMatch` requires a single array element to satisfy the nested predicate.

Desired behavior:

- Support scalar array `$elemMatch` with supported operators.
- Support document array `$elemMatch` with supported field predicates and
  logical operators where safe.
- Reject nested unsupported operators explicitly.

Acceptance criteria:

- Tests prove `$elemMatch` does not mix predicates across different array
  elements.
- Tests cover scalar arrays, document arrays, nested dotted fields inside an
  element, `$and`/`$or` where supported, malformed operands, and unsupported
  nested regex/options if not supported.
- PyMongo e2e covers find/count/update/delete/findAndModify target behavior.
- Milestone status is marked done in this file and committed.

Likely files:

- `src/main.rs`
- `tests/e2e/test_query_language.py`
- `tests/e2e/test_crud.py`
- `tests/e2e/test_find_and_modify.py`
- `tests/spec_corpus/query_language_v2.json`
- `docs/query-language-v2-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test elem
cargo test find_matcher
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_query_language.py tests/e2e/test_crud.py tests/e2e/test_find_and_modify.py
cargo test
```

Status 2026-07-04: Complete. Added `$elemMatch` for scalar arrays and document
arrays, including nested dotted fields, logical predicates, no cross-element
predicate mixing, regex/type/size/all nested scalar predicates where supported,
and `$all` `$elemMatch` clauses. Verification: `cargo test elem` passed; `cargo
test find_matcher` passed; after `cargo build`, unsandboxed
`UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest
tests/e2e/test_query_language.py tests/e2e/test_crud.py
tests/e2e/test_find_and_modify.py` passed with 41 tests; `cargo test` passed.
Commit: `c0f5d90`.

## Milestone 4: Cross-Command/E2E/Spec Coverage And Docs

Problem:

- Query predicates are shared by many commands; narrow tests can miss write-path
  or aggregation regressions.

Desired behavior:

- Add or update PyMongo e2e and spec corpus coverage for all new predicates.
- Update README compatibility table and roadmap scorecard.
- Run final verification.

Acceptance criteria:

- New predicates are covered across `find`, `count_documents`, aggregation
  `$match`, update/delete target selection, findAndModify, and `$pull` where
  applicable.
- Old unsupported-regex skip is converted into supported coverage.
- README documents supported and unsupported query language v2 behavior.
- `docs/mongodb-compatibility-uplifts-roadmap.md` records scorecard movement.
- Full verification passes.
- Milestone status is marked done in this file and committed.

Likely files:

- `README.md`
- `tests/e2e/test_query_language.py`
- `tests/e2e/test_spec_corpus.py`
- `tests/spec_corpus/query_language_v2.json`
- `docs/mongodb-compatibility-uplifts-roadmap.md`
- `docs/query-language-v2-goal-loop.md`

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

Status 2026-07-04: Complete. Added aggregation `$match` e2e and spec coverage,
converted the old unsupported-regex corpus case to supported coverage, refreshed
stale `$pull` regex error expectations to unsupported regex-option errors, and
updated README plus the compatibility roadmap scorecard from 49% to 55%.
Verification: `cargo fmt -- --check` passed; `cargo test` passed; `cargo build`
passed; `cargo run --bin mongolino-bench -- --profile ci --check-budget`
passed; `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check` passed;
`UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev` passed;
unsandboxed `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked
pytest tests/e2e` passed with 167 tests. Commit: pending.

Adversarial repair 2026-07-04: Added `$type` numeric code `7` for ObjectId,
mixed `$type` array coverage containing `7`, and explicit Mongo-compatible
`$all: []` behavior that returns no matches. Added Rust matcher, PyMongo e2e,
and spec-corpus coverage. Verification: `cargo fmt -- --check`, `cargo test
type`, `cargo test all`, `cargo test elem`, `cargo test`, and `cargo build`
passed; sandboxed `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked
pytest tests/e2e/test_query_language.py tests/e2e/test_spec_corpus.py` was
blocked by localhost binding permissions; the same command passed unsandboxed
with 29 tests. Commit: `8f4e69d`.

Use unsandboxed execution for PyMongo e2e if the sandbox blocks localhost
binding.

## Final Response Requirements

When complete, report:

- commits made;
- files changed;
- scorecard movement;
- supported query predicates and shapes;
- unsupported query shapes that remain explicit errors;
- final verification commands and outcomes;
- known residual risks.
