# Goal: Query Language v2 Adversarial Fix

You are working in `/Users/smarzola/projects/mongolino`.

Your objective is to fix adversarial review findings from the Query Language v2
uplift without broadening the scope beyond the current supported predicate
subset. The parent review found gaps in `$type` numeric-code coverage and `$all`
empty-array semantics after the initial `$regex`, `$type`, `$size`, `$all`, and
`$elemMatch` implementation.

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
- Do not revert unrelated user or agent changes.
- You are not alone in the codebase. Work with the current tree and preserve
  existing Query v2 edits in README, roadmap docs, e2e tests, and spec corpus
  files unless a direct fix requires a small adjustment.

## Target State

Fix the following compatibility issues:

1. `$type: 7` and `$type: [7, ...]` match BSON ObjectId values, consistent with
   the supported `objectId` alias.
2. `$all: []` has explicit, tested Mongo-compatible behavior. It must not
   accidentally match every array merely because there are no required values.
3. The fixes are covered in Rust matcher tests, PyMongo e2e tests, and the
   Query v2 spec corpus where appropriate.
4. Existing Query v2 behavior for `$regex`, `$type`, `$size`, `$all`, and
   `$elemMatch` remains intact.

Do not add new unsupported query families such as `$where`, text search,
geospatial predicates, collation-aware matching, or a general expression
language.

## Current State

Relevant files:

- `src/main.rs`
- `tests/e2e/test_query_language.py`
- `tests/spec_corpus/query_language_v2.json`
- `docs/query-language-v2-goal-loop.md`
- `README.md`
- `docs/mongodb-compatibility-uplifts-roadmap.md`

Known current behavior from parent review:

- `BsonTypeName::ObjectId` exists and alias `$type: "objectId"` is supported.
- `query_type_name_for_code` supports codes `1`, `2`, `3`, `4`, `8`, `9`, `10`,
  `16`, and `18`, but not ObjectId code `7`.
- `matches_all_predicate` iterates required values and currently has no explicit
  empty-array branch, which risks matching any array for `$all: []`.

## Definition Of Done

The fix is complete only when:

1. `$type: 7` matches ObjectId fields.
2. `$type: [7, "string"]` or equivalent mixed type-array coverage is tested and
   behaves correctly.
3. `$all: []` has the chosen Mongo-compatible behavior and tests prove it.
4. `$all` scalar membership and `$all` with `$elemMatch` clauses still pass.
5. Malformed `$type`, malformed `$all`, and unsupported operators still return
   explicit errors without mutating documents.
6. PyMongo e2e covers the fixed cases through a real client.
7. Spec corpus coverage is updated for the fixed cases.
8. `docs/query-language-v2-goal-loop.md` records this adversarial repair under
   Milestone 4 or a short review-fix note.
9. Verification commands pass locally.
10. The fix is committed with a focused commit message.

## Milestone Checklist

When a milestone is complete:

1. Run the milestone's verification commands.
2. Update this file by changing the milestone checkbox from `[ ]` to `[x]`.
3. Add a short status note with the date, exact commands run, and commit hash if
   available.
4. Commit the code, tests, docs, and status-note update with a focused commit
   message.
5. Report the commit hash in the goal-loop status before finishing.

- [x] Milestone 1: Query predicate semantic fixes
- [x] Milestone 2: Cross-command adversarial coverage and verification

## Milestone 1: Query Predicate Semantic Fixes

Problem:

- Query v2 intended to support common BSON type aliases and numeric type codes,
  including ObjectId. Alias support exists, but numeric code `7` is missing.
- `$all: []` needs explicit behavior and tests so it cannot silently match every
  array via vacuous truth.

Desired behavior:

- Add numeric ObjectId code `7` to query `$type` parsing.
- Make `$all: []` return the Mongo-compatible result. Prefer matching no
  documents, and document the choice through tests.
- Keep the existing explicit error behavior for non-array `$all` operands and
  operator-document entries that are not supported `$elemMatch` clauses.

Acceptance criteria:

- Rust matcher tests cover `$type: 7`, mixed `$type` arrays containing `7`, and
  `$all: []`.
- Existing Rust tests for `$regex`, `$type`, `$size`, `$all`, and `$elemMatch`
  still pass.
- No planner pushdown should assume these new predicates are index-safe unless
  the existing candidate validation proves correctness.

Likely files:

- `src/main.rs`
- `docs/query-language-v2-adversarial-fix-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test type
cargo test all
cargo test elem
```

Status 2026-07-04: Complete. Added BSON ObjectId numeric code `7` to `$type`
parsing, added mixed `$type` array coverage containing `7`, and made `$all: []`
return no matches explicitly. Verification: `cargo fmt -- --check` passed;
`cargo test type` passed; `cargo test all` passed; `cargo test elem` passed.
Commit: `8f4e69d`.

## Milestone 2: Cross-Command Adversarial Coverage And Verification

Problem:

- The semantic fixes must be observable through real PyMongo paths and the
  repo-local spec corpus, not only pure Rust matcher tests.

Desired behavior:

- Add PyMongo e2e coverage for ObjectId numeric type code and `$all: []`.
- Add spec corpus coverage for the same cases.
- Record the repair in `docs/query-language-v2-goal-loop.md` and leave the
  roadmap/README Query v2 claims accurate.

Acceptance criteria:

- PyMongo e2e proves `$type: 7` against an ObjectId field.
- PyMongo e2e proves `$all: []` returns the chosen result.
- Spec corpus includes at least one positive ObjectId-code case and one
  `$all: []` adversarial case.
- Full focused verification passes.

Likely files:

- `tests/e2e/test_query_language.py`
- `tests/spec_corpus/query_language_v2.json`
- `docs/query-language-v2-goal-loop.md`
- `docs/query-language-v2-adversarial-fix-goal-loop.md`

Verification:

```bash
cargo fmt -- --check
cargo test
cargo build
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e/test_query_language.py tests/e2e/test_spec_corpus.py
```

Use unsandboxed execution for PyMongo e2e if the sandbox blocks localhost
binding.

Status 2026-07-04: Complete. Added PyMongo and spec-corpus coverage for
`$type: 7` ObjectId matching and `$all: []` returning no documents, while
preserving scalar `$all`, `$all` with `$elemMatch`, malformed `$type`, malformed
`$all`, and unsupported operator error coverage. Verification: `cargo fmt --
--check` passed; `cargo test type` passed; `cargo test all` passed; `cargo test
elem` passed; `cargo test` passed; `cargo build` passed; sandboxed
`UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest
tests/e2e/test_query_language.py tests/e2e/test_spec_corpus.py` failed at
localhost port allocation with `PermissionError: [Errno 1] Operation not
permitted`; unsandboxed `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run
--locked pytest tests/e2e/test_query_language.py tests/e2e/test_spec_corpus.py`
passed with 29 tests. Commit: `8f4e69d`.

## Final Response Requirements

When complete, report:

- commit hash;
- files changed;
- exact behavior chosen for `$all: []`;
- verification commands and outcomes;
- any residual risks or intentionally unsupported adjacent behavior.
