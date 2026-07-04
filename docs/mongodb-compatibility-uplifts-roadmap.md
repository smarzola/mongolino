# MongoDB Compatibility Uplifts Roadmap

This roadmap tracks the seven aggressive MongoDB compatibility uplifts requested
after the index compatibility work. Each uplift must follow the established
workflow: write or update a durable goal-loop prompt, dispatch a subagent,
perform parent adversarial review, dispatch a fix subagent when issues are
found, verify locally, and push accepted work.

## Baseline

Current implementation has a practical CRUD, aggregation, validation, and index
subset, but several major MongoDB application surfaces remain unsupported or
intentionally conservative:

- Query predicates now support a practical `$regex`, `$elemMatch`, `$type`,
  `$size`, and `$all` subset; JavaScript, geospatial/text, expression, and
  collation-aware predicates remain unsupported.
- Index planning does not support compound prefix scans, range scans, sort
  pushdown, `hint`, or `explain`.
- TTL index metadata/expiration behavior is unsupported.
- Collation is rejected on find/count/distinct/aggregate/write paths.
- Aggregation has no general expression language, `$addFields`, `$lookup`, or
  broader stage coverage.
- Update pipelines, positional operators, and `arrayFilters` are unsupported.
- Sessions/retryable write/readConcern/writeConcern behavior is only minimally
  accepted or explicitly rejected.

## Compatibility Scorecard

The percentages below are repo-local practical compatibility scores, not a
claim of full MongoDB parity.

| Area | Weight | Current | Target After 7 Uplifts |
| --- | ---: | ---: | ---: |
| Query predicate compatibility | 20% | 17% | 17% |
| Index planning and diagnostics | 15% | 8% | 13% |
| Index lifecycle/TTL/collation behavior | 15% | 5% | 11% |
| Aggregation compatibility | 20% | 9% | 15% |
| Update language compatibility | 15% | 8% | 13% |
| Driver workflow semantics | 10% | 3% | 7% |
| Explicit unsupported behavior and tests | 5% | 5% | 5% |
| Total | 100% | 55% | 81% |

Completion target for this seven-uplift goal: reach at least **80%** on this
repo-local scorecard while preserving explicit errors for unsupported features.

## Uplifts

1. Query Language v2
   - Complete in uplift 1. Added `$regex`, `$elemMatch`, `$type`, `$size`, and
     `$all` for the supported BSON/query subset.
   - Hardened malformed predicate errors and practical array traversal cases.
   - Prompt: `docs/query-language-v2-goal-loop.md`.

2. Index Planner v2
   - Add compound prefix scans, safe range scans, sort pushdown, `hint`, and
     `explain` skeleton/diagnostics.
   - Prompt to write after Query v2: `docs/index-planner-v2-goal-loop.md`.

3. TTL Index Compatibility
   - Add `expireAfterSeconds` index metadata, validation, listing, and
     deterministic expiration/sweeper behavior.
   - Prompt to write after Index Planner v2: `docs/ttl-index-goal-loop.md`.

4. Collation Support
   - Add a supported simple collation subset for equality/sort and
     collation-aware indexes where safe.
   - Prompt to write after TTL: `docs/collation-compatibility-goal-loop.md`.

5. Aggregation v2
   - Add expression evaluation, `$addFields`/`$set`, broader `$project`, and a
     simple `$lookup` subset.
   - Prompt to write after Collation: `docs/aggregation-v2-goal-loop.md`.

6. Update Pipeline And Array Filters
   - Add update pipelines, positional `$`, `$[]`, `$[id]`, and `arrayFilters`
     for a conservative subset.
   - Prompt to write after Aggregation v2:
     `docs/update-pipeline-arrayfilters-goal-loop.md`.

7. Driver Workflow Semantics
   - Add practical `readConcern`, `writeConcern`, session, and retryable write
     skeleton behavior for single-node compatibility.
   - Prompt to write after Update Pipeline:
     `docs/driver-workflow-semantics-goal-loop.md`.

## Goal Completion Requirements

The seven-uplift goal is complete only when:

1. All seven goal-loop prompts exist and have completed milestone checklists.
2. Every uplift was implemented by a subagent.
3. Every uplift received a parent adversarial review.
4. Every review either found no blocking issues with evidence or produced a fix
   prompt handled by a subagent.
5. Full final verification passes:

```bash
cargo fmt -- --check
cargo test
cargo build
cargo run --bin mongolino-bench -- --profile ci --check-budget
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev
UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest tests/e2e
```

6. README and relevant docs accurately describe newly supported and still
   unsupported compatibility surfaces.
7. The scorecard above is updated with final evidence and reaches at least 80%.
8. The final branch is pushed to `origin/master`.
