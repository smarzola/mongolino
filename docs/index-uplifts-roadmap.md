# Index Compatibility And Performance Uplift Roadmap

This roadmap defines the three large index uplifts for the current goal. It is
not a substitute for per-uplift goal-loop prompts; each uplift must still have
its own executable prompt, subagent implementation, adversarial review, fix
loop, verification, and push.

## Baseline

Current index support after the scalar multikey planner uplift:

- `createIndexes`, `listIndexes`, and `dropIndexes` are implemented.
- `_id_` is always listed and cannot be dropped.
- User index key specs accept ascending and descending numeric directions.
- `unique: true` is enforced for supported unique key shapes.
- Maintained `index_entries` support safe single-field scalar planner paths and
  safe full-key compound scalar planner paths.
- Single-field indexes maintain one entry per distinct supported non-numeric
  scalar array element, including dotted scalar leaves reached through arrays.
  Unsupported multikey shapes still record omission sentinels and force
  fallback.
- Sparse indexes are accepted, persisted, listed, and maintained by indexing
  only documents where all indexed fields are present. Explicit `null` counts
  as present.
- Partial indexes are accepted for field equality, field `$eq`,
  `$exists: true`, and `$and` of those supported predicates. Numeric partial
  predicates and unsupported operators are explicit command errors.
- Unique sparse and unique partial indexes enforce duplicates only among
  included documents.
- Single-field scalar, supported scalar multikey, and full-key compound scalar
  indexes can accelerate
  `find`, `count`, aggregation `$match` + `$count`, update/delete target
  selection, findAndModify target selection, and safe non-numeric unique checks.
  Sparse and partial index entries are used only when the query filter safely
  implies index membership; count pushdown additionally requires all non-key
  predicates to be covered by the partial membership predicate.
- Unsupported index types and options return explicit command errors.

Major gaps:

- Compound prefix scans, range scans, sort-only compound planning, and
  collation-aware compound behavior are absent.
- Broader partial filter implication remains unsupported beyond the tested
  equality, `$eq`, `$exists: true`, and `$and` subset.
- Full MongoDB multikey semantics are not implemented. Compound multikey,
  unique multikey, numeric arrays, document-valued array elements, `$elemMatch`,
  geospatial arrays, text indexes, wildcard indexes, and collation-aware
  multikey behavior are unsupported or fall back explicitly.
- Query hints, index choice diagnostics, and broader sort/range pushdown are
  absent.
- Text, geospatial, hashed, wildcard, collation-aware, hidden, and TTL indexes
  are unsupported.

## Compatibility Scorecard

The percentages below are a repo-local scorecard for common application index
behavior, not a claim of full MongoDB parity.

| Area | Weight | Baseline | After Uplift 1 | After Uplift 2 | After Uplift 3 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Index command surface and catalog behavior | 15% | 10% | 11% | 12% | 12% |
| Basic btree key specs and metadata | 15% | 8% | 11% | 12% | 12% |
| Unique enforcement semantics | 20% | 12% | 14% | 16% | 16% |
| Planner use for reads and writes | 25% | 8% | 15% | 17% | 21% |
| Sparse/partial/multikey practical semantics | 20% | 1% | 1% | 6% | 12% |
| Explicit unsupported behavior and tests | 5% | 4% | 4% | 5% | 5% |
| Total | 100% | 43% | 56% | 68% | 78% |

Completion target for this goal: reach at least **78%** on this scorecard while
preserving explicit errors for unsupported MongoDB index families.

## Performance Targets

Use the local benchmark profile as the headline measurement unless a prompt
states otherwise.

Baseline local numbers from `docs/performance-baseline.md`:

- `find_collection_scan`: about `31.313 ms/op`.
- `find_indexed_scalar_equality`: about `2.151 ms/op`.
- `count_simple_equality`: about `0.066 ms/op`.
- `update_index_refresh`: about `1.147 ms/op`.

After compound planner uplift local numbers:

- `find_collection_scan`: about `30.807 ms/op`.
- `find_compound_equality`: about `2.122 ms/op`, `14.5x` faster than
  collection scan.
- `count_compound_equality`: about `0.030 ms/op`.
- `update_compound_target`: about `1.336 ms/op` on the dedicated 2000-document
  compound target-selection dataset.

Goal-level targets after all three index uplifts:

- Compound equality find on a selective compound index: **at least 10x faster**
  than collection scan and below **3 ms/op** on the local profile.
- Compound equality count: below **0.25 ms/op** on the local profile when fully
  covered by maintained index entries.
- Compound write target selection: below **2 ms/op** on the local profile for a
  selective compound indexed update benchmark.
- Sparse/partial unique conflict checks: avoid full namespace scans for safe
  non-numeric scalar keys and stay below **2 ms/op** in a local targeted
  benchmark.
- Multikey equality find for scalar array elements: at least **5x faster** than
  collection scan when a maintained multikey index is safe to use.
- Multikey equality count: below **1 ms/op** for safe scalar array element
  counts.
- Multikey write target selection: below **4 ms/op** for selective scalar array
  equality update targeting.

## Uplift 1: Compound Index Planner And Benchmarks

Target compatibility movement: **43% -> 55%**.

Delivered compatibility movement: **43% -> 56%**.

Target behavior:

- Maintain `index_entries` for compound scalar indexes.
- Use compound entries for exact equality predicates covering the full compound
  key.
- Use compound entries for safe read count and write target narrowing.
- Keep Rust matcher validation after narrowing.
- Add benchmark cases for compound equality find, count, and update targeting.

Prompt: `docs/index-compound-planner-goal-loop.md`.

## Uplift 2: Sparse And Partial Index Semantics

Target compatibility movement: **55% -> 67%**.

Delivered compatibility movement: **56% -> 68%**.

Target behavior:

- Parse and list `sparse` and `partialFilterExpression` options for supported
  predicate shapes.
- Enforce unique sparse and unique partial indexes for insert/update/upsert and
  findAndModify.
- Maintain index entries only for documents included by sparse/partial rules.
- Use sparse/partial entries for planner pushdown only when query filters imply
  index membership.
- Keep unsupported partial filter operators explicit.

Delivered behavior:

- `sparse: true|false` and supported `partialFilterExpression` metadata are
  accepted, persisted, listed, and dropped.
- Sparse index membership requires every indexed field to be present; explicit
  `null` remains indexed.
- Partial index membership supports field equality, field `$eq`,
  `$exists: true`, and `$and` of supported predicates.
- Unique sparse and partial indexes enforce duplicates only among included
  documents across insert, update, upsert, delete, and findAndModify refreshes.
- Sparse/partial entries are used for find, count, aggregation count, and write
  target selection only when query filters safely imply index membership.
- Numeric partial predicates, unsupported partial operators, empty partial
  filters, `$exists: false`, non-document partial filters, and broader
  implication forms remain explicit errors or matcher fallbacks.

Prompt to write after uplift 1 review: `docs/index-sparse-partial-goal-loop.md`.

## Uplift 3: Multikey Scalar Index Semantics

Target compatibility movement: **67% -> 78%**.

Delivered compatibility movement: **68% -> 78%**.

Target behavior:

- Maintain one index entry per scalar array element for supported single-field
  multikey indexes.
- Use multikey entries for scalar equality find/count and safe write target
  narrowing while preserving matcher validation.
- Define and enforce conservative unique multikey behavior with explicit errors
  where unsupported.
- Add PyMongo e2e coverage for array field queries and mutation freshness.
- Add benchmark cases for array-element indexed find/count.

Delivered behavior:

- Single-field indexes maintain distinct entries for supported non-numeric
  scalar array elements and dotted scalar leaves reached through arrays.
- Repeated array elements deduplicate per document, and count pushdown uses
  distinct document ids.
- Scalar array equality find, update, delete, and findAndModify narrow through
  maintained entries and still validate narrowed candidates with the Rust
  matcher before returning or mutating documents.
- Safe exact count and aggregation `$match` + `$count` use maintained entries
  for scalar array equality.
- Unique multikey, compound multikey, numeric arrays, document-valued array
  elements, and unsupported operators remain explicit errors or matcher
  fallbacks.
- Local profile measurements after uplift 3: `find_multikey_scalar_equality`
  `1.519 ms/op`, `count_multikey_scalar_equality` `0.029 ms/op`, and
  `update_multikey_target` `2.042 ms/op`.

Prompt to write after uplift 2 review: `docs/index-multikey-scalar-goal-loop.md`.

## Goal Completion Requirements

This goal is complete only when:

1. All three uplift prompts exist and have completed milestone checklists.
2. Each uplift was implemented by a subagent.
3. Each uplift received an adversarial parent review.
4. Each review either found no blocking issues with evidence or produced a fix
   prompt handled by a subagent.
5. `cargo fmt -- --check`, `cargo test`, `cargo build`,
   `cargo run --bin mongolino-bench -- --profile ci --check-budget`,
   `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv lock --check`,
   `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv sync --locked --dev`, and
   `UV_CACHE_DIR=/private/tmp/mongolino-uv-cache uv run --locked pytest
   tests/e2e` pass after the third uplift and fix loop.
6. `docs/performance-baseline.md` records before/after measurements for every
   benchmark added by the index uplifts.
7. The scorecard above is updated with final evidence and reaches at least 75%.
8. The final branch is pushed to `origin/master`.
