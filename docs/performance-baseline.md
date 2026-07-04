# Performance Baseline

Recorded on 2026-07-04 for commit `ab487d3`.

Machine note:

- macOS 26.5.1 build 25F80
- Darwin 25.5.0 arm64
- Benchmarks run through `cargo run` in the default dev profile
- Benchmark databases were temporary SQLite files created by the harness

The benchmark command exercises real `mongolino` command handlers, SQLite
schema initialization, BSON storage, index-entry maintenance, query matching,
update, and aggregation paths. It does not bind localhost and does not require
PyMongo, Docker, or an external MongoDB service.

## Commands

```sh
cargo run --bin mongolino-bench -- --profile smoke --json /tmp/mongolino-bench-smoke.json
cargo run --bin mongolino-bench -- --profile local --json /tmp/mongolino-bench-local.json
cargo run --bin mongolino-bench -- --profile ci --check-budget
```

## Smoke Results

Profile: `smoke`; seeded query dataset: 400 documents; git commit: `ab487d3`.

| Benchmark | Iterations | Elapsed ms | Ops/sec | Latency ms |
| --- | ---: | ---: | ---: | ---: |
| insert_batch_throughput | 25 | 40.07 | 31197.06 | 1.603 |
| find_id_equality | 25 | 0.63 | 39543.95 | 0.025 |
| find_collection_scan | 25 | 108.46 | 230.49 | 4.339 |
| find_indexed_scalar_equality | 25 | 6.98 | 3583.14 | 0.279 |
| count_empty_filter | 25 | 101.99 | 245.13 | 4.079 |
| count_simple_equality | 25 | 101.76 | 245.68 | 4.070 |
| update_index_refresh | 25 | 108.84 | 229.69 | 4.354 |
| aggregation_match_count | 25 | 101.80 | 245.57 | 4.072 |
| aggregation_unwind_group | 25 | 192.06 | 130.17 | 7.682 |

## Local Results

Profile: `local`; seeded query dataset: 3000 documents; git commit: `ab487d3`.

| Benchmark | Iterations | Elapsed ms | Ops/sec | Latency ms |
| --- | ---: | ---: | ---: | ---: |
| insert_batch_throughput | 100 | 274.16 | 36475.33 | 2.742 |
| find_id_equality | 100 | 2.27 | 44122.51 | 0.023 |
| find_collection_scan | 100 | 3091.44 | 32.35 | 30.914 |
| find_indexed_scalar_equality | 100 | 209.79 | 476.67 | 2.098 |
| count_empty_filter | 100 | 2929.76 | 34.13 | 29.298 |
| count_simple_equality | 100 | 3010.88 | 33.21 | 30.109 |
| update_index_refresh | 100 | 3070.73 | 32.57 | 30.707 |
| aggregation_match_count | 100 | 3003.17 | 33.30 | 30.032 |
| aggregation_unwind_group | 100 | 5733.27 | 17.44 | 57.333 |

## Count Pushdown Results

Recorded on 2026-07-04 for commit `4402545` after SQLite count pushdown.

Smoke profile: seeded query dataset 400 documents.

| Benchmark | Iterations | Elapsed ms | Ops/sec | Latency ms |
| --- | ---: | ---: | ---: | ---: |
| insert_batch_throughput | 25 | 39.03 | 32022.65 | 1.561 |
| find_id_equality | 25 | 0.70 | 35739.81 | 0.028 |
| find_collection_scan | 25 | 131.80 | 189.68 | 5.272 |
| find_indexed_scalar_equality | 25 | 7.04 | 3549.18 | 0.282 |
| count_empty_filter | 25 | 0.67 | 37059.88 | 0.027 |
| count_simple_equality | 25 | 0.90 | 27700.83 | 0.036 |
| update_index_refresh | 25 | 127.36 | 196.30 | 5.094 |
| aggregation_match_count | 25 | 0.95 | 26204.30 | 0.038 |
| aggregation_unwind_group | 25 | 195.90 | 127.62 | 7.836 |

Local profile: seeded query dataset 3000 documents.

| Benchmark | Before ms/op | After ms/op | Change |
| --- | ---: | ---: | ---: |
| count_empty_filter | 29.298 | 0.116 | 252.6x faster |
| count_simple_equality | 30.109 | 0.066 | 456.2x faster |
| aggregation_match_count | 30.032 | 0.071 | 423.0x faster |

Full local profile after count pushdown:

| Benchmark | Iterations | Elapsed ms | Ops/sec | Latency ms |
| --- | ---: | ---: | ---: | ---: |
| insert_batch_throughput | 100 | 260.60 | 38372.56 | 2.606 |
| find_id_equality | 100 | 2.25 | 44524.43 | 0.022 |
| find_collection_scan | 100 | 3091.26 | 32.35 | 30.913 |
| find_indexed_scalar_equality | 100 | 211.13 | 473.63 | 2.111 |
| count_empty_filter | 100 | 11.61 | 8609.68 | 0.116 |
| count_simple_equality | 100 | 6.65 | 15046.45 | 0.066 |
| update_index_refresh | 100 | 3078.52 | 32.48 | 30.785 |
| aggregation_match_count | 100 | 7.10 | 14084.18 | 0.071 |
| aggregation_unwind_group | 100 | 5761.48 | 17.36 | 57.615 |

## Write Targeting And Unique Pushdown Results

Recorded on 2026-07-04 for commit `bd50e45` after SQLite write-targeting and
unique-conflict pushdown.

Smoke profile: seeded query dataset 400 documents.

| Benchmark | Iterations | Elapsed ms | Ops/sec | Latency ms |
| --- | ---: | ---: | ---: | ---: |
| insert_batch_throughput | 25 | 43.79 | 28544.76 | 1.752 |
| find_id_equality | 25 | 0.60 | 41508.17 | 0.024 |
| find_collection_scan | 25 | 108.50 | 230.42 | 4.340 |
| find_indexed_scalar_equality | 25 | 6.94 | 3602.95 | 0.278 |
| count_empty_filter | 25 | 0.62 | 40013.32 | 0.025 |
| count_simple_equality | 25 | 0.81 | 31054.28 | 0.032 |
| update_index_refresh | 25 | 9.74 | 2566.88 | 0.390 |
| aggregation_match_count | 25 | 1.04 | 24143.91 | 0.041 |
| aggregation_unwind_group | 25 | 194.48 | 128.55 | 7.779 |

Local profile: seeded query dataset 3000 documents.

| Benchmark | Before ms/op | After ms/op | Change |
| --- | ---: | ---: | ---: |
| update_index_refresh | 30.707 | 1.147 | 26.8x faster |

Full local profile after write targeting and unique pushdown:

| Benchmark | Iterations | Elapsed ms | Ops/sec | Latency ms |
| --- | ---: | ---: | ---: | ---: |
| insert_batch_throughput | 100 | 272.93 | 36639.35 | 2.729 |
| find_id_equality | 100 | 2.23 | 44924.48 | 0.022 |
| find_collection_scan | 100 | 3131.30 | 31.94 | 31.313 |
| find_indexed_scalar_equality | 100 | 215.11 | 464.88 | 2.151 |
| count_empty_filter | 100 | 11.83 | 8451.95 | 0.118 |
| count_simple_equality | 100 | 6.65 | 15045.51 | 0.066 |
| update_index_refresh | 100 | 114.73 | 871.60 | 1.147 |
| aggregation_match_count | 100 | 7.40 | 13522.58 | 0.074 |
| aggregation_unwind_group | 100 | 5775.63 | 17.31 | 57.756 |

## Compound Index Planner Results

Recorded on 2026-07-04 from the working tree based on commit `87a8b08` after
compound index entry maintenance, read/count pushdown, write target selection,
and benchmark wiring.

Smoke profile: seeded query dataset 400 documents. The dedicated
`update_compound_target` collection uses the same smoke document count.

| Benchmark | Iterations | Elapsed ms | Ops/sec | Latency ms |
| --- | ---: | ---: | ---: | ---: |
| insert_batch_throughput | 25 | 36.25 | 34484.19 | 1.450 |
| find_id_equality | 25 | 0.82 | 30304.57 | 0.033 |
| find_collection_scan | 25 | 118.30 | 211.32 | 4.732 |
| find_indexed_scalar_equality | 25 | 8.63 | 2898.07 | 0.345 |
| find_compound_equality | 25 | 10.88 | 2298.27 | 0.435 |
| count_empty_filter | 25 | 1.30 | 19238.17 | 0.052 |
| count_simple_equality | 25 | 2.09 | 11945.52 | 0.084 |
| count_compound_equality | 25 | 1.36 | 18446.78 | 0.054 |
| update_index_refresh | 25 | 18.82 | 1328.34 | 0.753 |
| update_compound_target | 25 | 18.46 | 1354.00 | 0.739 |
| aggregation_match_count | 25 | 3.99 | 6262.79 | 0.160 |
| aggregation_unwind_group | 25 | 219.53 | 113.88 | 8.781 |

Local profile: seeded query dataset 3000 documents. The dedicated
`update_compound_target` collection uses 2000 documents to isolate selective
compound target selection from unrelated index refresh overhead.

| Benchmark | Before ms/op | After ms/op | Change |
| --- | ---: | ---: | ---: |
| find_compound_equality vs find_collection_scan | 30.807 | 2.122 | 14.5x faster |
| count_compound_equality | n/a | 0.030 | below 0.25 ms/op target |
| update_compound_target | n/a | 1.336 | below 2 ms/op target |

Full local profile after compound index planner uplift:

| Benchmark | Iterations | Elapsed ms | Ops/sec | Latency ms |
| --- | ---: | ---: | ---: | ---: |
| insert_batch_throughput | 100 | 267.27 | 37416.00 | 2.673 |
| find_id_equality | 100 | 2.29 | 43699.13 | 0.023 |
| find_collection_scan | 100 | 3080.73 | 32.46 | 30.807 |
| find_indexed_scalar_equality | 100 | 220.11 | 454.33 | 2.201 |
| find_compound_equality | 100 | 212.16 | 471.34 | 2.122 |
| count_empty_filter | 100 | 11.59 | 8631.11 | 0.116 |
| count_simple_equality | 100 | 8.39 | 11911.85 | 0.084 |
| count_compound_equality | 100 | 3.02 | 33150.09 | 0.030 |
| update_index_refresh | 100 | 148.91 | 671.56 | 1.489 |
| update_compound_target | 100 | 133.60 | 748.49 | 1.336 |
| aggregation_match_count | 100 | 9.07 | 11028.80 | 0.091 |
| aggregation_unwind_group | 100 | 5719.08 | 17.49 | 57.191 |

## Interpretation

Current behavior has these SQLite-backed fast paths:

- `_id` equality `find` uses the SQLite primary key `(namespace, id_key)` and
  avoids decoding the namespace.
- Simple scalar equality `find` can use maintained `index_entries`, then still
  decodes matching BSON documents for final matcher compatibility. If an
  indexed path has array traversal omitted from scalar planner entries, a
  maintained omission sentinel disables the pushdown and the Rust matcher scans
  the collection.
- Full-key safe compound equality `find` can use maintained compound
  `index_entries`, then still decodes candidate BSON documents for final Rust
  matcher validation. The same omission sentinel disables compound pushdown
  when any indexed key path contains arrays.
- `count` uses SQLite for empty filters, exact `_id` equality, and exact
  non-numeric indexed scalar equality with maintained single-field or full-key
  compound index entries.
- Aggregation pipelines exactly shaped as `$match` followed by `$count` reuse
  the same safe count planner and avoid BSON namespace decode when the filter
  is pushdown-safe.
- update, delete, and findAndModify target selection use transaction-local
  candidates for exact `_id` equality, safe indexed scalar equality, and safe
  full-key compound equality, then still validate every candidate with the Rust
  matcher before mutating.
- single-field unique indexes with present non-null non-numeric scalar values
  use maintained `index_entries` for duplicate checks. Compound unique indexes
  use the same pushdown only when every key part is present, non-null,
  non-numeric, and scalar. Numeric values fall back to the Rust scan so
  `Int32`, `Int64`, and `Double` equality semantics remain consistent.
- sparse and partial indexes maintain entries only for member documents.
  Planner pushdown uses sparse/partial entries only when the query filter
  safely implies index membership. Count pushdown remains stricter and only
  uses those entries when non-key predicates are covered by the partial
  membership predicate.

## Sparse And Partial Index Planner Results

Recorded on 2026-07-04 from the working tree based on commit `f4f3c96` after
sparse and partial metadata, membership, uniqueness, planner safety, and
benchmark wiring.

Smoke profile: seeded query dataset 400 documents. Dedicated sparse/partial
benchmark collections use the same smoke document count, except
`update_partial_unique_check`, which uses the dedicated 400-document write
targeting collection.

| Benchmark | Iterations | Elapsed ms | Ops/sec | Latency ms |
| --- | ---: | ---: | ---: | ---: |
| insert_batch_throughput | 25 | 48.09 | 25991.13 | 1.924 |
| find_id_equality | 25 | 0.65 | 38722.17 | 0.026 |
| find_collection_scan | 25 | 117.27 | 213.19 | 4.691 |
| find_indexed_scalar_equality | 25 | 8.79 | 2845.37 | 0.351 |
| find_compound_equality | 25 | 8.21 | 3044.85 | 0.328 |
| find_partial_index_equality | 25 | 7.40 | 3376.23 | 0.296 |
| count_empty_filter | 25 | 0.67 | 37549.26 | 0.027 |
| count_simple_equality | 25 | 1.21 | 20580.37 | 0.049 |
| count_compound_equality | 25 | 1.06 | 23518.34 | 0.043 |
| count_partial_index_equality | 25 | 0.87 | 28818.44 | 0.035 |
| update_index_refresh | 25 | 10.84 | 2306.12 | 0.434 |
| update_compound_target | 25 | 8.65 | 2888.59 | 0.346 |
| update_partial_unique_check | 25 | 5.13 | 4873.02 | 0.205 |
| aggregation_match_count | 25 | 1.33 | 18804.66 | 0.053 |
| aggregation_unwind_group | 25 | 214.05 | 116.80 | 8.562 |

Local profile: seeded query dataset 3000 documents. Dedicated
`update_partial_unique_check` uses 2000 documents to isolate selective partial
unique conflict checks from unrelated index refresh overhead.

| Benchmark | Before ms/op | After ms/op | Change |
| --- | ---: | ---: | ---: |
| find_partial_index_equality vs find_collection_scan | 30.946 | 2.085 | 14.8x faster |
| count_partial_index_equality | n/a | 0.031 | below 0.5 ms/op target |
| update_partial_unique_check | n/a | 0.274 | below 2 ms/op target |

Full local profile after sparse and partial index planner uplift:

| Benchmark | Iterations | Elapsed ms | Ops/sec | Latency ms |
| --- | ---: | ---: | ---: | ---: |
| insert_batch_throughput | 100 | 356.04 | 28086.63 | 3.560 |
| find_id_equality | 100 | 2.24 | 44679.42 | 0.022 |
| find_collection_scan | 100 | 3094.58 | 32.31 | 30.946 |
| find_indexed_scalar_equality | 100 | 222.32 | 449.81 | 2.223 |
| find_compound_equality | 100 | 227.66 | 439.26 | 2.277 |
| find_partial_index_equality | 100 | 208.54 | 479.53 | 2.085 |
| count_empty_filter | 100 | 11.98 | 8350.15 | 0.120 |
| count_simple_equality | 100 | 7.71 | 12977.67 | 0.077 |
| count_compound_equality | 100 | 3.80 | 26316.36 | 0.038 |
| count_partial_index_equality | 100 | 3.09 | 32348.94 | 0.031 |
| update_index_refresh | 100 | 150.51 | 664.41 | 1.505 |
| update_compound_target | 100 | 141.04 | 709.01 | 1.410 |
| update_partial_unique_check | 100 | 27.35 | 3655.71 | 0.274 |
| aggregation_match_count | 100 | 8.43 | 11867.03 | 0.084 |
| aggregation_unwind_group | 100 | 5703.63 | 17.53 | 57.036 |

The remaining slow local results cluster around full namespace decode:

- collection-scan `find` decodes every document before filtering;
- unsupported count filters fall back to Rust matcher semantics, including
  arrays, logical operators, unsupported operators, multi-predicate filters,
  unindexed fields, null equality, document equality, partial compound
  coverage, extra-field compound filters, and numeric indexed equality where
  matcher semantics compare `Int32`, `Int64`, and `Double` cross-type;
- general aggregation still starts by loading the full namespace into memory,
  then applies `$match`, `$count`, `$unwind`, and `$group` in Rust;
- write filters outside the conservative planner, including logical operators,
  multi-predicate filters, unindexed fields, arrays, null/missing semantics,
  indexed paths with array omissions, numeric unique values, partial compound
  filters, and multikey unique shapes, still use the Rust matcher and scan
  fallback.

Expect variance between local machines and GitHub-hosted runners. The CI budget
therefore uses intentionally coarse latency and throughput thresholds. Use JSON
outputs from the same profile on the same machine for before/after comparisons.

## SQLite Pushdown Roadmap

The target is to make SQLite the query engine whenever that is behaviorally
equivalent to the supported MongoDB subset, while keeping the Rust BSON matcher
as a compatibility fallback.

1. Push down `count` for empty filters and simple indexed equality filters.

   Baseline: `count_empty_filter` is 29.298 ms/op and
   `count_simple_equality` is 30.109 ms/op on the local profile. Both are close
   to collection-scan cost because they decode full namespaces.

   Completed: empty-filter count uses `SELECT COUNT(*) FROM documents WHERE
   namespace = ?`; `_id` equality count uses the `(namespace, id_key)` primary
   key; exact scalar equality count uses `index_entries` when a matching
   maintained single-field index exists. Skip and positive limit are applied to
   the SQL count result with the same count-command semantics.

   Fallbacks: unsupported operators, arrays, logical operators, multi-predicate
   filters, unindexed fields, null equality, document equality, and any other
   filter shape outside the conservative planner still use the Rust matcher.

   Measurement: local `count_empty_filter` improved from 29.298 to 0.116 ms/op;
   local `count_simple_equality` improved from 30.109 to 0.066 ms/op.

2. Broaden SQLite candidate narrowing for simple `find`.

   Baseline: `_id` equality is 0.023 ms/op, indexed scalar equality is
   2.098 ms/op, and collection scan is 30.914 ms/op on the local profile.

   Proposed implementation: keep `_id` equality as the model fast path, then
   use index entries more aggressively for supported simple equality filters,
   including selected dotted scalar paths if index-entry maintenance can prove
   equivalent. Continue to decode candidate BSON documents and run the Rust
   matcher before returning results.

   Correctness risks: array traversal and multikey behavior are intentionally
   limited today. Candidate narrowing must never drop a document that the Rust
   matcher would accept. Projection and sort should remain Rust-side until their
   semantics are covered.

   Measurement: compare `find_indexed_scalar_equality`,
   `find_collection_scan`, and targeted planner tests that verify candidate
   freshness after insert, update, delete, and drop.

3. Push down aggregation `$match` plus `$count` when the filter is safe.

   Baseline: `aggregation_match_count` is 30.032 ms/op on the local profile,
   matching the count and collection-scan cluster.

   Completed: pipelines exactly shaped as safe `$match` followed by `$count`
   reuse the same SQLite count planner as command `count`, and return the
   documented cursor response shape, including empty first batches for zero
   matched documents.

   Fallbacks: unsupported filters, malformed stages, and non-exact pipeline
   shapes continue through the existing Rust aggregation executor.

   Measurement: local `aggregation_match_count` improved from 30.032 to
   0.071 ms/op.

4. Explore SQLite grouping for bounded scalar `$unwind`/`$group` workloads.

   Baseline: `aggregation_unwind_group` is 57.333 ms/op on the local profile,
   the slowest benchmark because it decodes documents, expands arrays, and
   groups in Rust.

   Proposed implementation: do not start with general BSON array SQL. First
   evaluate whether maintained side tables for selected array scalar fields are
   worth the complexity, or whether this belongs after count/find pushdown.

   Correctness risks: `$unwind` preserve-null behavior, include-array-index,
   whole-value equality, and accumulator ordering are subtle. This is lower
   priority until simpler count/find pushdowns prove the measurement loop.

   Measurement: compare `aggregation_unwind_group` and the existing aggregation
   tests for `$unwind`, `$group`, `$push`, and `$addToSet`.

5. Use SQLite for safe write target selection and unique conflict checks.

   Baseline: local `update_index_refresh` was 30.707 ms/op because update,
   delete, findAndModify, and unique checks could decode full namespaces even
   when `_id` or maintained index entries were sufficient to narrow work.

   Completed: transaction-local write target loading now supports exact `_id`
   equality through `(namespace, id_key)` and safe indexed scalar equality
   through `index_entries`. update, delete, and findAndModify still run the
   Rust matcher against narrowed candidates before mutation, and findAndModify
   sorting remains Rust-side. Safe single-field non-numeric scalar unique
   checks use `index_entries` while excluding the current document during
   updates; numeric unique checks fall back because `index_entries` stores
   type-tagged values.

   Fallbacks: logical operators, range operators, `$in`/`$nin`/`$ne`, arrays,
   multi-predicate filters, unindexed fields, null/missing unique semantics,
   numeric unique values, document values, compound indexes, and multikey
   unique shapes continue through the Rust scan fallback.

   Measurement: local `update_index_refresh` improved from 30.707 to
   1.147 ms/op.

Remaining pushdown candidates are aggregation-oriented: broader `$match`
planning inside aggregation pipelines, possible SQLite grouping for bounded
scalar fields, and any future side-table design for array-heavy `$unwind` and
`$group` workloads.
