# mongolino

`mongolino` is a small MongoDB wire-protocol server backed by one SQLite file.
It supports a documented single-server CRUD subset for simple client smoke
tests and local experiments.

## Run

```sh
cargo run -- --addr 127.0.0.1:27017 --db mongolino.sqlite3
```

Then connect with a MongoDB client:

```sh
mongosh mongodb://127.0.0.1:27017
```

Example commands:

```javascript
use app
db.users.insertOne({ _id: "u1", name: "Ada" })
db.users.insertOne({ _id: "u2", name: "Grace", age: 39, profile: { city: "London" } })
db.users.find({ age: { $gte: 38 } }, { name: 1, _id: 0 }).sort({ age: -1 }).toArray()
db.users.updateOne({ _id: "u1" }, { $set: { name: "Ada Lovelace" }, $inc: { score: 1 }, $push: { tags: "logic" } })
db.users.findOneAndUpdate({ _id: "u1" }, { $inc: { score: 1 } }, { returnDocument: "after" })
db.users.aggregate([{ $match: { age: { $gte: 38 } } }, { $sort: { age: -1 } }, { $project: { name: 1 } }]).toArray()
db.users.deleteOne({ _id: "u2" })
```

## MongoDB Interface Surface

Compatibility flags:

- `Compatible`: expected to work for the documented subset.
- `Partial`: accepted, but missing meaningful MongoDB behavior.
- `Stub`: returns a successful response only to keep clients moving.
- `Unsupported`: returns a command or protocol error.

| Surface | Compatibility | Current behavior | Gaps |
| --- | --- | --- | --- |
| TCP listener | Compatible | Listens on `--addr`, defaulting to `127.0.0.1:27017`. | No TLS, Unix sockets, IPv6-specific handling, or connection limits. |
| MongoDB message header | Compatible | Reads and writes standard 16-byte wire message headers with request/response IDs. | Does not validate all cross-field semantics. |
| `OP_MSG` | Partial | Parses body section kind `0`, exposes document sequence section kind `1` as command arrays, and replies with `OP_MSG`. | Ignores flags; no compression. |
| `OP_QUERY` | Partial | Parses legacy query payloads and replies with `OP_REPLY`. Useful for legacy handshake-style clients. | No legacy cursor behavior beyond one-document command replies. |
| Other opcodes | Unsupported | Returns an error document for unknown opcodes. | No `OP_COMPRESSED`, `OP_GET_MORE`, `OP_INSERT`, `OP_UPDATE`, `OP_DELETE`, or `OP_KILL_CURSORS`. |
| `hello` | Compatible | Returns a standalone writable primary-style handshake with wire version and size limits. | No replica set, topology, compression, speculative auth, or server parameters. |
| `isMaster` / `ismaster` | Compatible | Same response shape as `hello`, with `ismaster` and `helloOk`. | Same handshake gaps as `hello`. |
| `ping` | Compatible | Returns `{ ok: 1.0 }`. | None for basic ping behavior. |
| `buildInfo` | Partial | Returns version, allocator/storage hints, BSON size, bitness, and `ok`. | Not byte-for-byte compatible with MongoDB server build metadata. |
| `listDatabases` | Partial | Lists database names derived from the collection catalog and persisted document namespaces, including databases with empty collections. | Size accounting is placeholder `0`. |
| `endSessions` | Stub | Returns `{ ok: 1.0 }`. | Sessions are not stored, validated, expired, or attached to operations. |
| `create` | Partial | Creates a durable empty collection catalog entry and stores supported `$jsonSchema` validators with `validationLevel: "strict"` and `validationAction: "error"`. | Collection options such as capped collections, clustered indexes, timeseries, collation, and unsupported validator features are rejected. |
| `listCollections` | Partial | Lists durable catalog entries and legacy document-only namespaces, with `nameOnly`, simple `name` equality filters, and stored validator options for non-`nameOnly` calls. | Size/details are minimal; complex filters and unsupported collection metadata options are rejected. |
| `collMod` | Partial | Updates or clears durable collection validator metadata for existing collections. Clearing uses `validator: {}`. Supports the narrow TTL update shape `index: { name, expireAfterSeconds }` for existing TTL indexes. | Only validator metadata, `validationLevel: "strict"`, `validationAction: "error"`, and TTL duration updates by index name are supported. No key-pattern index updates, non-TTL index conversion, view, timeseries, collation, or validation conversion behavior. |
| `drop` | Partial | Drops a collection by removing its documents, catalog entry, user index metadata, and maintained index entries. | No view, change-stream, or storage-stat side effects. |
| `dropDatabase` | Partial | Drops catalog entries and documents for the selected database only. | No users, roles, profiling collections, or storage statistics. |
| `count` | Partial | Counts documents matching the supported filter subset, with `skip` and `limit`; supports explicit command `hint` for safe exact/prefix/range index plans, `explain: true` diagnostics, and the supported collation subset. Runs deterministic namespace-scoped TTL sweeps before non-explain counts. PyMongo `estimated_document_count()` uses this path. | No read concern, maxTimeMS, or storage-stat semantics. PyMongo `count_documents()` uses aggregate and does not get command-level count hints. Non-simple collation range filters are rejected. |
| `aggregate` | Partial | Runs a sequential read pipeline subset: `$match`, `$unwind`, `$group`, `$sort`, `$skip`, `$limit`, computed and inclusion/exclusion `$project`, `$addFields`/`$set`, `$unset`, `$replaceRoot`, `$replaceWith`, simple same-database equality `$lookup`, and `$count`; supports a bounded expression subset (`$literal`, `$ifNull`, string conversion/case operators, comparisons, boolean operators, `$cond`, and numeric arithmetic); supports bounded group keys and `$sum`, `$avg`, `$min`, `$max`, `$first`, `$last`, `$push`, and `$addToSet` over supported expressions; preserves the PyMongo `count_documents()` `$group` shape; supports the documented collation subset for `$match`, `$sort`, `$count`, expression comparisons, and simple `$lookup`; returns cursor documents and supports per-client `cursor.batchSize` with `getMore`; runs deterministic namespace-scoped TTL sweeps before reading. | No `$lookup` pipeline/`let` form, cross-database lookup, `$facet`, `$bucket`, `$sortByCount`, `$out`, `$merge`, `$geoNear`, `$redact`, window stages, server-side JavaScript, allowDiskUse, hint, read concern, write concern, explain, maxTimeMS, aggregate command `let`, broad ICU collation, or full expression/operator parity. Unsupported shapes return command errors. Non-simple collation range filters are rejected. |
| `distinct` | Partial | Returns unique scalar, dotted-path, and array-expanded values for documents matching the supported filter subset, ordered deterministically by BSON sort order or supported collation order; de-duplicates strings under the supported collation subset; runs deterministic namespace-scoped TTL sweeps before reading. | No hint, read concern, maxTimeMS, or complex array semantics beyond the documented matcher behavior. Non-simple collation range filters are rejected. |
| `insert` | Partial | Accepts `documents`, assigns `_id` when missing, preserves existing documents on duplicate `_id`, reports duplicate key and validation `writeErrors`, supports ordered/unordered batches, and honors `bypassDocumentValidation: true`. | No write concern, retryable writes, or sessions beyond accepting `lsid`. |
| `find` | Partial | Returns `firstBatch` and creates a per-client server-side cursor when more shaped results remain. Supports exact matches, dotted paths, limited array traversal, `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$exists`, `$regex`, `$type`, `$size`, `$all`, `$elemMatch`, `$and`, `$or`, `$nor`, `$not`, projection, sort, skip, limit, capped batch size, supported collation, `hint`, `explain: true`, conservative exact/prefix/range index narrowing, sort-aware reads for unique fully covered bool/ObjectId/date scalar index keys, and deterministic namespace-scoped TTL sweeps before non-explain reads. | No `$where`, geospatial/text search, read concern, tailable cursors, text/geospatial/hashed/wildcard index planning, broad sort pushdown, or non-simple collation range filters. Unsupported operators and unsupported predicate shapes return command errors. |
| `findAndModify` / `findandmodify` | Partial | Supports PyMongo `find_one_and_update`, `find_one_and_replace`, and `find_one_and_delete` for one document, with filter, deterministic sort, supported collation for target matching/sort, `fields`/`projection`, pre-image or post-image return, update/replacement/pipeline upsert, supported update modifiers, the bounded update pipeline subset, conservative positional `$`, `$[]`, and `$[identifier]` with `arrayFilters`, `_id` immutability, validator enforcement, `bypassDocumentValidation: true`, unique-index enforcement, maintained index entries, supported `hint`, and deterministic namespace-scoped TTL sweeps before target selection. | No nested multi-array positional traversal, array modifiers through positional paths, unsupported update pipeline stages, write concern, maxTimeMS, `let`, retryable writes, transaction semantics, explain behavior, or non-simple collation range filters. Unsupported shapes return command errors. |
| `getMore` | Partial | Returns `nextBatch` for live per-client cursors and closes cursors on exhaustion. | Cursor state is in memory, per connection, and snapshot-at-find-time. No cursor timeout, awaitData, or cross-connection cursor lookup. |
| `killCursors` | Partial | Removes live per-client cursors and reports `cursorsKilled` or `cursorsNotFound`. | No cross-connection cursor lookup. Malformed cursor ids return command errors. |
| `update` | Partial | Supports replacement updates; update pipelines with `$set`/`$addFields`, `$unset`, `$project`, `$replaceRoot`, and `$replaceWith`; `$set`, `$unset`, `$inc`, `$rename`, `$min`, `$max`, `$mul`, `$setOnInsert`; array modifiers `$push`, `$addToSet`, `$pop`, `$pull`, and `$pullAll` for the documented subset; conservative positional `$`, `$[]`, and `$[identifier]` with `arrayFilters` for supported scalar modifiers; upsert; single-update; multi-update; ordered/unordered batches; supported per-entry collation; supported `hint`; `_id` immutability; validator enforcement; `bypassDocumentValidation: true`; and duplicate-key write errors. | No nested multi-array positional traversal, array modifiers through positional paths, unsupported update pipeline stages, `$push` `$position`/`$slice`/`$sort`, write concern, retryable writes, transactions, explain behavior, or non-simple collation range filters. |
| `delete` | Partial | Supports batch deletes with `q`, `limit`, supported per-entry collation, and supported `hint`; `limit: 1` deletes one deterministic match and `limit: 0` deletes all matches. | No write concern, retryable writes, explain behavior, or non-simple collation range filters. |
| Cursors | Partial | `find` and `aggregate` store remaining results under a positive cursor id, PyMongo can iterate across multiple batches, exhausted cursors close with `id: 0`, and `killCursors` explicitly closes live cursors. | No cursor timeout. Cursor state is not durable and is scoped to one client connection. Invalid or exhausted cursor ids return explicit command errors for `getMore`. |
| BSON storage | Partial | Stores BSON blobs in SQLite, derives a stable primary key from `_id`, maintains exact, compound-prefix, range, scalar multikey, sparse, partial, TTL, and supported collation-aware index metadata/entries for supported planner shapes, stores durable collection validator metadata, and enforces the supported validator subset on document-producing writes. Inserts with operator-shaped field names store those names as data. | No document size enforcement beyond message size. Unsupported planner shapes fall back or error as documented instead of weakening matcher semantics. |
| Authentication | Unsupported | No auth challenge or credential validation. | No SCRAM, x.509, keyfile, localhost exception, users, roles, or permissions. |
| Transactions | Unsupported | No multi-operation transaction protocol. | No sessions, transaction numbers, retryable writes, snapshot reads, or rollback semantics. |
| Indexes | Partial | Supports `createIndexes`, `listIndexes`, and `dropIndexes` metadata for simple ascending/descending single-field and compound keys, lists virtual `_id_`, enforces supported `unique: true` indexes across insert/update/upsert, maintains safe exact, compound-prefix, range, scalar multikey, sparse, partial, single-field TTL, and supported collation-aware planner entries, accepts supported hints, exposes partial explain diagnostics, and deletes expired TTL documents at deterministic command boundaries. Supported collation metadata is persisted and listed. | Text, geospatial, hashed, wildcard, compound TTL, `_id` TTL, hidden, background monitor timing, sparse/partial TTL combinations, collation combined with TTL or partial indexes, non-TTL-to-TTL `collMod` conversion, full compound multikey planning, numeric range planning, non-simple collation range planning, string sort pushdown, broad collation sort pushdown, and full MongoDB explain parity are unsupported. Unique indexes reject array/multikey values. |

## Current Storage Model

Documents are stored as BSON blobs in SQLite:

- `namespace`: MongoDB namespace, for example `app.users`
- `id_key`: a stable key derived from `_id`
- `bson`: the original BSON document

If an inserted document does not include `_id`, `mongolino` assigns a BSON
ObjectId before writing it to SQLite.

Collection names are also stored in a durable SQLite catalog, so empty
collections created through `create` remain visible to `listCollections` and
`listDatabases`.

## CRUD Compatibility Notes

Supported query matching is intentionally small and explicit. Field equality
works for scalars, documents, arrays, booleans, `null`, and comparable numeric
types. Dotted paths can traverse embedded documents and arrays of documents.
Unsupported or malformed operators return command errors instead of being
silently accepted.

The shared query matcher is used by `find`, `count`, `distinct`, aggregation
`$match`, update/delete target selection, `findAndModify`, index membership
checks, and `$pull` document predicates. Supported predicate shapes include
exact BSON equality; dotted paths with limited array traversal; comparison
operators; `$in`/`$nin`; `$exists`; `$not`; logical `$and`/`$or`/`$nor`;
`$regex` with string or BSON regex operands and options `i`, `m`, and `s`;
`$type` for common aliases and BSON type codes; `$size` for exact array length;
scalar `$all`; `$all` with supported `$elemMatch` clauses; and `$elemMatch` for
scalar and document arrays. Unsupported regex options, invalid regex patterns,
malformed `$type`/`$size`/`$all`/`$elemMatch` operands, JavaScript `$where`,
geospatial/text predicates, and expression predicates remain explicit errors.

Collation support is intentionally narrow. `{ locale: "simple" }` preserves
binary semantics. `{ locale: "en", strength: 2 }` and
`{ locale: "en_US", strength: 2 }` provide case-insensitive string equality,
ordering, and distinct de-duplication on supported read/write command paths.
Unsupported options such as `numericOrdering`, `caseLevel`, `alternate`,
`maxVariable`, `backwards`, `normalization`, unsupported locales, unsupported
strengths, non-document collation values, and unknown fields return explicit
command or write errors. Non-simple string range filters are rejected instead of
using unsafe binary index semantics. Full ICU behavior, locale-specific sort
orders, diacritic folding, text/geospatial collation behavior, and broad
collation sort pushdown are not implemented.

Projection supports inclusion or exclusion mode, with `_id` as the only allowed
mode override. Sort supports top-level or dotted fields with `1` or `-1`; missing
fields sort deterministically before present fields in ascending order.

Update paths support dotted document fields and a conservative single-array
positional subset. Dotted updates through scalar parents, conflicting paths such
as `{ a: 1, "a.b": 2 }`, nested/multiple positional segments, attempts to
change `_id`, and unsupported update operators are rejected with write errors.

Supported update modifiers are intentionally bounded. Scalar modifiers include
`$set`, `$unset`, `$inc`, `$rename`, `$min`, `$max`, `$mul`, and `$setOnInsert`.
Array modifiers include `$push` with scalar values or `$each`, `$addToSet` with
scalar values or `$each`, `$pop` with `1` or `-1`, `$pull` with equality or the
supported matcher predicate subset, and `$pullAll` with scalar/document equality.
`$setOnInsert` applies only to inserted upserts. Supported scalar modifiers
through positional paths are `$set`, `$unset`, `$inc`, `$min`, `$max`, and
`$mul`; `$` selects the first matching element from a supported query predicate,
`$[]` applies to every element of one array path, and `$[identifier]` uses
`arrayFilters` documents with one lowercase identifier and the supported matcher
subset. `$push` option documents using `$position`, `$slice`, `$sort`, or
unknown options are rejected explicitly, as are array modifiers through
positional paths.

Update pipelines are supported for `$set`/`$addFields`, `$unset`, `$project`,
`$replaceRoot`, and `$replaceWith` using the bounded aggregation expression
subset. Pipeline updates preserve `_id`, can synthesize conservative upsert base
documents from equality query fields, and reject unsupported stages such as
`$lookup`, `$group`, `$unwind`, `$out`, `$merge`, and `$facet`.

`findAndModify` uses the same matcher, sort, projection, update application,
duplicate-key checks, and maintained index entries as `find`, `update`, and
`delete`. It is SQLite-transaction backed for one selected document, but it does
not implement MongoDB sessions, retryable writes, write concern durability
semantics, or multi-document transactions.

Validation supports a small `$jsonSchema` subset for object documents:
root `bsonType: "object"`, `required`, and nested `properties` with `bsonType`
values `object`, `array`, `string`, `int`, `long`, `double`, `number`, `bool`,
`objectId`, `date`, and `null`. `number` matches int, long, and double. Nested
object properties may define their own `required` and `properties`. Dotted
validator property names, arrays/items, regex/patterns, numeric bounds, string
lengths, combinators, dependencies, collation, `validationLevel` values other
than `strict`, and `validationAction` values other than `error` are rejected
explicitly. `collMod` clears validation with `validator: {}`. Insert, update,
and find-and-modify enforce validators unless `bypassDocumentValidation: true`
is present; bypass does not disable `_id` immutability or unique indexes.

Aggregation is a document-stream subset. Each supported stage runs in order over
the current stream, so `$limit` before `$skip` is intentionally different from
`$skip` before `$limit`. Unsupported stages and unsupported expression shapes
return command errors instead of being ignored. `$unwind` supports field-path
strings and document form with `path`, `preserveNullAndEmptyArrays`, and
`includeArrayIndex`. `$group`, computed `$project`, `$addFields`/`$set`,
`$replaceRoot`, and `$replaceWith` share the bounded expression evaluator
documented in the command table. Simple `$lookup` supports only same-database
`from`/`localField`/`foreignField`/`as` equality joins; missing local or foreign
fields compare as `null`, local arrays match scalar foreign values by any
element, and unsupported pipeline/`let` forms return explicit command errors.

## Development

```sh
cargo fmt -- --check
cargo test
cargo build
```

Run repeatable local performance benchmarks without binding localhost or using
PyMongo:

```sh
cargo run --bin mongolino-bench -- --profile smoke
cargo run --bin mongolino-bench -- --profile smoke --json /tmp/mongolino-bench-smoke.json
cargo run --bin mongolino-bench -- --profile local --json /tmp/mongolino-bench-local.json
```

Run the CI-friendly smoke budget locally:

```sh
cargo run --bin mongolino-bench -- --profile ci --check-budget
```

Profiles:

- `smoke`: fast local check with small seeded collections.
- `ci`: fast budget profile used by GitHub Actions.
- `local`: larger baseline profile for before/after comparisons.

The benchmark harness creates temporary SQLite databases, seeds data through
`mongolino` command handlers, and exercises real insert, find, count, update,
index-entry refresh, and aggregation paths. Budget thresholds are intentionally
coarse: they are meant to catch severe regressions in CI, not to decide small
performance wins.

Current baseline results and SQLite pushdown targets are recorded in
`docs/performance-baseline.md`.

Run the PyMongo end-to-end suite with `uv`:

```sh
uv sync --locked --dev
uv run --locked pytest tests/e2e
```

Useful targeted subsets while developing:

```sh
uv run --locked pytest tests/e2e/test_cursors.py
uv run --locked pytest tests/e2e/test_find_and_modify.py
uv run --locked pytest tests/e2e/test_aggregation.py
uv run --locked pytest tests/e2e/test_lifecycle.py
uv run --locked pytest tests/e2e/test_indexes.py
uv run --locked pytest tests/e2e/test_metadata.py
uv run --locked pytest tests/e2e/test_update_operators.py
```

The e2e suite builds or locates `target/debug/mongolino`, starts it on a
temporary localhost port with a temporary SQLite file, connects with PyMongo,
and tears it down after each test. It does not require Docker or an external
MongoDB service.

`tests/spec_corpus/` contains a small local JSON corpus inspired by MongoDB's
Unified Test Format and CRUD API spec. It tracks the documented `mongolino`
subset only and is not an official MongoDB compliance claim.

GitHub Actions runs Rust formatting, Rust tests, a Rust build, locked uv sync,
and the PyMongo e2e suite on pushes and pull requests.

## Scope

This is not a full MongoDB replacement. The next major pieces are
broader query/update edge cases, more aggregation stages, richer index planning,
authentication behavior, transactions, retryable writes, and deeper driver
compatibility testing.
