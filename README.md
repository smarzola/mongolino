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
| `collMod` | Partial | Updates or clears durable collection validator metadata for existing collections. Clearing uses `validator: {}`. | Only validator metadata, `validationLevel: "strict"`, and `validationAction: "error"` are supported. No index, view, TTL, timeseries, collation, or validation conversion behavior. |
| `drop` | Partial | Drops a collection by removing its documents, catalog entry, user index metadata, and maintained index entries. | No view, change-stream, or storage-stat side effects. |
| `dropDatabase` | Partial | Drops catalog entries and documents for the selected database only. | No users, roles, profiling collections, or storage statistics. |
| `count` | Partial | Counts documents matching the supported filter subset, with `skip` and `limit`. PyMongo `estimated_document_count()` uses this path. | No hint, collation, read concern, maxTimeMS, or storage-stat semantics. |
| `aggregate` | Partial | Runs a sequential read pipeline subset: `$match`, `$unwind`, `$group`, `$sort`, `$skip`, `$limit`, `$project`, and `$count`; supports bounded group keys and `$sum`, `$avg`, `$min`, `$max`, `$first`, `$last`, `$push`, and `$addToSet`; preserves the PyMongo `count_documents()` `$group` shape; returns cursor documents and supports per-client `cursor.batchSize` with `getMore`. | No general expression language, `$lookup`, `$facet`, `$addFields`, `$set`, `$unset`, `$replaceRoot`, `$out`, `$merge`, `$geoNear`, window stages, allowDiskUse, collation, hint, read concern, write concern, explain, maxTimeMS, or `let`. Unsupported shapes return command errors. |
| `distinct` | Partial | Returns unique scalar, dotted-path, and array-expanded values for documents matching the supported filter subset, ordered deterministically by BSON sort order. | No collation, hint, read concern, maxTimeMS, or complex array semantics beyond the documented matcher behavior. |
| `insert` | Partial | Accepts `documents`, assigns `_id` when missing, preserves existing documents on duplicate `_id`, reports duplicate key and validation `writeErrors`, supports ordered/unordered batches, and honors `bypassDocumentValidation: true`. | No write concern, retryable writes, or sessions beyond accepting `lsid`. |
| `find` | Partial | Returns `firstBatch` and creates a per-client server-side cursor when more shaped results remain. Supports exact matches, dotted paths, limited array traversal, `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$exists`, `$and`, `$or`, `$nor`, `$not`, projection, sort, skip, limit, capped batch size, and conservative scalar equality lookup through supported indexes. | No regex, `$where`, `$elemMatch`, geospatial/text search, collation, read concern, or tailable cursors. Unsupported operators return command errors. |
| `findAndModify` / `findandmodify` | Partial | Supports PyMongo `find_one_and_update`, `find_one_and_replace`, and `find_one_and_delete` for one document, with filter, deterministic sort, `fields`/`projection`, pre-image or post-image return, update/replacement upsert, supported update modifiers, `_id` immutability, validator enforcement, `bypassDocumentValidation: true`, unique-index enforcement, and maintained index entries. | No array filters, pipeline updates, positional updates, collation, hint, write concern, maxTimeMS, `let`, retryable writes, or transaction semantics. Unsupported shapes return command errors. |
| `getMore` | Partial | Returns `nextBatch` for live per-client cursors and closes cursors on exhaustion. | Cursor state is in memory, per connection, and snapshot-at-find-time. No cursor timeout, awaitData, or cross-connection cursor lookup. |
| `killCursors` | Partial | Removes live per-client cursors and reports `cursorsKilled` or `cursorsNotFound`. | No cross-connection cursor lookup. Malformed cursor ids return command errors. |
| `update` | Partial | Supports replacement updates; `$set`, `$unset`, `$inc`, `$rename`, `$min`, `$max`, `$mul`, `$setOnInsert`; array modifiers `$push`, `$addToSet`, `$pop`, `$pull`, and `$pullAll` for the documented subset; upsert; single-update; multi-update; ordered/unordered batches; `_id` immutability; validator enforcement; `bypassDocumentValidation: true`; and duplicate-key write errors. | No array filters, positional operators, update pipelines, `$push` `$position`/`$slice`/`$sort`, hints, collation, write concern, retryable writes, or transactions. |
| `delete` | Partial | Supports batch deletes with `q` and `limit`; `limit: 1` deletes one deterministic match and `limit: 0` deletes all matches. | No hints, collation, write concern, retryable writes, or explain behavior. |
| Cursors | Partial | `find` and `aggregate` store remaining results under a positive cursor id, PyMongo can iterate across multiple batches, exhausted cursors close with `id: 0`, and `killCursors` explicitly closes live cursors. | No cursor timeout. Cursor state is not durable and is scoped to one client connection. Invalid or exhausted cursor ids return explicit command errors for `getMore`. |
| BSON storage | Partial | Stores BSON blobs in SQLite, derives a stable primary key from `_id`, maintains scalar equality index entries for supported simple indexes, stores durable collection validator metadata, and enforces the supported validator subset on document-producing writes. Inserts with operator-shaped field names store those names as data. | No document size enforcement beyond message size, compound index planning, or range planning. |
| Authentication | Unsupported | No auth challenge or credential validation. | No SCRAM, x.509, keyfile, localhost exception, users, roles, or permissions. |
| Transactions | Unsupported | No multi-operation transaction protocol. | No sessions, transaction numbers, retryable writes, snapshot reads, or rollback semantics. |
| Indexes | Partial | Supports `createIndexes`, `listIndexes`, and `dropIndexes` metadata for simple ascending/descending single-field and compound keys, lists virtual `_id_`, enforces supported `unique: true` indexes across insert/update/upsert, maintains scalar equality entries for single-field indexes, and rejects unsupported index options explicitly. | Compound indexes are stored but not planned. Range planning is not implemented. Unique indexes reject array/multikey values. Text, geospatial, hashed, wildcard, partial, sparse, TTL, hidden, collation, and background semantics are unsupported. |

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

Projection supports inclusion or exclusion mode, with `_id` as the only allowed
mode override. Sort supports top-level or dotted fields with `1` or `-1`; missing
fields sort deterministically before present fields in ascending order.

Update paths support dotted document fields. Dotted updates through scalar
parents, conflicting paths such as `{ a: 1, "a.b": 2 }`, positional path
segments, attempts to change `_id`, and unsupported update operators are
rejected with write errors.

Supported update modifiers are intentionally bounded. Scalar modifiers include
`$set`, `$unset`, `$inc`, `$rename`, `$min`, `$max`, `$mul`, and `$setOnInsert`.
Array modifiers include `$push` with scalar values or `$each`, `$addToSet` with
scalar values or `$each`, `$pop` with `1` or `-1`, `$pull` with equality or the
supported matcher predicate subset, and `$pullAll` with scalar/document equality.
`$setOnInsert` applies only to inserted upserts. `$push` option documents using
`$position`, `$slice`, `$sort`, or unknown options are rejected explicitly, as
are update pipelines, array filters, and positional operators.

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
`$skip` before `$limit`. Unsupported stages and projection expressions return
command errors instead of being ignored. `$unwind` supports field-path strings
and document form with `path`, `preserveNullAndEmptyArrays`, and
`includeArrayIndex`. `$group` supports `_id` values of `null`, scalar literals,
field paths, and simple document key specs. Accumulator operands are field paths
or scalar literals where documented by tests; object, array, and operator
expressions remain unsupported.

## Development

```sh
cargo fmt -- --check
cargo test
cargo build
```

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
