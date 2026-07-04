import pytest
from bson import ObjectId
from bson.int64 import Int64
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError, DuplicateKeyError, WriteError


pytestmark = pytest.mark.e2e


def ids(cursor):
    return [doc["_id"] for doc in cursor]


def test_insert_one_with_explicit_id(collection):
    result = collection.insert_one({"_id": "u1", "name": "Ada"})

    assert result.inserted_id == "u1"
    assert collection.find_one({"_id": "u1"})["name"] == "Ada"


def test_insert_one_generates_object_id(collection):
    result = collection.insert_one({"name": "Generated"})

    assert isinstance(result.inserted_id, ObjectId)
    assert collection.find_one({"_id": result.inserted_id})["name"] == "Generated"


def test_insert_many_ordered_duplicate_stops_and_preserves_original(collection):
    collection.insert_one({"_id": "u1", "name": "Original"})

    with pytest.raises(BulkWriteError) as excinfo:
        collection.insert_many(
            [
                {"_id": "u2", "name": "Before"},
                {"_id": "u1", "name": "Duplicate"},
                {"_id": "u3", "name": "After"},
            ],
            ordered=True,
        )

    details = excinfo.value.details
    assert details["nInserted"] == 1
    assert details["writeErrors"][0]["code"] == 11000
    assert collection.find_one({"_id": "u1"})["name"] == "Original"
    assert ids(collection.find({}, projection={"_id": 1}).sort("_id", ASCENDING)) == ["u1", "u2"]


def test_insert_many_unordered_duplicate_continues(collection):
    collection.insert_one({"_id": "u1", "name": "Original"})

    with pytest.raises(BulkWriteError) as excinfo:
        collection.insert_many(
            [
                {"_id": "u2", "name": "Before"},
                {"_id": "u1", "name": "Duplicate"},
                {"_id": "u3", "name": "After"},
            ],
            ordered=False,
        )

    details = excinfo.value.details
    assert details["nInserted"] == 2
    assert details["writeErrors"][0]["code"] == 11000
    assert collection.find_one({"_id": "u1"})["name"] == "Original"
    assert ids(collection.find({}, projection={"_id": 1}).sort("_id", ASCENDING)) == [
        "u1",
        "u2",
        "u3",
    ]


def test_find_by_id_equality_dotted_paths_and_operators(collection):
    seed_users(collection)

    assert collection.find_one({"_id": "u1"})["name"] == "Ada"
    assert ids(collection.find({"active": False}).sort("_id", ASCENDING)) == ["u2"]
    assert ids(collection.find({"profile.city": "Rome"}).sort("_id", ASCENDING)) == ["u1", "u3"]
    assert ids(collection.find({"tags": "logic"})) == ["u1"]

    assert ids(collection.find({"age": {"$eq": 37}})) == ["u1"]
    assert ids(collection.find({"age": {"$ne": 39}}).sort("_id", ASCENDING)) == ["u1", "u3"]
    assert ids(collection.find({"age": {"$gt": 38}}).sort("_id", ASCENDING)) == ["u2", "u3"]
    assert ids(collection.find({"age": {"$gte": 39, "$lte": 41}}).sort("_id", ASCENDING)) == [
        "u2",
        "u3",
    ]
    assert ids(collection.find({"name": {"$in": ["Ada", "Katherine"]}}).sort("_id", ASCENDING)) == [
        "u1",
        "u3",
    ]
    assert ids(collection.find({"name": {"$nin": ["Ada", "Grace"]}})) == ["u3"]
    assert ids(collection.find({"score": {"$exists": False}}).sort("_id", ASCENDING)) == [
        "u2",
        "u3",
    ]


def test_find_logical_operators(collection):
    seed_users(collection)

    assert ids(collection.find({"$and": [{"active": True}, {"age": {"$lt": 40}}]})) == ["u1"]
    assert ids(collection.find({"$or": [{"name": "Ada"}, {"name": "Grace"}]}).sort("_id", ASCENDING)) == [
        "u1",
        "u2",
    ]
    assert ids(collection.find({"$nor": [{"profile.city": "Rome"}]})) == ["u2"]
    assert ids(collection.find({"age": {"$not": {"$lt": 39}}}).sort("_id", ASCENDING)) == ["u2", "u3"]


def test_find_projection_edges(collection):
    seed_users(collection)

    no_id = collection.find_one({"_id": "u1"}, projection={"_id": 0})
    assert no_id == {
        "name": "Ada",
        "age": 37,
        "active": True,
        "profile": {"city": "Rome"},
        "tags": ["math", "logic"],
        "score": 7,
    }

    only_id = collection.find_one({"_id": "u1"}, projection={"_id": 1})
    assert only_id == {"_id": "u1"}

    included_without_id = collection.find_one(
        {"_id": "u1"},
        projection={"name": 1, "profile.city": 1, "_id": 0},
    )
    assert included_without_id == {"name": "Ada", "profile": {"city": "Rome"}}


def test_find_sort_skip_limit_and_batch_size_cursor_iteration(collection):
    seed_users(collection)

    assert ids(collection.find({}).sort("age", DESCENDING).skip(1).limit(1)) == ["u2"]
    assert ids(collection.find({}).sort("_id", ASCENDING).batch_size(1)) == ["u1", "u2", "u3"]
    assert ids(collection.find({}).sort("_id", ASCENDING).limit(2).batch_size(1)) == ["u1", "u2"]
    assert ids(collection.find({}).sort("_id", ASCENDING).batch_size(2)) == ["u1", "u2", "u3"]


def test_find_command_get_more_batches(collection):
    seed_users(collection)

    first = collection.database.command(
        {
            "find": collection.name,
            "filter": {},
            "sort": {"_id": 1},
            "batchSize": 1,
        }
    )
    cursor = first["cursor"]
    assert cursor["id"] > 0
    assert [doc["_id"] for doc in cursor["firstBatch"]] == ["u1"]

    second = collection.database.command(
        {
            "getMore": cursor["id"],
            "collection": collection.name,
            "batchSize": 1,
        }
    )
    assert second["cursor"]["id"] == cursor["id"]
    assert [doc["_id"] for doc in second["cursor"]["nextBatch"]] == ["u2"]

    final = collection.database.command(
        {
            "getMore": cursor["id"],
            "collection": collection.name,
            "batchSize": 10,
        }
    )
    assert final["cursor"]["id"] == 0
    assert [doc["_id"] for doc in final["cursor"]["nextBatch"]] == ["u3"]


def test_update_one_set_and_unset(collection):
    seed_users(collection)

    set_result = collection.update_one({"_id": "u1"}, {"$set": {"name": "Ada Lovelace"}})
    assert set_result.matched_count == 1
    assert set_result.modified_count == 1

    unset_result = collection.update_one({"_id": "u1"}, {"$unset": {"score": ""}})
    assert unset_result.matched_count == 1
    assert unset_result.modified_count == 1

    doc = collection.find_one({"_id": "u1"})
    assert doc["name"] == "Ada Lovelace"
    assert "score" not in doc


def test_update_many_inc(collection):
    seed_users(collection)

    result = collection.update_many({"active": True}, {"$inc": {"score": 2}})

    assert result.matched_count == 2
    assert result.modified_count == 2
    assert collection.find_one({"_id": "u1"})["score"] == 9
    assert collection.find_one({"_id": "u3"})["score"] == 2


def test_update_targets_id_indexed_scalar_and_fallback_filters(collection):
    seed_users(collection)
    collection.create_index([("profile.city", ASCENDING)], name="city_1")
    collection.create_index([("active", ASCENDING)], name="active_1")

    by_id = collection.update_one({"_id": {"$eq": "u2"}}, {"$set": {"name": "Grace Hopper"}})
    assert by_id.matched_count == 1
    assert by_id.modified_count == 1

    by_index = collection.update_one(
        {"profile.city": "Rome"},
        {"$set": {"team": "math"}},
    )
    assert by_index.matched_count == 1
    assert by_index.modified_count == 1

    many_by_index = collection.update_many({"active": True}, {"$inc": {"score": 1}})
    assert many_by_index.matched_count == 2
    assert many_by_index.modified_count == 2

    fallback = collection.update_one(
        {"$or": [{"name": "Nobody"}, {"age": 41}]},
        {"$set": {"team": "fallback"}},
    )
    assert fallback.matched_count == 1
    assert fallback.modified_count == 1

    assert collection.find_one({"_id": "u2"})["name"] == "Grace Hopper"
    assert collection.find_one({"_id": "u1"})["team"] == "math"
    assert collection.find_one({"_id": "u1"})["score"] == 8
    assert collection.find_one({"_id": "u3"})["team"] == "fallback"
    assert collection.find_one({"_id": "u3"})["score"] == 1


def test_update_targets_compound_indexed_filters(collection):
    seed_users(collection)
    collection.create_index([("profile.city", ASCENDING), ("active", ASCENDING)], name="city_active_1")

    one = collection.update_one(
        {"profile.city": "Rome", "active": True},
        {"$set": {"team": "compound-one"}},
    )
    assert one.matched_count == 1
    assert one.modified_count == 1
    assert collection.find_one({"_id": "u1"})["team"] == "compound-one"

    many = collection.update_many(
        {"profile.city": "Rome", "active": True},
        {"$inc": {"score": 1}},
    )
    assert many.matched_count == 2
    assert many.modified_count == 2
    assert collection.find_one({"_id": "u1"})["score"] == 8
    assert collection.find_one({"_id": "u3"})["score"] == 1

    fallback_numeric = collection.update_one(
        {"profile.city": "Rome", "active": 1},
        {"$set": {"team": "numeric-fallback"}},
    )
    assert fallback_numeric.matched_count == 0


def test_update_targets_array_backed_matches_when_index_entries_are_incomplete(collection):
    collection.insert_many(
        [
            {"_id": "u1", "tags": ["math"], "active": True},
            {"_id": "u2", "tags": "math", "active": True},
            {"_id": "u3", "tags": "math", "active": False},
        ]
    )
    collection.create_index([("tags", ASCENDING)], name="tags_1")
    collection.create_index([("tags", ASCENDING), ("active", ASCENDING)], name="tags_active_1")

    one = collection.update_one({"tags": "math", "active": True}, {"$set": {"seen": True}})
    assert one.matched_count == 1
    assert collection.find_one({"_id": "u1"})["seen"] is True

    many = collection.update_many({"tags": "math"}, {"$set": {"touched": True}})
    assert many.matched_count == 3
    assert ids(collection.find({"touched": True}).sort("_id", ASCENDING)) == ["u1", "u2", "u3"]


def test_replacement_update_and_upsert(collection):
    seed_users(collection)

    replacement = collection.replace_one({"_id": "u1"}, {"name": "Ada Replaced"})
    assert replacement.matched_count == 1
    assert collection.find_one({"_id": "u1"}) == {"_id": "u1", "name": "Ada Replaced"}

    upsert = collection.update_one(
        {"_id": "u4"},
        {"$set": {"name": "Mary"}, "$inc": {"score": 1}},
        upsert=True,
    )
    assert upsert.upserted_id == "u4"
    assert collection.find_one({"_id": "u4"})["score"] == 1


def test_update_errors_preserve_existing_data(collection):
    seed_users(collection)

    with pytest.raises(WriteError):
        collection.update_one({"_id": "u1"}, {"$set": {"_id": "changed"}})
    assert collection.find_one({"_id": "u1"})["name"] == "Ada"

    with pytest.raises(DuplicateKeyError):
        collection.replace_one({"name": "New"}, {"_id": "u1", "name": "New"}, upsert=True)
    assert collection.find_one({"_id": "u1"})["name"] == "Ada"
    assert collection.find_one({"name": "New"}) is None


def test_inc_overflow_is_error_and_preserves_existing_data(collection):
    collection.insert_one({"_id": "n1", "value": Int64(9223372036854775807)})

    with pytest.raises(WriteError):
        collection.update_one({"_id": "n1"}, {"$inc": {"value": 1}})

    assert collection.find_one({"_id": "n1"})["value"] == 9223372036854775807


def test_delete_one_many_and_repeated_noop(collection):
    seed_users(collection)

    one = collection.delete_one({"profile.city": "Rome"})
    assert one.deleted_count == 1
    assert ids(collection.find({"profile.city": "Rome"})) == ["u3"]

    many = collection.delete_many({"active": True})
    assert many.deleted_count == 1
    assert ids(collection.find({})) == ["u2"]

    missing = collection.delete_one({"_id": "u1"})
    assert missing.deleted_count == 0


def test_delete_targets_id_indexed_scalar_and_fallback_filters(collection):
    seed_users(collection)
    collection.create_index([("profile.city", ASCENDING)], name="city_1")
    collection.create_index([("active", ASCENDING)], name="active_1")

    by_id = collection.delete_one({"_id": {"$eq": "u2"}})
    assert by_id.deleted_count == 1

    by_index = collection.delete_one({"profile.city": "Rome"})
    assert by_index.deleted_count == 1

    fallback = collection.delete_many({"$or": [{"name": "Nobody"}, {"age": 41}]})
    assert fallback.deleted_count == 1
    assert ids(collection.find({})) == []


def test_delete_targets_compound_indexed_filters(collection):
    seed_users(collection)
    collection.create_index([("profile.city", ASCENDING), ("active", ASCENDING)], name="city_active_1")

    one = collection.delete_one({"profile.city": "Rome", "active": True})
    assert one.deleted_count == 1
    assert ids(collection.find({"profile.city": "Rome", "active": True})) == ["u3"]

    many = collection.delete_many({"profile.city": "Rome", "active": True})
    assert many.deleted_count == 1
    assert ids(collection.find({})) == ["u2"]


def test_delete_targets_array_backed_matches_when_index_entries_are_incomplete(collection):
    collection.insert_many(
        [
            {"_id": "u1", "tags": ["math"], "active": True},
            {"_id": "u2", "tags": "math", "active": True},
            {"_id": "u3", "tags": "math", "active": False},
        ]
    )
    collection.create_index([("tags", ASCENDING)], name="tags_1")
    collection.create_index([("tags", ASCENDING), ("active", ASCENDING)], name="tags_active_1")

    one = collection.delete_one({"tags": "math", "active": True})
    assert one.deleted_count == 1
    assert ids(collection.find({"tags": "math"}).sort("_id", ASCENDING)) == ["u2", "u3"]

    many = collection.delete_many({"tags": "math"})
    assert many.deleted_count == 2
    assert ids(collection.find({})) == []


def seed_users(collection):
    collection.insert_many(
        [
            {
                "_id": "u1",
                "name": "Ada",
                "age": 37,
                "active": True,
                "profile": {"city": "Rome"},
                "tags": ["math", "logic"],
                "score": 7,
            },
            {
                "_id": "u2",
                "name": "Grace",
                "age": 39,
                "active": False,
                "profile": {"city": "London"},
                "tags": ["compiler"],
            },
            {
                "_id": "u3",
                "name": "Katherine",
                "age": 41,
                "active": True,
                "profile": {"city": "Rome"},
            },
        ]
    )
