import pytest
from bson.int64 import Int64
from pymongo import ReturnDocument
from pymongo.errors import WriteError


pytestmark = pytest.mark.e2e


def test_scalar_update_operators_happy_path(collection):
    collection.insert_one(
        {
            "_id": "u1",
            "age": 37,
            "score": 7,
            "multiplier": 4,
            "profile": {"city": "Rome"},
        }
    )

    result = collection.update_one(
        {"_id": "u1"},
        {
            "$rename": {"profile.city": "location"},
            "$min": {"age": 35, "floor": 4},
            "$max": {"score": 10, "ceiling": 8},
            "$mul": {"multiplier": 3, "missingProduct": 2},
            "$setOnInsert": {"created": True},
        },
    )

    assert result.matched_count == 1
    assert result.modified_count == 1
    doc = collection.find_one({"_id": "u1"})
    assert doc["location"] == "Rome"
    assert "city" not in doc["profile"]
    assert doc["age"] == 35
    assert doc["floor"] == 4
    assert doc["score"] == 10
    assert doc["ceiling"] == 8
    assert doc["multiplier"] == 12
    assert doc["missingProduct"] == 0
    assert "created" not in doc


def test_set_on_insert_only_applies_to_upsert_inserts(collection):
    collection.insert_one({"_id": "u1", "name": "Ada"})

    existing = collection.update_one(
        {"_id": "u1"},
        {"$set": {"name": "Ada Lovelace"}, "$setOnInsert": {"created": True}},
    )
    assert existing.matched_count == 1
    assert "created" not in collection.find_one({"_id": "u1"})

    inserted = collection.update_one(
        {"_id": "u2", "email": "new@example.test"},
        {
            "$set": {"name": "New"},
            "$setOnInsert": {"created": True},
            "$mul": {"count": 2},
        },
        upsert=True,
    )
    assert inserted.upserted_id == "u2"
    doc = collection.find_one({"_id": "u2"})
    assert doc["email"] == "new@example.test"
    assert doc["created"] is True
    assert doc["count"] == 0


def test_scalar_update_operators_find_one_and_update(collection):
    collection.insert_one(
        {"_id": "u1", "age": 37, "score": 7, "profile": {"city": "Rome"}}
    )

    doc = collection.find_one_and_update(
        {"_id": "u1"},
        {
            "$rename": {"profile.city": "city"},
            "$mul": {"age": 2},
            "$max": {"score": 10},
            "$setOnInsert": {"created": True},
        },
        return_document=ReturnDocument.AFTER,
    )

    assert doc["city"] == "Rome"
    assert doc["age"] == 74
    assert doc["score"] == 10
    assert "created" not in doc


def test_scalar_update_operator_errors_preserve_documents(collection):
    collection.insert_one(
        {
            "_id": "u1",
            "name": "Ada",
            "count": "many",
            "profile": {"city": "Rome"},
        }
    )

    bad_updates = [
        {"$rename": {"name": 5}},
        {"$rename": {"_id": "other"}},
        {"$rename": {"name": "_id"}},
        {"$rename": {"profile": "profile.city"}},
        {"$rename": {"items.$.name": "name"}},
        {"$mul": {"count": 2}},
        {"$mul": {"count": "bad"}},
        {"$set": {"created": False}, "$setOnInsert": {"created": True}},
    ]
    for update in bad_updates:
        with pytest.raises(WriteError):
            collection.update_one({"_id": "u1"}, update)

    assert collection.find_one({"_id": "u1"}) == {
        "_id": "u1",
        "name": "Ada",
        "count": "many",
        "profile": {"city": "Rome"},
    }


def test_mul_overflow_is_error_and_preserves_existing_data(collection):
    collection.insert_one({"_id": "overflow", "value": Int64(9223372036854775807)})

    with pytest.raises(WriteError):
        collection.update_one({"_id": "overflow"}, {"$mul": {"value": 2}})

    assert collection.find_one({"_id": "overflow"})["value"] == 9223372036854775807


def test_array_update_operators_happy_path_and_update_many(collection):
    collection.insert_many(
        [
            {
                "_id": "u1",
                "active": True,
                "tags": ["math"],
                "batch": [],
                "unique": ["math"],
                "numbers": [1, 2, 3],
                "scores": [1, 3, 5],
                "docs": [{"kind": "a"}, {"kind": "b"}],
                "letters": ["x", "y", "z"],
            },
            {"_id": "u2", "active": True},
        ]
    )

    result = collection.update_one(
        {"_id": "u1"},
        {
            "$push": {"tags": "logic", "batch": {"$each": ["a", "b"]}},
            "$addToSet": {"unique": {"$each": ["math", "logic"]}},
            "$pop": {"numbers": 1},
            "$pull": {"scores": {"$gte": 3}, "docs": {"kind": "a"}},
            "$pullAll": {"letters": ["x", "z"]},
        },
    )
    assert result.modified_count == 1

    many = collection.update_many({"active": True}, {"$push": {"events": "seen"}})
    assert many.matched_count == 2
    assert many.modified_count == 2

    doc = collection.find_one({"_id": "u1"})
    assert doc["tags"] == ["math", "logic"]
    assert doc["batch"] == ["a", "b"]
    assert doc["unique"] == ["math", "logic"]
    assert doc["numbers"] == [1, 2]
    assert doc["scores"] == [1]
    assert doc["docs"] == [{"kind": "b"}]
    assert doc["letters"] == ["y"]
    assert doc["events"] == ["seen"]
    assert collection.find_one({"_id": "u2"})["events"] == ["seen"]


def test_array_update_operators_find_one_and_update(collection):
    collection.insert_one(
        {
            "_id": "u1",
            "tags": ["math"],
            "unique": ["math"],
            "numbers": [1, 2],
            "scores": [1, 4],
            "letters": ["x", "y"],
        }
    )

    doc = collection.find_one_and_update(
        {"_id": "u1"},
        {
            "$push": {"tags": {"$each": ["logic", "systems"]}},
            "$addToSet": {"unique": {"$each": ["math", "logic"]}},
            "$pop": {"numbers": -1},
            "$pull": {"scores": {"$gt": 2}},
            "$pullAll": {"letters": ["x"]},
        },
        return_document=ReturnDocument.AFTER,
    )

    assert doc["tags"] == ["math", "logic", "systems"]
    assert doc["unique"] == ["math", "logic"]
    assert doc["numbers"] == [2]
    assert doc["scores"] == [1]
    assert doc["letters"] == ["y"]


def test_array_update_operator_errors_preserve_documents(collection):
    collection.insert_one({"_id": "u1", "tags": [], "name": "Ada", "profile": "flat"})

    bad_updates = [
        {"$push": {"tags": {"$each": ["x"], "$position": 0}}},
        {"$push": {"tags": {"$slice": 1}}},
        {"$push": {"tags": {"$sort": 1}}},
        {"$push": {"tags": {"$each": "x"}}},
        {"$addToSet": {"tags": {"$each": "x"}}},
        {"$pop": {"tags": 0}},
        {"$pullAll": {"tags": "x"}},
        {"$push": {"name": "x"}},
        {"$pull": {"name": "Ada"}},
        {"$push": {"profile.tags": "x"}},
        {"$push": {"tags.$": "x"}},
    ]
    for update in bad_updates:
        with pytest.raises(WriteError):
            collection.update_one({"_id": "u1"}, update)

    assert collection.find_one({"_id": "u1"}) == {
        "_id": "u1",
        "tags": [],
        "name": "Ada",
        "profile": "flat",
    }
