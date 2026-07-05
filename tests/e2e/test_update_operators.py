import pytest
from bson.int64 import Int64
from pymongo import ASCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError, WriteError


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


def test_update_pipeline_subset_update_many_upsert_and_errors(collection):
    collection.insert_many(
        [
            {
                "_id": "u1",
                "active": True,
                "first": "Ada",
                "last": "Lovelace",
                "score": 2,
            },
            {
                "_id": "u2",
                "active": True,
                "first": "Grace",
                "last": "Hopper",
                "score": 3,
            },
        ]
    )

    result = collection.update_many(
        {"active": True},
        [
            {
                "$set": {
                    "full": {"$concat": ["$first", " ", "$last"]},
                    "doubleScore": {"$multiply": ["$score", 2]},
                }
            },
            {"$unset": "last"},
        ],
    )
    assert result.matched_count == 2
    assert result.modified_count == 2
    assert collection.find_one({"_id": "u1"})["full"] == "Ada Lovelace"
    assert collection.find_one({"_id": "u2"})["doubleScore"] == 6
    assert "last" not in collection.find_one({"_id": "u1"})

    upsert = collection.update_one(
        {"_id": "u3", "first": "Katherine"},
        [{"$set": {"full": {"$concat": ["$first", " Johnson"]}, "score": 5}}],
        upsert=True,
    )
    assert upsert.upserted_id == "u3"
    assert collection.find_one({"_id": "u3"})["full"] == "Katherine Johnson"

    with pytest.raises(WriteError):
        collection.update_one({"_id": "u2"}, [{"$set": {"_id": "changed"}}])
    with pytest.raises(WriteError):
        collection.update_one({"_id": "u2"}, [{"$lookup": {"from": "x"}}])
    assert collection.find_one({"_id": "u2"})["_id"] == "u2"


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
                "docs": [
                    {"kind": "a", "score": 1, "meta": {"flag": False}},
                    {"kind": "a", "score": 3, "meta": {"flag": True}},
                    {"kind": "b", "score": 4, "meta": {"flag": True}},
                    {"kind": "c", "score": 2, "meta": {"flag": True}},
                ],
                "letters": ["x", "y", "z"],
            },
            {
                "_id": "u2",
                "active": True,
                "docs": [
                    {"kind": "a", "score": 4},
                    {"kind": "b", "score": 2},
                ],
            },
        ]
    )

    result = collection.update_one(
        {"_id": "u1"},
        {
            "$push": {"tags": "logic", "batch": {"$each": ["a", "b"]}},
            "$addToSet": {"unique": {"$each": ["math", "logic"]}},
            "$pop": {"numbers": 1},
            "$pull": {
                "scores": {"$gte": 3},
                "docs": {"kind": "a", "score": {"$gte": 2}},
            },
            "$pullAll": {"letters": ["x", "z"]},
        },
    )
    assert result.modified_count == 1

    many = collection.update_many(
        {"active": True},
        {"$push": {"events": "seen"}, "$pull": {"docs": {"kind": "b"}}},
    )
    assert many.matched_count == 2
    assert many.modified_count == 2

    doc = collection.find_one({"_id": "u1"})
    assert doc["tags"] == ["math", "logic"]
    assert doc["batch"] == ["a", "b"]
    assert doc["unique"] == ["math", "logic"]
    assert doc["numbers"] == [1, 2]
    assert doc["scores"] == [1]
    assert doc["docs"] == [
        {"kind": "a", "score": 1, "meta": {"flag": False}},
        {"kind": "c", "score": 2, "meta": {"flag": True}},
    ]
    assert doc["letters"] == ["y"]
    assert doc["events"] == ["seen"]
    assert collection.find_one({"_id": "u2"}) == {
        "_id": "u2",
        "active": True,
        "docs": [{"kind": "a", "score": 4}],
        "events": ["seen"],
    }


def test_positional_first_and_all_update_subset(collection):
    collection.insert_many(
        [
            {
                "_id": "o1",
                "active": True,
                "items": [
                    {"kind": "open", "status": "new", "score": 1},
                    {"kind": "closed", "status": "done", "score": 5},
                    {"kind": "open", "status": "new", "score": 3},
                ],
            },
            {
                "_id": "o2",
                "active": True,
                "items": [
                    {"kind": "open", "status": "new", "score": 2},
                    {"kind": "closed", "status": "done", "score": 4},
                ],
            },
        ]
    )

    first = collection.update_one(
        {"items.kind": "open"},
        {"$set": {"items.$.status": "working"}, "$inc": {"items.$.score": 2}},
    )
    assert first.matched_count == 1
    assert first.modified_count == 1

    all_result = collection.update_many(
        {"active": True},
        {"$mul": {"items.$[].score": 2}},
    )
    assert all_result.matched_count == 2
    assert all_result.modified_count == 2

    assert collection.find_one({"_id": "o1"})["items"] == [
        {"kind": "open", "status": "working", "score": 6},
        {"kind": "closed", "status": "done", "score": 10},
        {"kind": "open", "status": "new", "score": 6},
    ]
    assert collection.find_one({"_id": "o2"})["items"] == [
        {"kind": "open", "status": "new", "score": 4},
        {"kind": "closed", "status": "done", "score": 8},
    ]

    before = collection.find_one({"_id": "o1"})
    with pytest.raises(WriteError):
        collection.update_one(
            {"_id": "o1"},
            {"$inc": {"items.$[].status": 1}},
        )
    assert collection.find_one({"_id": "o1"}) == before


def test_positional_first_scalar_elem_match_update_subset(collection):
    collection.insert_one(
        {
            "_id": "u1",
            "scores": [1, 5, 7, 11],
            "tags": ["Alpha", "BETA", "beta"],
        }
    )

    numeric = collection.update_one(
        {"scores": {"$elemMatch": {"$gte": 5, "$lt": 10}}},
        {"$set": {"scores.$": 99}},
    )
    assert numeric.matched_count == 1
    assert numeric.modified_count == 1

    collated = collection.update_one(
        {"tags": {"$elemMatch": {"$eq": "beta"}}},
        {"$set": {"tags.$": "MATCH"}},
        collation={"locale": "en", "strength": 2},
    )
    assert collated.modified_count == 1

    assert collection.find_one({"_id": "u1"}) == {
        "_id": "u1",
        "scores": [1, 99, 7, 11],
        "tags": ["Alpha", "MATCH", "beta"],
    }


def test_positional_first_scalar_elem_match_errors_do_not_mutate(collection):
    collection.insert_one({"_id": "u1", "scores": [1, 5, 7]})
    before = collection.find_one({"_id": "u1"})

    with pytest.raises(WriteError):
        collection.update_one(
            {"scores": {"$elemMatch": {"$where": "bad"}}},
            {"$set": {"scores.$": 99}},
        )

    assert collection.find_one({"_id": "u1"}) == before


def test_array_filters_update_subset_and_errors(collection):
    collection.insert_one(
        {
            "_id": "o1",
            "items": [
                {"kind": "open", "status": "new", "score": 1},
                {"kind": "closed", "status": "done", "score": 5},
                {"kind": "open", "status": "new", "score": 3},
            ],
        }
    )

    result = collection.update_one(
        {"_id": "o1"},
        {
            "$set": {"items.$[open].status": "closed"},
            "$max": {"items.$[open].score": 10},
        },
        array_filters=[{"open.kind": "open"}],
    )
    assert result.modified_count == 1
    assert collection.find_one({"_id": "o1"})["items"] == [
        {"kind": "open", "status": "closed", "score": 10},
        {"kind": "closed", "status": "done", "score": 5},
        {"kind": "open", "status": "closed", "score": 10},
    ]

    before = collection.find_one({"_id": "o1"})
    for kwargs in [
        {"array_filters": [{"open.kind": "open"}, {"open.score": {"$gte": 1}}]},
        {"array_filters": [{"unused.kind": "open"}]},
        {"array_filters": [{"Open.kind": "open"}]},
    ]:
        with pytest.raises(WriteError):
            collection.update_one(
                {"_id": "o1"},
                {"$set": {"items.$[open].status": "bad"}},
                **kwargs,
            )
    with pytest.raises(WriteError):
        collection.update_one(
            {"_id": "o1"},
            {"$set": {"items.$[open].status": "bad"}},
        )
    assert collection.find_one({"_id": "o1"}) == before


def test_pull_document_arrays_supports_logical_predicates(collection):
    collection.insert_many(
        [
            {
                "_id": "or",
                "items": [
                    {"kind": "active", "score": 5},
                    {"kind": "archived", "score": 2},
                    {"kind": "active", "score": 0},
                    {"kind": "review", "score": 3},
                ],
            },
            {
                "_id": "and",
                "items": [
                    {"kind": "active", "score": 1},
                    {"kind": "active", "score": 4},
                    {"kind": "archived", "score": 1},
                ],
            },
            {
                "_id": "nor",
                "items": [
                    {"kind": "active", "score": 5},
                    {"kind": "archived", "score": 2},
                    {"kind": "active", "score": 0},
                ],
            },
            {
                "_id": "none",
                "items": [
                    {"kind": "active", "score": 5},
                    {"kind": "review", "score": 3},
                ],
            },
            {
                "_id": "preserve",
                "scores": [1, 3, 5],
                "docs": [{"kind": "a"}, {"kind": "b"}],
            },
        ]
    )

    assert (
        collection.update_one(
            {"_id": "or"},
            {
                "$pull": {
                    "items": {
                        "$or": [{"kind": "archived"}, {"score": {"$lte": 0}}]
                    }
                }
            },
        ).modified_count
        == 1
    )
    assert (
        collection.update_one(
            {"_id": "and"},
            {
                "$pull": {
                    "items": {
                        "$and": [{"kind": "active"}, {"score": {"$lte": 1}}]
                    }
                }
            },
        ).modified_count
        == 1
    )
    assert (
        collection.update_one(
            {"_id": "nor"},
            {
                "$pull": {
                    "items": {
                        "$nor": [{"kind": "archived"}, {"score": {"$lte": 0}}]
                    }
                }
            },
        ).modified_count
        == 1
    )
    assert (
        collection.update_one(
            {"_id": "none"},
            {"$pull": {"items": {"$or": [{"kind": "missing"}]}}},
        ).modified_count
        == 0
    )
    assert (
        collection.update_one(
            {"_id": "preserve"},
            {"$pull": {"scores": {"$gte": 3}, "docs": {"$eq": {"kind": "a"}}}},
        ).modified_count
        == 1
    )

    assert collection.find_one({"_id": "or"})["items"] == [
        {"kind": "active", "score": 5},
        {"kind": "review", "score": 3},
    ]
    assert collection.find_one({"_id": "and"})["items"] == [
        {"kind": "active", "score": 4},
        {"kind": "archived", "score": 1},
    ]
    assert collection.find_one({"_id": "nor"})["items"] == [
        {"kind": "archived", "score": 2},
        {"kind": "active", "score": 0},
    ]
    assert collection.find_one({"_id": "none"})["items"] == [
        {"kind": "active", "score": 5},
        {"kind": "review", "score": 3},
    ]
    assert collection.find_one({"_id": "preserve"}) == {
        "_id": "preserve",
        "scores": [1],
        "docs": [{"kind": "b"}],
    }


def test_array_update_operators_find_one_and_update(collection):
    collection.insert_one(
        {
            "_id": "u1",
            "tags": ["math"],
            "unique": ["math"],
            "numbers": [1, 2],
            "scores": [1, 4],
            "docs": [
                {"kind": "a", "score": 1},
                {"kind": "a", "score": 3},
                {"kind": "b", "score": 5},
            ],
            "letters": ["x", "y"],
        }
    )

    doc = collection.find_one_and_update(
        {"_id": "u1"},
        {
            "$push": {"tags": {"$each": ["logic", "systems"]}},
            "$addToSet": {"unique": {"$each": ["math", "logic"]}},
            "$pop": {"numbers": -1},
            "$pull": {"scores": {"$gt": 2}, "docs": {"kind": "a", "score": {"$gte": 2}}},
            "$pullAll": {"letters": ["x"]},
        },
        return_document=ReturnDocument.AFTER,
    )

    assert doc["tags"] == ["math", "logic", "systems"]
    assert doc["unique"] == ["math", "logic"]
    assert doc["numbers"] == [2]
    assert doc["scores"] == [1]
    assert doc["docs"] == [{"kind": "a", "score": 1}, {"kind": "b", "score": 5}]
    assert doc["letters"] == ["y"]


def test_array_update_operator_errors_preserve_documents(collection):
    collection.insert_one(
        {
            "_id": "u1",
            "tags": [],
            "name": "Ada",
            "profile": "flat",
            "docs": [{"name": "Ada"}, {"name": "Grace"}],
        }
    )

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
        {"$pull": {"docs": {"name": {"$regex": "^A", "$options": "x"}}}},
        {"$pull": {"docs": {"$or": [{"name": "Ada"}], "$where": "bad"}}},
        {"$pull": {"docs": {"$and": []}}},
        {"$pull": {"docs": {"$nor": [{"name": "Ada"}, "bad"]}}},
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
        "docs": [{"name": "Ada"}, {"name": "Grace"}],
    }


def test_new_update_modifiers_preserve_validation_unique_and_indexes(mongo_client):
    db = mongo_client["update_operator_invariants"]
    collection = db.create_collection(
        "users",
        validator={
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["name"],
                "properties": {
                    "name": {"bsonType": "string"},
                    "age": {"bsonType": "number"},
                },
            }
        },
    )
    collection.insert_one({"_id": "u1", "name": "Ada", "age": 37})

    with pytest.raises(WriteError) as invalid:
        collection.update_one({"_id": "u1"}, {"$rename": {"age": "name"}})
    assert invalid.value.code == 121
    assert collection.find_one({"_id": "u1"})["name"] == "Ada"

    bypassed = db.command(
        {
            "update": "users",
            "bypassDocumentValidation": True,
            "updates": [{"q": {"_id": "u1"}, "u": {"$rename": {"age": "name"}}}],
        }
    )
    assert bypassed["nModified"] == 1
    assert collection.find_one({"_id": "u1"})["name"] == 37

    unique = mongo_client["update_operator_unique"].users
    unique.create_index([("email", ASCENDING)], unique=True)
    unique.create_index([("rank", ASCENDING)], unique=True)
    unique.insert_many(
        [
            {"_id": "u1", "email": "ada@example.test", "rank": 1},
            {"_id": "u2", "altEmail": "ada@example.test", "rank": 5},
        ]
    )
    with pytest.raises(DuplicateKeyError):
        unique.update_one({"_id": "u2"}, {"$rename": {"altEmail": "email"}})
    with pytest.raises(DuplicateKeyError):
        unique.update_one({"_id": "u2"}, {"$min": {"rank": 1}})
    with pytest.raises(DuplicateKeyError):
        unique.update_one(
            {"_id": "u3"},
            {"$setOnInsert": {"email": "ada@example.test"}},
            upsert=True,
        )
    with pytest.raises(WriteError) as array_update:
        unique.update_one({"_id": "u2"}, {"$set": {"email": ["array@example.test"]}})
    assert array_update.value.code == 72
    assert "does not support array value" in str(array_update.value)
    with pytest.raises(WriteError) as array_upsert:
        unique.update_one(
            {"_id": "u4"},
            {"$setOnInsert": {"email": ["array@example.test"]}},
            upsert=True,
        )
    assert array_upsert.value.code == 72
    assert "does not support array value" in str(array_upsert.value)
    with pytest.raises(WriteError) as array_insert:
        unique.insert_one({"_id": "u5", "email": ["array@example.test"]})
    assert array_insert.value.code == 72
    assert "does not support array value" in str(array_insert.value)

    sparse = mongo_client["update_operator_sparse_unique"].users
    sparse.create_index([("email", ASCENDING)], unique=True, sparse=True)
    sparse.insert_many(
        [
            {"_id": "s1", "name": "missing"},
            {"_id": "s2", "altEmail": "ada@example.test"},
            {"_id": "s3", "email": "taken@example.test"},
        ]
    )
    sparse.update_one({"_id": "s2"}, {"$rename": {"altEmail": "email"}})
    with pytest.raises(DuplicateKeyError):
        sparse.update_one({"_id": "s1"}, {"$set": {"email": "ada@example.test"}})
    sparse.update_one({"_id": "s2"}, {"$unset": {"email": ""}})
    sparse.update_one({"_id": "s1"}, {"$set": {"email": "ada@example.test"}})

    numeric = mongo_client["update_operator_numeric_unique"].users
    numeric.create_index([("n", ASCENDING)], unique=True)
    numeric.insert_many([{"_id": "n1", "n": 1}, {"_id": "n2", "n": 2}])
    with pytest.raises(DuplicateKeyError):
        numeric.update_one({"_id": "n2"}, {"$mul": {"n": 0.5}})
    with pytest.raises(DuplicateKeyError):
        numeric.update_one(
            {"_id": "n3"},
            {"$setOnInsert": {"n": Int64(1)}},
            upsert=True,
        )

    indexed = mongo_client["update_operator_index_freshness"].users
    indexed.insert_one({"_id": "u1", "profile": {"city": "Rome"}, "score": 4})
    indexed.create_index([("city", ASCENDING)])
    indexed.create_index([("score", ASCENDING)])
    indexed.create_index([("city", ASCENDING), ("score", ASCENDING)])
    indexed.update_one(
        {"_id": "u1"},
        {"$rename": {"profile.city": "city"}, "$mul": {"score": 3}},
    )
    assert indexed.find_one({"city": "Rome"})["_id"] == "u1"
    assert indexed.find_one({"profile.city": "Rome"}) is None
    assert indexed.find_one({"score": 12})["_id"] == "u1"
    assert indexed.find_one({"score": 4}) is None
    assert indexed.find_one({"city": "Rome", "score": 12})["_id"] == "u1"
    assert indexed.find_one({"city": "Rome", "score": 4}) is None

    multikey = mongo_client["update_operator_multikey_index_freshness"].users
    multikey.insert_many(
        [
            {"_id": "m1", "tags": ["math"]},
            {"_id": "m2", "tags": ["systems"]},
        ]
    )
    multikey.create_index([("tags", ASCENDING)], name="tags_1")
    multikey.create_index([("labels", ASCENDING)], name="labels_1")

    multikey.update_one({"_id": "m1"}, {"$push": {"tags": "logic"}})
    assert [doc["_id"] for doc in multikey.find({"tags": "logic"})] == ["m1"]

    multikey.update_one({"_id": "m1"}, {"$pull": {"tags": "math"}})
    assert list(multikey.find({"tags": "math"})) == []
    assert [doc["_id"] for doc in multikey.find({"tags": "logic"})] == ["m1"]

    multikey.update_one({"_id": "m1"}, {"$rename": {"tags": "labels"}})
    assert list(multikey.find({"tags": "logic"})) == []
    assert [doc["_id"] for doc in multikey.find({"labels": "logic"})] == ["m1"]

    multikey.update_one({"_id": "m1"}, {"$unset": {"labels": ""}})
    assert list(multikey.find({"labels": "logic"})) == []


def test_new_update_modifier_batch_ordering(collection):
    collection.insert_many(
        [
            {"_id": "u1", "name": "Ada", "tags": []},
            {"_id": "u2", "name": "Grace", "tags": []},
        ]
    )

    ordered = collection.database.command(
        {
            "update": collection.name,
            "ordered": True,
            "updates": [
                {"q": {"_id": "u1"}, "u": {"$push": {"name": "bad"}}},
                {"q": {"_id": "u2"}, "u": {"$rename": {"name": "displayName"}}},
            ],
        }
    )
    assert ordered["n"] == 0
    assert ordered["writeErrors"][0]["index"] == 0
    assert collection.find_one({"displayName": "Grace"}) is None

    unordered = collection.database.command(
        {
            "update": collection.name,
            "ordered": False,
            "updates": [
                {"q": {"_id": "u1"}, "u": {"$push": {"name": "bad"}}},
                {"q": {"_id": "u2"}, "u": {"$rename": {"name": "displayName"}}},
            ],
        }
    )
    assert unordered["n"] == 1
    assert unordered["nModified"] == 1
    assert unordered["writeErrors"][0]["index"] == 0
    assert collection.find_one({"displayName": "Grace"})["_id"] == "u2"
