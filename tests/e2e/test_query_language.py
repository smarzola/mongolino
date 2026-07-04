import pytest
from bson import ObjectId
from bson.int64 import Int64
from pymongo import ASCENDING
from pymongo.errors import OperationFailure


pytestmark = pytest.mark.e2e


def ids(cursor):
    return [doc["_id"] for doc in cursor]


def test_type_size_and_all_predicates_find_and_count(collection):
    oid = ObjectId()
    collection.insert_many(
        [
            {
                "_id": "u1",
                "name": "Ada",
                "profile": {"city": "Rome"},
                "tags": ["math", "logic"],
                "scores": [1, 2, Int64(2)],
                "nothing": None,
                "active": True,
                "oid": oid,
                "age": 37,
                "long": Int64(37),
                "ratio": 1.5,
            },
            {"_id": "u2", "name": "Grace", "tags": ["navy"], "scores": [3], "age": Int64(39)},
            {"_id": "u3", "name": "Katherine", "tags": [], "scores": "none", "age": 41.0},
        ]
    )

    assert ids(collection.find({"name": {"$type": "string"}}).sort("_id", ASCENDING)) == [
        "u1",
        "u2",
        "u3",
    ]
    assert ids(collection.find({"profile": {"$type": "object"}})) == ["u1"]
    assert ids(collection.find({"tags": {"$type": "array"}}).sort("_id", ASCENDING)) == [
        "u1",
        "u2",
        "u3",
    ]
    assert ids(collection.find({"tags": {"$type": "string"}}).sort("_id", ASCENDING)) == [
        "u1",
        "u2",
    ]
    assert ids(collection.find({"active": {"$type": "bool"}})) == ["u1"]
    assert ids(collection.find({"oid": {"$type": "objectId"}})) == ["u1"]
    assert ids(collection.find({"nothing": {"$type": 10}})) == ["u1"]
    assert ids(collection.find({"age": {"$type": ["int", "long"]}}).sort("_id", ASCENDING)) == [
        "u1",
        "u2",
    ]
    assert collection.count_documents({"age": {"$type": "number"}}) == 3
    assert collection.count_documents({"ratio": {"$type": 1}}) == 1

    assert ids(collection.find({"tags": {"$size": 2}})) == ["u1"]
    assert ids(collection.find({"tags": {"$size": Int64(0)}})) == ["u3"]
    assert collection.count_documents({"scores": {"$size": 1}}) == 1

    assert ids(collection.find({"tags": {"$all": ["logic", "math"]}})) == ["u1"]
    assert ids(collection.find({"scores": {"$all": [2, 2]}})) == ["u1"]
    assert collection.count_documents({"tags": {"$all": ["math", "missing"]}}) == 0


def test_type_size_and_all_malformed_predicates_are_errors(collection):
    collection.insert_one({"_id": "u1", "tags": ["math"], "name": "Ada"})

    bad_filters = [
        ({"name": {"$type": "decimal"}}, "$type alias decimal is not supported"),
        ({"name": {"$type": 99}}, "$type code 99 is not supported"),
        ({"name": {"$type": []}}, "$type array must not be empty"),
        ({"tags": {"$size": -1}}, "$size requires a non-negative integer"),
        ({"tags": {"$size": 1.5}}, "$size requires a non-negative integer"),
        ({"tags": {"$all": "math"}}, "$all requires an array"),
        (
            {"tags": {"$all": [{"$elemMatch": {"$eq": "math"}}]}},
            "$all $elemMatch clauses are not supported yet",
        ),
    ]

    for filter_doc, message in bad_filters:
        with pytest.raises(OperationFailure) as excinfo:
            list(collection.find(filter_doc))
        assert excinfo.value.code == 2
        assert message in str(excinfo.value)
