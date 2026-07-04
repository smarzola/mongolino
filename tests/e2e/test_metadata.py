import pytest
from bson.int64 import Int64
from pymongo.errors import OperationFailure


pytestmark = pytest.mark.e2e


def seed(collection):
    collection.insert_many(
        [
            {
                "_id": "u1",
                "name": "Ada",
                "active": True,
                "profile": {"city": "Rome"},
                "tags": ["math", "logic"],
            },
            {
                "_id": "u2",
                "name": "Grace",
                "active": False,
                "profile": {"city": "London"},
                "tags": ["compiler"],
            },
            {
                "_id": "u3",
                "name": "Katherine",
                "active": True,
                "profile": {"city": "Rome"},
                "tags": ["math"],
            },
        ]
    )


def test_count_documents_and_estimated_count(collection):
    seed(collection)

    assert collection.count_documents({}) == 3
    assert collection.count_documents({}, skip=1, limit=1) == 1
    assert collection.count_documents({"_id": "u1"}) == 1
    assert collection.count_documents({"_id": {"$eq": "u1"}}, skip=1) == 0
    assert collection.count_documents({"_id": "missing"}) == 0
    assert collection.count_documents({"active": True}) == 2
    assert collection.count_documents({"active": True}, skip=1, limit=10) == 1
    assert collection.estimated_document_count() == 3


def test_count_command_with_filter(collection):
    seed(collection)

    response = collection.database.command(
        {"count": collection.name, "query": {"profile.city": "Rome"}}
    )

    assert response["n"] == 2


def test_indexed_count_mixed_numeric_values_match_cross_type(collection):
    collection.insert_many(
        [
            {"_id": "i32", "n": 1},
            {"_id": "i64", "n": Int64(1)},
            {"_id": "double", "n": 1.0},
            {"_id": "other", "n": 2},
        ]
    )
    collection.create_index("n", name="n_1")

    assert collection.count_documents({"n": 1}) == 3
    assert collection.count_documents({"n": {"$eq": Int64(1)}}) == 3
    assert collection.count_documents({"n": 1.0}) == 3


def test_compound_indexed_count_uses_safe_full_key_and_falls_back(collection):
    collection.insert_many(
        [
            {"_id": "u1", "profile": {"city": "Rome"}, "active": True},
            {"_id": "u2", "profile": {"city": "Rome"}, "active": False},
            {"_id": "u3", "profile": {"city": "London"}, "active": True},
            {"_id": "u4", "profile": {"city": "Rome"}, "active": 1},
        ]
    )
    collection.create_index([("profile.city", 1), ("active", 1)], name="city_active_1")

    assert collection.count_documents({"profile.city": "Rome", "active": True}) == 1
    assert collection.count_documents({"active": True, "profile.city": {"$eq": "Rome"}}, skip=1) == 0
    assert collection.count_documents({"profile.city": "Rome"}) == 3
    assert collection.count_documents({"profile.city": "Rome", "active": 1}) == 1


def test_sparse_and_partial_indexed_count_uses_safe_membership_filters(collection):
    collection.insert_many(
        [
            {"_id": "u1", "email": "same@example.test", "active": True, "handle": "ada"},
            {"_id": "u2", "email": "same@example.test", "active": False},
            {"_id": "u3", "name": "missing"},
            {"_id": "u4", "email": "other@example.test", "active": True, "handle": "grace"},
        ]
    )
    collection.create_index("email", name="email_sparse", sparse=True)
    collection.create_index(
        "email",
        name="email_active_partial",
        partialFilterExpression={"active": True},
    )
    collection.create_index(
        "email",
        name="email_active_handle_partial",
        partialFilterExpression={
            "$and": [{"active": {"$eq": True}}, {"handle": {"$exists": True}}]
        },
    )

    assert collection.count_documents({"email": "same@example.test"}) == 2
    assert collection.count_documents({"email": "same@example.test", "active": True}) == 1
    assert (
        collection.count_documents(
            {"email": "same@example.test", "active": True, "handle": "ada"}
        )
        == 1
    )
    assert collection.count_documents({"email": "same@example.test", "active": False}) == 1
    assert collection.count_documents({"active": True}) == 2


def test_scalar_multikey_indexed_count_uses_entries_and_validates_matches(collection):
    collection.insert_many(
        [
            {"_id": "u1", "tags": ["math", "math"], "active": True},
            {"_id": "u2", "tags": "math", "active": True},
            {"_id": "u3", "tags": "math", "active": False},
            {"_id": "u4", "tags": [{"name": "math"}], "active": True},
            {"_id": "u5", "scores": [1, 2]},
        ]
    )
    collection.create_index("tags", name="tags_1")
    collection.create_index([("tags", 1), ("active", 1)], name="tags_active_1")
    collection.create_index("scores", name="scores_1")

    assert collection.count_documents({"tags": "math"}) == 3
    assert collection.count_documents({"tags": "math", "active": True}) == 2
    assert collection.count_documents({"tags": {"$eq": "math"}}, skip=1, limit=1) == 1
    assert collection.count_documents({"tags": [{"name": "math"}]}) == 1
    assert collection.count_documents({"scores": 1}) == 1


def test_distinct_scalar_dotted_and_array_values(collection):
    seed(collection)

    assert collection.distinct("profile.city") == ["London", "Rome"]
    assert collection.distinct("tags", {"active": True}) == ["logic", "math"]


def test_count_and_distinct_unsupported_options_are_explicit(collection):
    seed(collection)

    with pytest.raises(OperationFailure) as count_error:
        collection.count_documents({}, hint="_id_")
    assert count_error.value.code == 72
    assert "hint is not supported" in str(count_error.value)

    with pytest.raises(OperationFailure) as distinct_error:
        collection.distinct("name", collation={"locale": "en"})
    assert distinct_error.value.code == 72
    assert "requires strength 2" in str(distinct_error.value)
