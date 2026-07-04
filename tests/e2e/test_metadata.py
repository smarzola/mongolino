import pytest
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

    assert collection.count_documents({"active": True}) == 2
    assert collection.count_documents({"active": True}, skip=1, limit=10) == 1
    assert collection.estimated_document_count() == 3


def test_count_command_with_filter(collection):
    seed(collection)

    response = collection.database.command(
        {"count": collection.name, "query": {"profile.city": "Rome"}}
    )

    assert response["n"] == 2


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
    assert "collation is not supported" in str(distinct_error.value)
