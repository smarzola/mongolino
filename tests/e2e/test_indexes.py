import pytest
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import OperationFailure


pytestmark = pytest.mark.e2e


def index_names(collection):
    return [index["name"] for index in collection.list_indexes()]


def test_create_list_and_drop_indexes(collection):
    assert index_names(collection) == ["_id_"]

    email = collection.create_index([("email", ASCENDING)], name="email_1", unique=True)
    city = collection.create_index([("profile.city", DESCENDING)])

    assert email == "email_1"
    assert city == "profile.city_-1"
    assert index_names(collection) == ["_id_", "email_1", "profile.city_-1"]
    assert any(index.get("unique") for index in collection.list_indexes() if index["name"] == "email_1")

    collection.drop_index("email_1")

    assert index_names(collection) == ["_id_", "profile.city_-1"]


def test_duplicate_index_create_is_idempotent_and_conflict_errors(collection):
    collection.create_index([("email", ASCENDING)], name="email_1")
    assert collection.create_index([("email", ASCENDING)], name="email_1") == "email_1"

    with pytest.raises(OperationFailure) as excinfo:
        collection.create_index([("email", DESCENDING)], name="email_1")

    assert excinfo.value.code == 85


def test_drop_indexes_all_preserves_id_index(collection):
    collection.create_index([("email", ASCENDING)], name="email_1")
    collection.create_index([("name", ASCENDING)], name="name_1")

    response = collection.database.command(
        {"dropIndexes": collection.name, "index": "*"}
    )

    assert response["ok"] == 1.0
    assert index_names(collection) == ["_id_"]


def test_unsupported_index_options_are_explicit(collection):
    with pytest.raises(OperationFailure) as text_error:
        collection.create_index([("name", "text")], name="name_text")
    assert text_error.value.code == 72
    assert "text indexes are not supported" in str(text_error.value)

    with pytest.raises(OperationFailure) as partial_error:
        collection.create_index(
            [("email", ASCENDING)],
            name="email_partial",
            partialFilterExpression={"active": True},
        )
    assert partial_error.value.code == 72
    assert "partialFilterExpression is not supported" in str(partial_error.value)

    with pytest.raises(OperationFailure) as id_error:
        collection.drop_index("_id_")
    assert id_error.value.code == 67
