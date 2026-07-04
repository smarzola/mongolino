import pytest
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError, DuplicateKeyError, OperationFailure


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


def test_unique_index_creation_rejects_existing_duplicates(collection):
    collection.insert_many(
        [
            {"_id": "u1", "email": "same@example.test"},
            {"_id": "u2", "email": "same@example.test"},
        ]
    )

    with pytest.raises(OperationFailure) as excinfo:
        collection.create_index([("email", ASCENDING)], name="email_1", unique=True)

    assert excinfo.value.code == 11000
    assert index_names(collection) == ["_id_"]


def test_unique_index_enforces_insert_update_and_upsert(collection):
    collection.insert_many(
        [
            {"_id": "u1", "email": "ada@example.test"},
            {"_id": "u2", "email": "grace@example.test"},
        ]
    )
    collection.create_index([("email", ASCENDING)], name="email_1", unique=True)

    with pytest.raises(DuplicateKeyError):
        collection.insert_one({"_id": "u3", "email": "ada@example.test"})

    with pytest.raises(DuplicateKeyError):
        collection.update_one({"_id": "u2"}, {"$set": {"email": "ada@example.test"}})
    assert collection.find_one({"_id": "u2"})["email"] == "grace@example.test"

    with pytest.raises(DuplicateKeyError):
        collection.update_one(
            {"_id": "u4"},
            {"$set": {"email": "ada@example.test"}},
            upsert=True,
        )
    assert collection.find_one({"_id": "u4"}) is None


def test_unique_unordered_bulk_partial_success_and_drop_index(collection):
    collection.create_index([("email", ASCENDING)], name="email_1", unique=True)

    with pytest.raises(BulkWriteError) as excinfo:
        collection.insert_many(
            [
                {"_id": "u1", "email": "same@example.test"},
                {"_id": "u2", "email": "same@example.test"},
                {"_id": "u3", "email": "other@example.test"},
            ],
            ordered=False,
        )

    assert excinfo.value.details["nInserted"] == 2
    assert excinfo.value.details["writeErrors"][0]["index"] == 1

    collection.drop_index("email_1")
    collection.insert_one({"_id": "u4", "email": "same@example.test"})
    assert collection.count_documents({"email": "same@example.test"}) == 2


def test_unique_index_rejects_array_values(collection):
    collection.insert_one({"_id": "u1", "emails": ["a@example.test"]})

    with pytest.raises(OperationFailure) as excinfo:
        collection.create_index([("emails", ASCENDING)], name="emails_1", unique=True)

    assert excinfo.value.code == 72
    assert "does not support array value" in str(excinfo.value)


def test_indexed_query_results_stay_correct_after_mutations(collection):
    collection.insert_many(
        [
            {"_id": "u1", "name": "Ada", "profile": {"city": "Rome"}},
            {"_id": "u2", "name": "Grace", "profile": {"city": "London"}},
            {"_id": "u3", "name": "Katherine", "profile": {"city": "Rome"}},
        ]
    )
    collection.create_index([("profile.city", ASCENDING)], name="city_1")

    assert [doc["_id"] for doc in collection.find({"profile.city": "Rome"}).sort("_id", 1)] == [
        "u1",
        "u3",
    ]

    collection.update_one({"_id": "u1"}, {"$set": {"profile.city": "Milan"}})
    assert [doc["_id"] for doc in collection.find({"profile.city": "Rome"}).sort("_id", 1)] == [
        "u3"
    ]
    assert [doc["_id"] for doc in collection.find({"profile.city": "Milan"}).sort("_id", 1)] == [
        "u1"
    ]

    collection.delete_one({"_id": "u3"})
    assert list(collection.find({"profile.city": "Rome"})) == []

    collection.drop_index("city_1")
    assert [doc["_id"] for doc in collection.find({"profile.city": "Milan"}).sort("_id", 1)] == [
        "u1"
    ]
