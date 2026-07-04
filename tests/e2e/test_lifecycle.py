import uuid

import pytest
from pymongo.errors import CollectionInvalid, OperationFailure


pytestmark = pytest.mark.e2e


def unique_name(prefix):
    return f"{prefix}_{uuid.uuid4().hex}"


def test_create_collection_and_list_empty_collection(mongo_client):
    db_name = unique_name("life")
    db = mongo_client[db_name]

    created = db.create_collection("empty")

    assert created.name == "empty"
    assert "empty" in db.list_collection_names()
    assert db_name in mongo_client.list_database_names()


def test_create_existing_collection_is_explicit_error(mongo_client):
    db = mongo_client[unique_name("life")]
    db.create_collection("users")

    with pytest.raises(CollectionInvalid):
        db.create_collection("users")


def test_inserted_collection_is_listed_and_drop_removes_catalog_and_documents(collection):
    collection.insert_one({"_id": "u1", "name": "Ada"})

    assert collection.name in collection.database.list_collection_names()
    assert collection.find_one({"_id": "u1"})["name"] == "Ada"

    collection.drop()

    assert collection.name not in collection.database.list_collection_names()
    assert collection.find_one({"_id": "u1"}) is None


def test_database_drop_collection_helper(mongo_client):
    db = mongo_client[unique_name("life")]
    db.create_collection("empty")

    assert "empty" in db.list_collection_names()
    db.drop_collection("empty")
    assert "empty" not in db.list_collection_names()


def test_drop_database_removes_only_target_database(mongo_client):
    target_name = unique_name("life")
    other_name = unique_name("life")
    mongo_client[target_name].users.insert_one({"_id": "u1"})
    mongo_client[other_name].users.insert_one({"_id": "u2"})

    mongo_client.drop_database(target_name)

    assert target_name not in mongo_client.list_database_names()
    assert mongo_client[target_name].users.find_one({"_id": "u1"}) is None
    assert mongo_client[other_name].users.find_one({"_id": "u2"})["_id"] == "u2"


def test_create_collection_rejects_unsupported_options(mongo_client):
    db = mongo_client[unique_name("life")]

    with pytest.raises(OperationFailure) as excinfo:
        db.create_collection("capped", capped=True)

    assert excinfo.value.code == 72
    assert "capped is not supported" in str(excinfo.value)
