import pytest
from pymongo.errors import OperationFailure, WriteError


pytestmark = pytest.mark.e2e


def test_unsupported_query_operator_is_explicit_error(collection):
    collection.insert_one({"_id": "u1", "name": "Ada"})

    with pytest.raises(OperationFailure) as excinfo:
        list(collection.find({"name": {"$regex": "A"}}))

    assert excinfo.value.code == 2
    assert "unsupported query operator $regex" in str(excinfo.value)


def test_unsupported_push_option_is_write_error(collection):
    collection.insert_one({"_id": "u1", "tags": []})

    with pytest.raises(WriteError) as excinfo:
        collection.update_one(
            {"_id": "u1"},
            {"$push": {"tags": {"$each": ["new"], "$position": 0}}},
        )

    assert "$push option $position is not supported" in str(excinfo.value)
    assert collection.find_one({"_id": "u1"}) == {"_id": "u1", "tags": []}


def test_malformed_update_is_write_error(collection):
    collection.insert_one({"_id": "u1", "name": "Ada"})

    response = collection.database.command(
        "update",
        collection.name,
        updates=[{"q": {"_id": "u1"}, "u": {"$set": {"name": "Changed"}, "plain": 1}}],
    )

    assert response["ok"] == 1.0
    assert response["writeErrors"][0]["errmsg"] == (
        "update document cannot mix replacement fields and operators"
    )
    assert collection.find_one({"_id": "u1"})["name"] == "Ada"


def test_invalid_delete_limit_is_write_error(collection):
    collection.insert_one({"_id": "u1", "name": "Ada"})

    response = collection.database.command(
        "delete",
        collection.name,
        deletes=[{"q": {"_id": "u1"}, "limit": 2}],
    )

    assert response["ok"] == 1.0
    assert response["writeErrors"][0]["errmsg"] == "delete limit must be 0 or 1"
    assert collection.find_one({"_id": "u1"})["name"] == "Ada"


def test_projection_mode_mix_is_explicit_error(collection):
    collection.insert_one({"_id": "u1", "name": "Ada", "age": 37})

    with pytest.raises(OperationFailure) as excinfo:
        list(collection.find({}, projection={"name": 1, "age": 0}))

    assert excinfo.value.code == 2
    assert "projection cannot mix inclusion and exclusion" in str(excinfo.value)


def test_top_level_where_operator_is_explicit_error(collection):
    collection.insert_one({"_id": "u1", "name": "Ada"})

    with pytest.raises(OperationFailure) as excinfo:
        list(collection.find({"$where": "this.name == 'Ada'"}))

    assert excinfo.value.code == 2
    assert "unsupported top-level query operator $where" in str(excinfo.value)


def test_unsupported_command_remains_explicit(mongo_client):
    with pytest.raises(OperationFailure) as excinfo:
        mongo_client.e2e.command("collStats", "users")

    assert excinfo.value.code == 59
    assert "command 'collStats' is not supported yet" in str(excinfo.value)
