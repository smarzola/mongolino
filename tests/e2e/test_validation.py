import pytest
from pymongo.errors import OperationFailure


pytestmark = pytest.mark.e2e


def user_validator():
    return {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["name"],
            "properties": {
                "name": {"bsonType": "string"},
                "profile": {
                    "bsonType": "object",
                    "required": ["city"],
                    "properties": {"city": {"bsonType": "string"}},
                },
            },
        }
    }


def listed_options(db, name):
    return db.command({"listCollections": 1, "filter": {"name": name}})["cursor"][
        "firstBatch"
    ][0]["options"]


def test_create_collection_with_validator_is_listed(mongo_client):
    db = mongo_client["validation_metadata"]
    collection = db.create_collection(
        "users",
        validator=user_validator(),
        validationLevel="strict",
        validationAction="error",
    )

    options = listed_options(db, collection.name)
    assert options["validator"] == user_validator()
    assert options["validationLevel"] == "strict"
    assert options["validationAction"] == "error"

    name_only = db.command({"listCollections": 1, "nameOnly": True})["cursor"][
        "firstBatch"
    ]
    assert "options" not in next(item for item in name_only if item["name"] == "users")


def test_create_collection_rejects_malformed_validator(mongo_client):
    db = mongo_client["validation_metadata_bad_create"]

    with pytest.raises(OperationFailure) as excinfo:
        db.create_collection(
            "bad",
            validator={
                "$jsonSchema": {
                    "bsonType": "object",
                    "properties": {"profile.city": {"bsonType": "string"}},
                }
            },
        )

    assert excinfo.value.code == 72
    assert "must not contain dots" in str(excinfo.value)


def test_create_collection_rejects_unsupported_validation_options(mongo_client):
    db = mongo_client["validation_metadata_options"]

    with pytest.raises(OperationFailure) as level_error:
        db.create_collection("bad_level", validationLevel="moderate")
    assert level_error.value.code == 72
    assert "validationLevel moderate is not supported" in str(level_error.value)

    with pytest.raises(OperationFailure) as action_error:
        db.create_collection("bad_action", validationAction="warn")
    assert action_error.value.code == 72
    assert "validationAction warn is not supported" in str(action_error.value)

    with pytest.raises(OperationFailure):
        db.create_collection("capped", capped=True)


def test_coll_mod_updates_and_clears_validator(mongo_client):
    db = mongo_client["validation_metadata_collmod"]
    db.create_collection("users")

    response = db.command(
        {
            "collMod": "users",
            "validator": user_validator(),
            "validationLevel": "strict",
            "validationAction": "error",
        }
    )
    assert response["ok"] == 1.0
    assert listed_options(db, "users")["validator"] == user_validator()

    response = db.command({"collMod": "users", "validator": {}})
    assert response["ok"] == 1.0
    assert "validator" not in listed_options(db, "users")


def test_coll_mod_rejects_missing_collection_and_bad_shapes(mongo_client):
    db = mongo_client["validation_metadata_collmod_errors"]
    db.create_collection("users")

    with pytest.raises(OperationFailure) as missing:
        db.command({"collMod": "missing", "validator": {}})
    assert missing.value.code == 26

    with pytest.raises(OperationFailure) as unsupported:
        db.command({"collMod": "users", "expireAfterSeconds": 1})
    assert unsupported.value.code == 72
    assert "expireAfterSeconds" in str(unsupported.value)

    with pytest.raises(OperationFailure) as malformed:
        db.command(
            {
                "collMod": "users",
                "validator": {
                    "$jsonSchema": {
                        "bsonType": "object",
                        "properties": {"age": {"bsonType": "decimal"}},
                    }
                },
            }
        )
    assert malformed.value.code == 72
    assert "decimal is not supported" in str(malformed.value)
