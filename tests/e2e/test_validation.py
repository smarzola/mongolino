import pytest
from pymongo import ReturnDocument
from pymongo.errors import BulkWriteError, DuplicateKeyError, OperationFailure, WriteError


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
    options = listed_options(db, "users")
    assert "validator" not in options
    assert "validationLevel" not in options
    assert "validationAction" not in options

    response = db.command(
        {"collMod": "users", "validator": {}, "validationLevel": "strict"}
    )
    assert response["ok"] == 1.0
    options = listed_options(db, "users")
    assert "validator" not in options
    assert options["validationLevel"] == "strict"
    assert "validationAction" not in options


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


def test_insert_enforces_validator_ordered_unordered_and_bypass(mongo_client):
    db = mongo_client["validation_writes_insert"]
    collection = db.create_collection("users", validator=user_validator())

    with pytest.raises(BulkWriteError) as ordered:
        collection.insert_many(
            [
                {"_id": "u1", "name": "Ada"},
                {"_id": "bad", "age": 1},
                {"_id": "u2", "name": "Grace"},
            ]
        )
    assert ordered.value.details["nInserted"] == 1
    assert ordered.value.details["writeErrors"][0]["code"] == 121
    assert collection.find_one({"_id": "u2"}) is None

    with pytest.raises(BulkWriteError) as unordered:
        collection.insert_many(
            [
                {"_id": "bad2", "age": 2},
                {"_id": "u2", "name": "Grace"},
                {"_id": "bad3", "profile": {}},
            ],
            ordered=False,
        )
    assert unordered.value.details["nInserted"] == 1
    assert [err["index"] for err in unordered.value.details["writeErrors"]] == [0, 2]

    collection.insert_one(
        {"_id": "bypassed", "age": "old"}, bypass_document_validation=True
    )
    assert collection.find_one({"_id": "bypassed"})["age"] == "old"

    with pytest.raises(OperationFailure) as malformed:
        db.command(
            {
                "insert": "users",
                "documents": [{"_id": "x"}],
                "bypassDocumentValidation": "yes",
            }
        )
    assert malformed.value.code == 9


def test_update_enforces_validator_for_replacement_modifier_upsert_and_bypass(mongo_client):
    db = mongo_client["validation_writes_update"]
    collection = db.create_collection("users", validator=user_validator())
    collection.insert_one({"_id": "u1", "name": "Ada", "age": 37})

    with pytest.raises(WriteError) as replacement:
        collection.replace_one({"_id": "u1"}, {"_id": "u1", "age": 38})
    assert replacement.value.code == 121

    with pytest.raises(WriteError) as modifier:
        collection.update_one({"_id": "u1"}, {"$set": {"name": 5}})
    assert modifier.value.code == 121

    with pytest.raises(WriteError) as upsert:
        collection.update_one({"_id": "u2"}, {"$set": {"age": 39}}, upsert=True)
    assert upsert.value.code == 121

    collection.update_one(
        {"_id": "u1"},
        {"$set": {"name": 5}},
        bypass_document_validation=True,
    )
    assert collection.find_one({"_id": "u1"})["name"] == 5

    with pytest.raises(OperationFailure) as malformed:
        db.command(
            {
                "update": "users",
                "updates": [{"q": {}, "u": {"$set": {"name": "Ada"}}}],
                "bypassDocumentValidation": "yes",
            }
        )
    assert malformed.value.code == 9


def test_noop_update_of_invalid_existing_document_does_not_revalidate(mongo_client):
    db = mongo_client["validation_writes_noop"]
    collection = db.users
    collection.insert_one({"_id": "legacy", "age": 1})
    db.command({"collMod": "users", "validator": user_validator()})

    result = collection.update_one({"_id": "legacy"}, {"$set": {"age": 1}})
    assert result.modified_count == 0

    with pytest.raises(WriteError) as changed:
        collection.update_one({"_id": "legacy"}, {"$set": {"age": 2}})
    assert changed.value.code == 121


def test_find_and_modify_enforces_validator_and_bypass(mongo_client):
    db = mongo_client["validation_writes_fam"]
    collection = db.create_collection("users", validator=user_validator())
    collection.insert_one({"_id": "u1", "name": "Ada"})

    with pytest.raises(OperationFailure) as update:
        collection.find_one_and_update({"_id": "u1"}, {"$set": {"name": 5}})
    assert update.value.code == 121

    with pytest.raises(OperationFailure) as upsert:
        collection.find_one_and_update(
            {"_id": "u2"},
            {"$set": {"age": 39}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
    assert upsert.value.code == 121

    result = collection.find_one_and_update(
        {"_id": "u1"},
        {"$set": {"name": 5}},
        return_document=ReturnDocument.AFTER,
        bypass_document_validation=True,
    )
    assert result["name"] == 5

    result = db.command(
        {
            "findAndModify": "users",
            "query": {"_id": "u1"},
            "update": {"$set": {"name": 6}},
            "new": True,
            "bypass_document_validation": True,
        }
    )
    assert result["value"]["name"] == 6

    with pytest.raises(OperationFailure) as conflicting:
        db.command(
            {
                "findAndModify": "users",
                "query": {"_id": "u1"},
                "update": {"$set": {"name": "Mutated"}},
                "new": True,
                "bypassDocumentValidation": True,
                "bypass_document_validation": False,
            }
        )
    assert conflicting.value.code == 9
    assert "cannot conflict" in str(conflicting.value)
    assert collection.find_one({"_id": "u1"})["name"] == 6

    with pytest.raises(OperationFailure) as malformed_snake:
        db.command(
            {
                "findAndModify": "users",
                "query": {"_id": "u1"},
                "update": {"$set": {"name": "Ada"}},
                "bypass_document_validation": "yes",
            }
        )
    assert malformed_snake.value.code == 9

    with pytest.raises(OperationFailure) as malformed:
        db.command(
            {
                "findAndModify": "users",
                "query": {"_id": "u1"},
                "update": {"$set": {"name": "Ada"}},
                "bypassDocumentValidation": "yes",
            }
        )
    assert malformed.value.code == 9


def test_bypass_document_validation_does_not_bypass_unique_indexes(mongo_client):
    db = mongo_client["validation_writes_unique"]
    collection = db.create_collection("users", validator=user_validator())
    collection.create_index("email", unique=True)
    collection.insert_one({"_id": "u1", "name": "Ada", "email": "a@example.test"})

    with pytest.raises(DuplicateKeyError):
        collection.insert_one(
            {"_id": "u2", "email": "a@example.test"},
            bypass_document_validation=True,
        )
