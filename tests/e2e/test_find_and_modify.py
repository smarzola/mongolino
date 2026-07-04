import pytest
from pymongo import ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError, OperationFailure


pytestmark = pytest.mark.e2e


def seed_jobs(collection):
    collection.insert_many(
        [
            {"_id": "j1", "owner": "a", "priority": 1, "state": "queued", "email": "a@example.test"},
            {"_id": "j2", "owner": "b", "priority": 3, "state": "queued", "email": "b@example.test"},
            {"_id": "j3", "owner": "c", "priority": 2, "state": "done", "email": "c@example.test"},
        ]
    )


def test_find_one_and_update_returns_pre_image_by_default(collection):
    seed_jobs(collection)

    result = collection.find_one_and_update(
        {"_id": "j1"},
        {"$inc": {"attempts": 1}, "$set": {"state": "running"}},
        projection={"state": 1, "attempts": 1, "_id": 0},
    )

    assert result == {"state": "queued"}
    assert collection.find_one({"_id": "j1"})["attempts"] == 1
    assert collection.find_one({"_id": "j1"})["state"] == "running"


def test_find_one_and_update_after_and_sorted_selection(collection):
    seed_jobs(collection)

    result = collection.find_one_and_update(
        {"state": "queued"},
        {"$set": {"state": "running"}},
        sort=[("priority", DESCENDING)],
        projection={"_id": 1, "state": 1},
        return_document=ReturnDocument.AFTER,
    )

    assert result == {"_id": "j2", "state": "running"}
    assert collection.find_one({"_id": "j1"})["state"] == "queued"


def test_find_one_and_replace_preserves_id_and_duplicate_unique_conflicts(collection):
    seed_jobs(collection)
    collection.create_index([("email", ASCENDING)], name="email_1", unique=True)

    replaced = collection.find_one_and_replace(
        {"_id": "j1"},
        {"owner": "a2", "priority": 5, "state": "queued", "email": "a2@example.test"},
        return_document=ReturnDocument.AFTER,
    )

    assert replaced["_id"] == "j1"
    assert replaced["owner"] == "a2"

    with pytest.raises(DuplicateKeyError):
        collection.find_one_and_replace(
            {"_id": "j2"},
            {"owner": "dup", "priority": 1, "state": "queued", "email": "a2@example.test"},
        )

    assert collection.find_one({"_id": "j2"})["email"] == "b@example.test"


def test_find_one_and_delete_removes_and_returns_sorted_document(collection):
    seed_jobs(collection)

    removed = collection.find_one_and_delete(
        {"state": "queued"},
        sort=[("priority", ASCENDING)],
        projection={"_id": 1, "priority": 1},
    )

    assert removed == {"_id": "j1", "priority": 1}
    assert collection.find_one({"_id": "j1"}) is None
    assert collection.find_one({"_id": "j2"}) is not None


def test_find_one_and_update_upsert_returns_inserted_document(collection):
    result = collection.find_one_and_update(
        {"_id": "counter", "kind": "local"},
        {"$inc": {"value": 1}},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )

    assert result["_id"] == "counter"
    assert result["kind"] == "local"
    assert result["value"] == 1


def test_find_and_modify_adversarial_errors_are_explicit(collection):
    seed_jobs(collection)

    for command, contains in [
        (
            {
                "findAndModify": collection.name,
                "query": {},
                "remove": True,
                "update": {"$set": {"state": "bad"}},
            },
            "cannot combine remove and update",
        ),
        (
            {
                "findAndModify": collection.name,
                "query": {},
                "update": [{"$set": {"state": "bad"}}],
            },
            "pipeline updates are not supported",
        ),
        (
            {
                "findAndModify": collection.name,
                "query": {},
                "update": {"$set": {"state": "bad"}},
                "arrayFilters": [],
            },
            "arrayFilters",
        ),
        (
            {
                "findAndModify": collection.name,
                "query": {},
                "update": {"$set": {"state": "bad"}},
                "collation": {},
            },
            "collation",
        ),
        (
            {
                "findAndModify": collection.name,
                "query": {},
                "update": {"$set": {"state": "bad"}},
                "hint": "_id_",
            },
            "hint",
        ),
    ]:
        with pytest.raises(OperationFailure) as excinfo:
            collection.database.command(command)
        assert contains in str(excinfo.value)
