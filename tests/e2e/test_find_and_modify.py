import pytest
from bson.int64 import Int64
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


def test_find_one_and_update_rejects_numeric_unique_conflict(collection):
    collection.insert_many([{"_id": "n1", "n": 1}, {"_id": "n2", "n": 2}])
    collection.create_index([("n", ASCENDING)], name="n_1", unique=True)

    with pytest.raises(DuplicateKeyError):
        collection.find_one_and_update(
            {"_id": "n2"},
            {"$set": {"n": Int64(1)}},
            return_document=ReturnDocument.AFTER,
        )

    assert collection.find_one({"_id": "n2"})["n"] == 2


def test_find_and_modify_refreshes_sparse_unique_membership(collection):
    collection.insert_many(
        [
            {"_id": "j1", "state": "queued"},
            {"_id": "j2", "state": "queued", "email": "taken@example.test"},
        ]
    )
    collection.create_index([("email", ASCENDING)], name="email_sparse", unique=True, sparse=True)

    updated = collection.find_one_and_update(
        {"_id": "j1"},
        {"$set": {"email": "new@example.test"}},
        return_document=ReturnDocument.AFTER,
    )
    assert updated["email"] == "new@example.test"

    with pytest.raises(DuplicateKeyError):
        collection.find_one_and_update(
            {"_id": "j1"},
            {"$set": {"email": "taken@example.test"}},
            return_document=ReturnDocument.AFTER,
        )

    removed = collection.find_one_and_update(
        {"_id": "j1"},
        {"$unset": {"email": ""}},
        return_document=ReturnDocument.AFTER,
    )
    assert "email" not in removed


def test_find_and_modify_preserves_partial_unique_membership(collection):
    collection.insert_many(
        [
            {"_id": "j1", "email": "same@example.test", "active": True},
            {"_id": "j2", "email": "same@example.test", "active": False},
        ]
    )
    collection.create_index(
        [("email", ASCENDING)],
        name="email_active_partial",
        unique=True,
        partialFilterExpression={"active": True},
    )

    with pytest.raises(DuplicateKeyError):
        collection.find_one_and_update(
            {"_id": "j2"},
            {"$set": {"active": True}},
            return_document=ReturnDocument.AFTER,
        )
    assert collection.find_one({"_id": "j2"})["active"] is False

    updated = collection.find_one_and_update(
        {"_id": "j2"},
        {"$set": {"email": "other@example.test", "active": True}},
        return_document=ReturnDocument.AFTER,
    )
    assert updated["active"] is True

    removed = collection.find_one_and_update(
        {"_id": "j2"},
        {"$set": {"active": False}},
        return_document=ReturnDocument.AFTER,
    )
    assert removed["active"] is False


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


def test_find_and_modify_targets_id_indexed_scalar_and_fallback_filters(collection):
    seed_jobs(collection)
    collection.create_index([("email", ASCENDING)], name="email_1")
    collection.create_index([("state", ASCENDING)], name="state_1")

    by_id = collection.find_one_and_update(
        {"_id": {"$eq": "j1"}},
        {"$set": {"state": "running"}},
        return_document=ReturnDocument.AFTER,
    )
    assert by_id["_id"] == "j1"
    assert by_id["state"] == "running"

    by_index = collection.find_one_and_update(
        {"email": "b@example.test"},
        {"$set": {"email": "b2@example.test"}},
        return_document=ReturnDocument.AFTER,
    )
    assert by_index["_id"] == "j2"
    assert by_index["email"] == "b2@example.test"
    assert collection.find_one({"email": "b@example.test"}) is None

    replaced = collection.find_one_and_replace(
        {"email": "b2@example.test"},
        {"owner": "b2", "priority": 7, "state": "queued", "email": "b3@example.test"},
        return_document=ReturnDocument.AFTER,
    )
    assert replaced["_id"] == "j2"
    assert replaced["email"] == "b3@example.test"

    removed = collection.find_one_and_delete({"state": "done"})
    assert removed["_id"] == "j3"
    assert collection.find_one({"_id": "j3"}) is None

    fallback = collection.find_one_and_update(
        {"$or": [{"owner": "missing"}, {"owner": "b2"}]},
        {"$set": {"state": "fallback"}},
        return_document=ReturnDocument.AFTER,
    )
    assert fallback["_id"] == "j2"
    assert fallback["state"] == "fallback"


def test_find_and_modify_refreshes_compound_index_entries(collection):
    seed_jobs(collection)
    collection.create_index([("state", ASCENDING), ("email", ASCENDING)], name="state_email_1")

    updated = collection.find_one_and_update(
        {"state": "queued", "email": "b@example.test"},
        {"$set": {"state": "running"}},
        return_document=ReturnDocument.AFTER,
    )
    assert updated["_id"] == "j2"
    assert collection.find_one({"state": "queued", "email": "b@example.test"}) is None
    assert collection.find_one({"state": "running", "email": "b@example.test"})["_id"] == "j2"

    replaced = collection.find_one_and_replace(
        {"state": "running", "email": "b@example.test"},
        {"owner": "b", "priority": 4, "state": "queued", "email": "b@example.test"},
        return_document=ReturnDocument.AFTER,
    )
    assert replaced["_id"] == "j2"
    assert collection.find_one({"state": "running", "email": "b@example.test"}) is None
    assert collection.find_one({"state": "queued", "email": "b@example.test"})["_id"] == "j2"

    removed = collection.find_one_and_delete({"state": "queued", "email": "b@example.test"})
    assert removed["_id"] == "j2"
    assert collection.find_one({"state": "queued", "email": "b@example.test"}) is None


def test_find_and_modify_targets_compound_prefix_filter(collection):
    seed_jobs(collection)
    collection.create_index([("state", ASCENDING), ("email", ASCENDING)], name="state_email_1")

    updated = collection.find_one_and_update(
        {"state": "queued"},
        {"$set": {"state": "running"}},
        sort=[("priority", DESCENDING)],
        return_document=ReturnDocument.AFTER,
    )
    assert updated["_id"] == "j2"
    assert collection.find_one({"state": "queued", "email": "b@example.test"}) is None
    assert collection.find_one({"state": "running"})["_id"] == "j2"


def test_find_and_modify_targets_array_backed_matches_when_index_entries_are_incomplete(collection):
    collection.insert_many(
        [
            {"_id": "j1", "tags": ["math"], "state": "queued"},
            {"_id": "j2", "tags": "math", "state": "queued"},
            {"_id": "j3", "tags": "math", "state": "done"},
        ]
    )
    collection.create_index([("tags", ASCENDING)], name="tags_1")
    collection.create_index([("tags", ASCENDING), ("state", ASCENDING)], name="tags_state_1")

    updated = collection.find_one_and_update(
        {"tags": "math", "state": "queued"},
        {"$set": {"state": "running"}},
        return_document=ReturnDocument.AFTER,
    )
    assert updated["_id"] == "j1"
    assert updated["state"] == "running"

    removed = collection.find_one_and_delete({"tags": "math", "state": "queued"})
    assert removed["_id"] == "j2"
    assert collection.find_one({"_id": "j2"}) is None


def test_find_and_modify_targets_elem_match_filters(collection):
    collection.insert_many(
        [
            {
                "_id": "j1",
                "items": [
                    {"kind": "a", "score": 1},
                    {"kind": "b", "score": 5},
                ],
                "scores": [1, 5, 8],
            },
            {
                "_id": "j2",
                "items": [
                    {"kind": "a", "score": 6},
                    {"kind": "b", "score": 2},
                ],
                "scores": [2, 9],
            },
        ]
    )

    updated = collection.find_one_and_update(
        {"items": {"$elemMatch": {"kind": "a", "score": {"$gte": 5}}}},
        {"$set": {"state": "matched"}},
        return_document=ReturnDocument.AFTER,
    )
    assert updated["_id"] == "j2"
    assert updated["state"] == "matched"

    removed = collection.find_one_and_delete({"scores": {"$elemMatch": {"$gt": 4, "$lt": 7}}})
    assert removed["_id"] == "j1"
    assert collection.find_one({"_id": "j1"}) is None


def test_find_and_modify_keeps_scalar_multikey_entries_fresh(collection):
    collection.insert_many(
        [
            {"_id": "j1", "tags": ["queued"], "state": "queued"},
            {"_id": "j2", "tags": ["done"], "state": "done"},
        ]
    )
    collection.create_index([("tags", ASCENDING)], name="tags_1")

    updated = collection.find_one_and_update(
        {"tags": "queued"},
        {"$push": {"tags": "running"}, "$set": {"state": "running"}},
        return_document=ReturnDocument.AFTER,
    )
    assert updated["_id"] == "j1"
    assert [doc["_id"] for doc in collection.find({"tags": "running"})] == ["j1"]

    replaced = collection.find_one_and_replace(
        {"tags": "running"},
        {"_id": "j1", "tags": ["archived"], "state": "archived"},
        return_document=ReturnDocument.AFTER,
    )
    assert replaced["_id"] == "j1"
    assert list(collection.find({"tags": "running"})) == []
    assert [doc["_id"] for doc in collection.find({"tags": "archived"})] == ["j1"]

    removed = collection.find_one_and_delete({"tags": "archived"})
    assert removed["_id"] == "j1"
    assert list(collection.find({"tags": "archived"})) == []


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


def test_find_and_modify_rejects_ambiguous_command_aliases_before_mutation(collection):
    seed_jobs(collection)

    with pytest.raises(OperationFailure) as excinfo:
        collection.database.command(
            {
                "findAndModify": collection.name,
                "findandmodify": collection.name,
                "query": {"_id": "j1"},
                "update": {"$set": {"state": "mutated"}},
                "new": True,
            }
        )

    assert "both command aliases" in str(excinfo.value)
    assert collection.find_one({"_id": "j1"})["state"] == "queued"


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
