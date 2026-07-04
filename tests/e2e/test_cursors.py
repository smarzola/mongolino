import pytest
from pymongo.errors import OperationFailure


pytestmark = pytest.mark.e2e


def seed(collection):
    collection.insert_many(
        [
            {"_id": "u1", "name": "Ada"},
            {"_id": "u2", "name": "Grace"},
            {"_id": "u3", "name": "Katherine"},
        ]
    )


def open_cursor(collection, batch_size=1):
    response = collection.database.command(
        {
            "find": collection.name,
            "filter": {},
            "sort": {"_id": 1},
            "batchSize": batch_size,
        }
    )
    cursor = response["cursor"]
    assert cursor["id"] > 0
    return cursor["id"]


def test_kill_cursors_kills_live_cursor_and_repeated_kill_is_not_found(collection):
    seed(collection)
    cursor_id = open_cursor(collection)

    killed = collection.database.command(
        {"killCursors": collection.name, "cursors": [cursor_id]}
    )

    assert killed["cursorsKilled"] == [cursor_id]
    assert killed["cursorsNotFound"] == []

    repeated = collection.database.command(
        {"killCursors": collection.name, "cursors": [cursor_id]}
    )

    assert repeated["cursorsKilled"] == []
    assert repeated["cursorsNotFound"] == [cursor_id]


def test_kill_cursors_namespace_mismatch_does_not_kill_cursor(collection):
    seed(collection)
    cursor_id = open_cursor(collection)

    mismatch = collection.database.command(
        {"killCursors": f"{collection.name}_other", "cursors": [cursor_id]}
    )

    assert mismatch["cursorsKilled"] == []
    assert mismatch["cursorsNotFound"] == [cursor_id]

    next_batch = collection.database.command(
        {"getMore": cursor_id, "collection": collection.name}
    )
    assert [doc["_id"] for doc in next_batch["cursor"]["nextBatch"]]


def test_get_more_after_kill_is_explicit_operation_failure(collection):
    seed(collection)
    cursor_id = open_cursor(collection)
    collection.database.command({"killCursors": collection.name, "cursors": [cursor_id]})

    with pytest.raises(OperationFailure) as excinfo:
        collection.database.command({"getMore": cursor_id, "collection": collection.name})

    assert excinfo.value.code == 43
    assert "cursor not found" in str(excinfo.value)


def test_get_more_zero_batch_size_is_explicit_operation_failure(collection):
    seed(collection)
    cursor_id = open_cursor(collection)

    with pytest.raises(OperationFailure) as excinfo:
        collection.database.command(
            {"getMore": cursor_id, "collection": collection.name, "batchSize": 0}
        )

    assert excinfo.value.code == 9
    assert "batchSize must be positive" in str(excinfo.value)

    next_batch = collection.database.command(
        {"getMore": cursor_id, "collection": collection.name, "batchSize": 10}
    )
    cursor = next_batch["cursor"]
    assert cursor["id"] == 0
    assert [doc["_id"] for doc in cursor["nextBatch"]] == ["u2", "u3"]


def test_pymongo_cursor_close_is_sane(collection):
    seed(collection)
    cursor = collection.find({}).sort("_id", 1).batch_size(1)

    assert next(cursor)["_id"] == "u1"
    cursor.close()
    assert list(cursor) == []
