import uuid

import pytest
from bson.binary import Binary
from bson.int64 import Int64
from pymongo import MongoClient, ReturnDocument
from pymongo.errors import OperationFailure
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

from conftest import make_client, wait_for_ping


pytestmark = pytest.mark.e2e


def test_client_sessions_and_end_sessions_are_accepted(mongo_client):
    collection = mongo_client.e2e.driver_sessions
    with mongo_client.start_session() as session:
        collection.insert_one({"_id": "s1", "name": "Ada"}, session=session)
        assert collection.find_one({"_id": "s1"}, session=session)["name"] == "Ada"

    ended = mongo_client.admin.command(
        "endSessions",
        [{"id": Binary.from_uuid(uuid.uuid4())}],
    )
    assert ended["ok"] == 1.0

    with pytest.raises(OperationFailure) as excinfo:
        mongo_client.admin.command("endSessions", [{"id": b"short"}])
    assert "lsid.id" in str(excinfo.value)


def test_safe_read_concern_and_journaled_write_concern_are_single_node_local(mongo_client):
    db = mongo_client.get_database(
        "e2e",
        read_concern=ReadConcern("local"),
        write_concern=WriteConcern(w="majority", j=True, wtimeout=0),
    )
    collection = db.driver_concerns

    collection.insert_one({"_id": "c1", "name": "Ada"})
    assert collection.find_one({"_id": "c1"})["name"] == "Ada"

    journaled_collection = mongo_client.get_database(
        "e2e",
        write_concern=WriteConcern(j=True),
    ).driver_concerns
    journaled_collection.update_one({"_id": "c1"}, {"$set": {"journaled": True}})
    assert journaled_collection.find_one({"_id": "c1"})["journaled"] is True

    available = mongo_client.get_database(
        "e2e",
        read_concern=ReadConcern("available"),
    ).driver_concerns
    assert available.find_one({"_id": "c1"})["_id"] == "c1"


def test_unsupported_concerns_are_explicit_and_preserve_data(mongo_client):
    collection = mongo_client.e2e.driver_bad_concerns
    collection.insert_one({"_id": "stable", "name": "Ada"})

    snapshot_collection = mongo_client.get_database(
        "e2e",
        read_concern=ReadConcern("snapshot"),
    ).driver_bad_concerns
    with pytest.raises(OperationFailure) as read_failure:
        list(snapshot_collection.find({}))
    assert "readConcern level snapshot is not supported" in str(read_failure.value)

    with pytest.raises(OperationFailure) as write_failure:
        collection.database.command(
            "insert",
            collection.name,
            documents=[{"_id": "should_not_insert"}],
            writeConcern={"w": 0},
        )
    assert "writeConcern w:0 is not supported" in str(write_failure.value)
    assert collection.find_one({"_id": "should_not_insert"}) is None
    assert collection.find_one({"_id": "stable"})["name"] == "Ada"


def test_transactions_are_rejected_before_mutation(mongo_client):
    collection = mongo_client.e2e.driver_transactions

    with mongo_client.start_session() as session:
        session.start_transaction()
        with pytest.raises(OperationFailure) as excinfo:
            collection.insert_one({"_id": "tx1"}, session=session)
        assert "transactions are not supported" in str(excinfo.value)
        session.abort_transaction()

    assert collection.find_one({"_id": "tx1"}) is None
    with pytest.raises(OperationFailure) as commit_failure:
        mongo_client.admin.command("commitTransaction", 1)
    assert "transactions are not supported" in str(commit_failure.value)


def test_retry_writes_true_client_can_perform_single_writes(mongolino_server):
    uri = f"mongodb://{mongolino_server.addr}/?directConnection=true&retryWrites=true"
    client = make_client(uri)
    wait_for_ping(client, mongolino_server)
    try:
        collection = client.e2e.driver_retry_client
        collection.insert_one({"_id": "r1", "count": 1})
        collection.update_one({"_id": "r1"}, {"$inc": {"count": 1}})
        collection.delete_one({"_id": "missing"})
        doc = collection.find_one_and_update(
            {"_id": "r1"},
            {"$inc": {"count": 1}},
            return_document=ReturnDocument.AFTER,
        )
        assert doc["count"] == 3
    finally:
        client.close()


def test_retryable_write_replay_and_conflict_detection(mongo_client):
    lsid = {"id": Binary.from_uuid(uuid.uuid4())}
    collection = mongo_client.e2e.driver_retry_replay

    command = {
        "insert": collection.name,
        "lsid": lsid,
        "txnNumber": Int64(1),
        "documents": [{"_id": "r1"}],
    }
    first = collection.database.command(command)
    second = collection.database.command(command)
    assert first == second
    assert collection.count_documents({"_id": "r1"}) == 1

    with pytest.raises(OperationFailure) as conflict:
        collection.database.command(
            {
                "insert": collection.name,
                "lsid": lsid,
                "txnNumber": Int64(1),
                "documents": [{"_id": "r2"}],
            }
        )
    assert "already used for a different command" in str(conflict.value)
    assert collection.find_one({"_id": "r2"}) is None
