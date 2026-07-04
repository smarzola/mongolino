from pathlib import Path

import pytest
from pymongo.errors import OperationFailure

from conftest import allocate_local_port, make_client, start_mongolino, wait_for_ping


pytestmark = pytest.mark.e2e


def test_ping_succeeds(mongo_client):
    assert mongo_client.admin.command("ping")["ok"] == 1.0


def test_hello_reports_standalone_writable_primary(mongo_client):
    hello = mongo_client.admin.command("hello")

    assert hello["ok"] == 1.0
    assert hello["isWritablePrimary"] is True
    assert hello["helloOk"] is True
    assert hello["maxWireVersion"] >= 17


def test_server_info_uses_build_info(mongo_client):
    info = mongo_client.server_info()

    assert info["ok"] == 1.0
    assert info["version"]
    assert info["allocator"] == "sqlite"


def test_missing_binary_failure_is_clear(tmp_path):
    missing = tmp_path / "missing-mongolino"

    with pytest.raises(AssertionError, match="mongolino binary not found"):
        start_mongolino(missing, tmp_path / "db.sqlite3")


def test_server_exit_during_startup_includes_logs(tmp_path, mongolino_binary):
    db_dir = tmp_path / "db-dir"
    db_dir.mkdir()
    server = start_mongolino(mongolino_binary, db_dir)
    client = make_client(server.uri)

    try:
        with pytest.raises(AssertionError) as excinfo:
            wait_for_ping(client, server, timeout=2.0)
        message = str(excinfo.value)
        assert "mongolino exited before accepting connections" in message
        assert "sqlite" in message.lower()
        assert "stderr:" in message
    finally:
        client.close()
        server.stop()


def test_port_allocation_is_ephemeral():
    ports = {allocate_local_port() for _ in range(3)}

    assert all(port > 0 for port in ports)
    assert ports != {27017}


def test_unsupported_command_returns_operation_failure(mongo_client):
    with pytest.raises(OperationFailure) as excinfo:
        mongo_client.admin.command("createIndexes", "users", indexes=[])

    assert excinfo.value.code == 59
