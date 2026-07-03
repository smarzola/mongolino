import os
import socket
import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest
from pymongo import MongoClient
from pymongo.errors import PyMongoError


ROOT = Path(__file__).resolve().parents[2]


def pytest_runtest_makereport(item, call):
    if call.when == "call":
        setattr(item, "_mongolino_call_failed", call.excinfo is not None)


@dataclass
class MongolinoServer:
    process: subprocess.Popen
    addr: str
    db_path: Path
    stdout_path: Path
    stderr_path: Path

    @property
    def uri(self) -> str:
        return f"mongodb://{self.addr}/?directConnection=true&retryWrites=false"

    def logs(self) -> str:
        stdout = _read_file(self.stdout_path)
        stderr = _read_file(self.stderr_path)
        return f"stdout:\n{stdout or '<empty>'}\nstderr:\n{stderr or '<empty>'}"

    def stop(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)


@pytest.fixture(scope="session")
def mongolino_binary() -> Path:
    return locate_or_build_mongolino()


@pytest.fixture
def mongolino_server(mongolino_binary, tmp_path, request):
    server = start_mongolino(mongolino_binary, tmp_path / "mongolino.sqlite3")
    try:
        yield server
    finally:
        failed = getattr(request.node, "_mongolino_call_failed", False)
        server.stop()
        logs = server.logs() if failed else None
        if logs:
            print("\n--- mongolino server logs ---")
            print(logs)


@pytest.fixture
def mongo_client(mongolino_server):
    client = make_client(mongolino_server.uri)
    wait_for_ping(client, mongolino_server)
    try:
        yield client
    finally:
        client.close()


@pytest.fixture
def collection(mongo_client, request):
    suffix = uuid.uuid4().hex
    name = f"{request.node.name}_{suffix}".replace("[", "_").replace("]", "_")
    return mongo_client["e2e"][name]


def locate_or_build_mongolino() -> Path:
    env_path = os.environ.get("MONGOLINO_BIN")
    if env_path:
        binary = Path(env_path)
        if not binary.is_file():
            raise AssertionError(f"MONGOLINO_BIN does not point to a file: {binary}")
        return binary

    binary = ROOT / "target" / "debug" / "mongolino"
    if not binary.is_file():
        subprocess.run(["cargo", "build"], cwd=ROOT, check=True)
    if not binary.is_file():
        raise AssertionError(f"mongolino binary was not built at {binary}")
    return binary


def allocate_local_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def make_client(uri: str) -> MongoClient:
    return MongoClient(
        uri,
        serverSelectionTimeoutMS=1500,
        connectTimeoutMS=1500,
        socketTimeoutMS=1500,
        appname="mongolino-e2e",
    )


def start_mongolino(binary: Path, db_path: Path) -> MongolinoServer:
    if not binary.is_file():
        raise AssertionError(f"mongolino binary not found: {binary}")

    addr = f"127.0.0.1:{allocate_local_port()}"
    stdout_path = db_path.with_suffix(".stdout.log")
    stderr_path = db_path.with_suffix(".stderr.log")
    stdout = stdout_path.open("w")
    stderr = stderr_path.open("w")
    process = subprocess.Popen(
        [str(binary), "--addr", addr, "--db", str(db_path)],
        cwd=ROOT,
        stdout=stdout,
        stderr=stderr,
        text=True,
    )
    stdout.close()
    stderr.close()
    return MongolinoServer(
        process=process,
        addr=addr,
        db_path=db_path,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
    )


def wait_for_ping(client: MongoClient, server: MongolinoServer, timeout: float = 5.0):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        if server.process.poll() is not None:
            raise AssertionError(
                f"mongolino exited before accepting connections with code "
                f"{server.process.returncode}\n{server.logs()}"
            )
        try:
            client.admin.command("ping")
            return
        except PyMongoError as err:
            last_error = err
            time.sleep(0.05)

    raise AssertionError(f"timed out waiting for mongolino ping: {last_error}\n{server.logs()}")


def _read_file(path: Path) -> str:
    try:
        return path.read_text()
    except FileNotFoundError:
        return ""
