import json
from pathlib import Path

import pytest
from pymongo.errors import DuplicateKeyError, OperationFailure, WriteError


pytestmark = pytest.mark.e2e

CORPUS_DIR = Path(__file__).resolve().parents[1] / "spec_corpus"
ERROR_TYPES = {
    "DuplicateKeyError": DuplicateKeyError,
    "OperationFailure": OperationFailure,
    "WriteError": WriteError,
}


def load_local_cases():
    cases = []
    for path in sorted(CORPUS_DIR.glob("*.json")):
        case = json.loads(path.read_text())
        validate_case(case, path.name)
        marks = []
        if case.get("status") == "skip":
            marks.append(pytest.mark.skip(reason=case["skip_reason"]))
        if case.get("status") == "xfail":
            marks.append(pytest.mark.xfail(reason=case["xfail_reason"], strict=True))
        cases.append(pytest.param(case, id=path.stem, marks=marks))
    return cases


def test_runner_rejects_unknown_operation():
    case = {
        "name": "unknown operation",
        "source": "unified-test-format",
        "status": "supported",
        "setup": [],
        "operations": [{"name": "rename_collection"}],
    }

    with pytest.raises(AssertionError, match="unsupported corpus operation"):
        run_case(case, None)


def test_runner_rejects_unsupported_expected_assertion_shape():
    case = {
        "name": "bad assertion",
        "source": "crud",
        "status": "supported",
        "setup": [],
        "operations": [{"name": "ping", "expect_result": ["bad"]}],
    }

    with pytest.raises(AssertionError, match="expect_result must be an object"):
        run_case(case, None)


def test_runner_rejects_malformed_setup_document():
    case = {
        "name": "bad setup",
        "source": "crud",
        "status": "supported",
        "setup": ["not a document"],
        "operations": [{"name": "ping"}],
    }

    with pytest.raises(AssertionError, match="setup entries must be objects"):
        validate_case(case, "generated")


def test_runner_reports_skipped_unsupported_feature():
    case = {
        "name": "skip regex",
        "source": "crud",
        "status": "skip",
        "skip_reason": "regex is unsupported",
        "setup": [],
        "operations": [{"name": "find", "filter": {"name": {"$regex": "A"}}}],
    }

    with pytest.raises(pytest.skip.Exception, match="regex is unsupported"):
        run_case(case, None)


def run_case(case, collection):
    validate_case(case, case["name"])
    if case["status"] == "skip":
        pytest.skip(case["skip_reason"])
    if case["status"] == "xfail":
        pytest.xfail(case["xfail_reason"])

    if case["setup"]:
        collection.insert_many(case["setup"])

    for operation in case["operations"]:
        run_operation(operation, collection)

    if "expect_final_documents" in case:
        actual = list(collection.find({}, projection={"_id": 1, **_final_projection(case)}).sort("_id", 1))
        assert actual == case["expect_final_documents"]


def run_operation(operation, collection):
    if "expect_result" in operation:
        assert isinstance(operation["expect_result"], dict), "expect_result must be an object"

    expected_error = operation.get("expect_error")
    if expected_error:
        error_type = ERROR_TYPES[expected_error["type"]]
        with pytest.raises(error_type) as excinfo:
            execute_operation(operation, collection)
        if "code" in expected_error:
            assert excinfo.value.code == expected_error["code"]
        if "contains" in expected_error:
            assert expected_error["contains"] in str(excinfo.value)
        return

    result = execute_operation(operation, collection)
    if "expect_result" in operation:
        assert_result(result, operation["expect_result"])
    if "expect_documents" in operation:
        assert result == operation["expect_documents"]


def execute_operation(operation, collection):
    name = operation["name"]
    if name == "ping":
        return collection.database.client.admin.command("ping")
    if name == "insert_one":
        return collection.insert_one(operation["document"])
    if name == "find":
        cursor = collection.find(
            operation.get("filter", {}),
            projection=operation.get("projection"),
        )
        if "batch_size" in operation:
            cursor = cursor.batch_size(operation["batch_size"])
        return list(cursor.sort("_id", 1))
    if name == "update_one":
        return collection.update_one(
            operation.get("filter", {}),
            operation["update"],
            upsert=operation.get("upsert", False),
        )
    if name == "update_many":
        return collection.update_many(operation.get("filter", {}), operation["update"])
    if name == "delete_one":
        return collection.delete_one(operation.get("filter", {}))
    if name == "delete_many":
        return collection.delete_many(operation.get("filter", {}))
    if name == "count_documents":
        kwargs = {}
        if "skip" in operation:
            kwargs["skip"] = operation["skip"]
        if "limit" in operation:
            kwargs["limit"] = operation["limit"]
        return collection.count_documents(operation.get("filter", {}), **kwargs)
    if name == "distinct":
        return collection.distinct(operation["key"], operation.get("filter"))
    if name == "create_index":
        return collection.create_index(
            list(operation["keys"].items()),
            name=operation.get("index_name"),
            unique=operation.get("unique", False),
        )
    if name == "list_index_names":
        return [index["name"] for index in collection.list_indexes()]
    if name == "drop_index":
        return collection.drop_index(operation["index_name"])
    if name == "command":
        database = collection.database.client[operation.get("database", collection.database.name)]
        return database.command(operation["command"])
    raise AssertionError(f"unsupported corpus operation: {name}")


def assert_result(result, expected):
    if isinstance(result, dict):
        actual = result
    else:
        actual = {
            key: getattr(result, key)
            for key in (
                "inserted_id",
                "matched_count",
                "modified_count",
            "upserted_id",
            "deleted_count",
        )
        if hasattr(result, key)
    }
    if isinstance(result, int):
        actual = {"count": result}
    if isinstance(result, list):
        actual = {"values": result}
    if isinstance(result, str):
        actual = {"name": result}
    if result is None:
        actual = {"ok": None}
    for key, value in expected.items():
        assert actual[key] == value


def validate_case(case, source):
    assert isinstance(case, dict), f"{source}: corpus case must be an object"
    for key in ("name", "source", "status", "setup", "operations"):
        assert key in case, f"{source}: missing required key {key}"
    assert case["status"] in {"supported", "skip", "xfail"}
    if case["status"] == "skip":
        assert case.get("skip_reason"), f"{source}: skipped cases need skip_reason"
    if case["status"] == "xfail":
        assert case.get("xfail_reason"), f"{source}: xfail cases need xfail_reason"
    assert isinstance(case["setup"], list), f"{source}: setup must be a list"
    assert all(isinstance(doc, dict) for doc in case["setup"]), (
        f"{source}: setup entries must be objects"
    )
    assert isinstance(case["operations"], list) and case["operations"], (
        f"{source}: operations must be a non-empty list"
    )
    for operation in case["operations"]:
        assert isinstance(operation, dict), f"{source}: operations entries must be objects"
        assert "name" in operation, f"{source}: operation missing name"
        if "expect_error" in operation:
            error_type = operation["expect_error"].get("type")
            assert error_type in ERROR_TYPES, f"{source}: unsupported expected error {error_type}"


def _final_projection(case):
    fields = {}
    for document in case["expect_final_documents"]:
        fields.update({key: 1 for key in document if key != "_id"})
    return fields


@pytest.mark.parametrize("case", load_local_cases())
def test_local_spec_corpus(case, collection):
    run_case(case, collection)
