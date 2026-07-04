import json
from pathlib import Path

import pytest
from pymongo import ReturnDocument
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
    if "expect_path_values" in operation:
        assert_path_values(result, operation["expect_path_values"])
    if "expect_documents" in operation:
        assert result == operation["expect_documents"]


def execute_operation(operation, collection):
    name = operation["name"]
    if name == "ping":
        return collection.database.client.admin.command("ping")
    if name == "insert_one":
        return collection.insert_one(operation["document"])
    if name == "find":
        kwargs = {"projection": operation.get("projection")}
        if "collation" in operation:
            kwargs["collation"] = operation["collation"]
        cursor = collection.find(
            operation.get("filter", {}),
            **kwargs,
        )
        if "batch_size" in operation:
            cursor = cursor.batch_size(operation["batch_size"])
        return list(cursor.sort(_sort(operation) or [("_id", 1)]))
    if name == "update_one":
        kwargs = {"upsert": operation.get("upsert", False)}
        if "collation" in operation:
            kwargs["collation"] = operation["collation"]
        return collection.update_one(
            operation.get("filter", {}),
            operation["update"],
            **kwargs,
        )
    if name == "update_many":
        kwargs = {}
        if "collation" in operation:
            kwargs["collation"] = operation["collation"]
        return collection.update_many(operation.get("filter", {}), operation["update"], **kwargs)
    if name == "find_one_and_update":
        kwargs = {
            "projection": operation.get("projection"),
            "sort": _sort(operation),
            "upsert": operation.get("upsert", False),
            "return_document": _return_document(operation),
        }
        if "collation" in operation:
            kwargs["collation"] = operation["collation"]
        return collection.find_one_and_update(
            operation.get("filter", {}),
            operation["update"],
            **kwargs,
        )
    if name == "find_one_and_replace":
        kwargs = {
            "projection": operation.get("projection"),
            "sort": _sort(operation),
            "upsert": operation.get("upsert", False),
            "return_document": _return_document(operation),
        }
        if "collation" in operation:
            kwargs["collation"] = operation["collation"]
        return collection.find_one_and_replace(
            operation.get("filter", {}),
            operation["replacement"],
            **kwargs,
        )
    if name == "find_one_and_delete":
        kwargs = {
            "projection": operation.get("projection"),
            "sort": _sort(operation),
        }
        if "collation" in operation:
            kwargs["collation"] = operation["collation"]
        return collection.find_one_and_delete(
            operation.get("filter", {}),
            **kwargs,
        )
    if name == "delete_one":
        kwargs = {}
        if "collation" in operation:
            kwargs["collation"] = operation["collation"]
        return collection.delete_one(operation.get("filter", {}), **kwargs)
    if name == "delete_many":
        kwargs = {}
        if "collation" in operation:
            kwargs["collation"] = operation["collation"]
        return collection.delete_many(operation.get("filter", {}), **kwargs)
    if name == "aggregate":
        kwargs = {}
        if "batch_size" in operation:
            kwargs["batchSize"] = operation["batch_size"]
        if "collation" in operation:
            kwargs["collation"] = operation["collation"]
        return list(collection.aggregate(_expand_collection_placeholder(operation["pipeline"], collection.name), **kwargs))
    if name == "count_documents":
        kwargs = {}
        if "skip" in operation:
            kwargs["skip"] = operation["skip"]
        if "limit" in operation:
            kwargs["limit"] = operation["limit"]
        if "collation" in operation:
            kwargs["collation"] = operation["collation"]
        return collection.count_documents(operation.get("filter", {}), **kwargs)
    if name == "distinct":
        kwargs = {}
        if "collation" in operation:
            kwargs["collation"] = operation["collation"]
        return collection.distinct(operation["key"], operation.get("filter"), **kwargs)
    if name == "create_index":
        kwargs = {
            "name": operation.get("index_name"),
            "unique": operation.get("unique", False),
        }
        if "expireAfterSeconds" in operation:
            kwargs["expireAfterSeconds"] = operation["expireAfterSeconds"]
        if "collation" in operation:
            kwargs["collation"] = operation["collation"]
        return collection.create_index(
            list(operation["keys"].items()),
            **kwargs,
        )
    if name == "list_index_names":
        return [index["name"] for index in collection.list_indexes()]
    if name == "drop_index":
        return collection.drop_index(operation["index_name"])
    if name == "command":
        database = collection.database.client[operation.get("database", collection.database.name)]
        return database.command(_expand_collection_placeholder(operation["command"], collection.name))
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


def assert_path_values(result, expected):
    assert isinstance(result, dict), "path assertions require a document result"
    for assertion in expected:
        current = result
        for part in assertion["path"]:
            current = current[part]
        assert current == assertion["value"]


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


def _sort(operation):
    sort = operation.get("sort")
    if sort is None:
        return None
    return [tuple(item) for item in sort]


def _return_document(operation):
    value = operation.get("return_document", "before")
    if value == "after":
        return ReturnDocument.AFTER
    assert value == "before", "return_document must be before or after"
    return ReturnDocument.BEFORE


def _expand_collection_placeholder(value, collection_name):
    if value == "$$collection":
        return collection_name
    if isinstance(value, dict):
        return {
            key: _expand_collection_placeholder(item, collection_name)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_expand_collection_placeholder(item, collection_name) for item in value]
    return value


@pytest.mark.parametrize("case", load_local_cases())
def test_local_spec_corpus(case, collection):
    run_case(case, collection)
