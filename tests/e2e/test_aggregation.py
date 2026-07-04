import pytest
from pymongo.errors import OperationFailure


pytestmark = pytest.mark.e2e


def seed_scores(collection):
    collection.insert_many(
        [
            {"_id": "s1", "team": "red", "score": 7, "active": True, "meta": {"rank": 2}},
            {"_id": "s2", "team": "blue", "score": 5, "active": False, "meta": {"rank": 3}},
            {"_id": "s3", "team": "red", "score": 11, "active": True, "meta": {"rank": 1}},
        ]
    )


def test_aggregate_match_sort_project(collection):
    seed_scores(collection)

    result = list(
        collection.aggregate(
            [
                {"$match": {"team": "red"}},
                {"$sort": {"score": -1}},
                {"$project": {"_id": 0, "team": 1, "score": 1, "meta.rank": 1}},
            ]
        )
    )

    assert result == [
        {"team": "red", "score": 11, "meta": {"rank": 1}},
        {"team": "red", "score": 7, "meta": {"rank": 2}},
    ]


def test_aggregate_skip_limit_stage_order(collection):
    seed_scores(collection)

    limit_then_skip = list(
        collection.aggregate(
            [
                {"$sort": {"_id": 1}},
                {"$limit": 1},
                {"$skip": 1},
            ]
        )
    )
    skip_then_limit = list(
        collection.aggregate(
            [
                {"$sort": {"_id": 1}},
                {"$skip": 1},
                {"$limit": 1},
                {"$project": {"_id": 1}},
            ]
        )
    )

    assert limit_then_skip == []
    assert skip_then_limit == [{"_id": "s2"}]


def test_aggregate_count(collection):
    seed_scores(collection)

    assert list(collection.aggregate([{"$match": {"active": True}}, {"$count": "total"}])) == [
        {"total": 2}
    ]
    assert list(collection.aggregate([{"$match": {"team": "none"}}, {"$count": "total"}])) == []


def test_aggregate_unsupported_stage_is_explicit_error(collection):
    seed_scores(collection)

    with pytest.raises(OperationFailure) as excinfo:
        list(collection.aggregate([{"$lookup": {"from": "other"}}]))
    assert "$lookup" in str(excinfo.value)

    with pytest.raises(OperationFailure) as excinfo:
        list(collection.aggregate([{"$group": {"_id": "$team", "n": {"$sum": 1}}}]))
    assert "count_documents group shape" in str(excinfo.value)


def test_aggregate_batch_size_iterates_with_get_more(collection):
    seed_scores(collection)

    cursor = collection.aggregate(
        [
            {"$sort": {"_id": 1}},
            {"$project": {"_id": 1}},
        ],
        batchSize=1,
    )

    assert [document["_id"] for document in cursor] == ["s1", "s2", "s3"]


def test_aggregate_cursor_and_option_errors_are_explicit(collection):
    seed_scores(collection)

    for command, contains in [
        (
            {"aggregate": collection.name, "pipeline": [], "cursor": {"batchSize": 0}},
            "batchSize",
        ),
        (
            {"aggregate": collection.name, "pipeline": [], "cursor": {"batchSize": 1001}},
            "batchSize",
        ),
        (
            {"aggregate": collection.name, "pipeline": [], "cursor": {"unknown": 1}},
            "unknown",
        ),
        (
            {
                "aggregate": collection.name,
                "pipeline": [],
                "cursor": {},
                "allowDiskUse": True,
            },
            "allowDiskUse",
        ),
        (
            {"aggregate": collection.name, "pipeline": [], "cursor": {}, "hint": "_id_"},
            "hint",
        ),
        (
            {"aggregate": collection.name, "pipeline": [], "cursor": {}, "let": {}},
            "let",
        ),
    ]:
        with pytest.raises(OperationFailure) as excinfo:
            collection.database.command(command)
        assert contains in str(excinfo.value)
