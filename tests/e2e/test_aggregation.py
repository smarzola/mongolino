import pytest
from bson.int64 import Int64
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
    collection.create_index("active", name="active_1")

    assert list(collection.aggregate([{"$match": {"active": True}}, {"$count": "total"}])) == [
        {"total": 2}
    ]
    assert list(collection.aggregate([{"$match": {"_id": "s1"}}, {"$count": "total"}])) == [
        {"total": 1}
    ]
    assert list(collection.aggregate([{"$match": {"team": "none"}}, {"$count": "total"}])) == []


def test_aggregate_match_count_indexed_mixed_numeric_values(collection):
    collection.insert_many(
        [
            {"_id": "i32", "n": 1},
            {"_id": "i64", "n": Int64(1)},
            {"_id": "double", "n": 1.0},
            {"_id": "other", "n": 2},
        ]
    )
    collection.create_index("n", name="n_1")

    for filter in [{"n": 1}, {"n": {"$eq": Int64(1)}}, {"n": 1.0}]:
        assert list(collection.aggregate([{"$match": filter}, {"$count": "total"}])) == [
            {"total": 3}
        ]


def test_aggregate_match_count_uses_compound_index_for_safe_full_key(collection):
    collection.insert_many(
        [
            {"_id": "s1", "team": "red", "active": True},
            {"_id": "s2", "team": "red", "active": False},
            {"_id": "s3", "team": "blue", "active": True},
            {"_id": "s4", "team": "red", "active": 1},
        ]
    )
    collection.create_index([("team", 1), ("active", 1)], name="team_active_1")

    assert list(collection.aggregate([{"$match": {"team": "red", "active": True}}, {"$count": "total"}])) == [
        {"total": 1}
    ]
    assert list(collection.aggregate([{"$match": {"team": "red", "active": 1}}, {"$count": "total"}])) == [
        {"total": 1}
    ]
    assert list(collection.aggregate([{"$match": {"team": "missing", "active": True}}, {"$count": "total"}])) == []


def test_aggregate_match_count_fallback_preserves_filter_errors(collection):
    seed_scores(collection)

    with pytest.raises(OperationFailure) as excinfo:
        list(collection.aggregate([{"$match": {"$where": "this.active"}}, {"$count": "total"}]))

    assert excinfo.value.code == 2
    assert "$where" in str(excinfo.value)


def seed_tagged_items(collection):
    collection.insert_many(
        [
            {"_id": "a", "tags": ["red", "blue"]},
            {"_id": "b", "tags": []},
            {"_id": "c"},
            {"_id": "d", "tags": None},
            {"_id": "e", "tags": "green"},
        ]
    )


def test_aggregate_unwind_default_and_preserve_behavior(collection):
    seed_tagged_items(collection)

    assert list(
        collection.aggregate(
            [
                {"$unwind": "$tags"},
                {"$project": {"_id": 1, "tags": 1}},
            ]
        )
    ) == [
        {"_id": "a", "tags": "red"},
        {"_id": "a", "tags": "blue"},
        {"_id": "e", "tags": "green"},
    ]

    assert list(
        collection.aggregate(
            [
                {
                    "$unwind": {
                        "path": "$tags",
                        "preserveNullAndEmptyArrays": True,
                        "includeArrayIndex": "idx",
                    }
                },
                {"$project": {"_id": 1, "tags": 1, "idx": 1}},
            ]
        )
    ) == [
        {"_id": "a", "tags": "red", "idx": 0},
        {"_id": "a", "tags": "blue", "idx": 1},
        {"_id": "b", "idx": None},
        {"_id": "c", "idx": None},
        {"_id": "d", "tags": None, "idx": None},
        {"_id": "e", "tags": "green", "idx": None},
    ]


def test_aggregate_group_scalar_accumulators(collection):
    collection.insert_many(
        [
            {"_id": "s1", "team": "red", "score": 7, "active": True},
            {"_id": "s2", "team": "blue", "score": 5, "active": False},
            {"_id": "s3", "team": "red", "score": 11, "active": True},
            {"_id": "s4", "team": "red", "score": "bad", "active": False},
            {"_id": "s5", "team": "blue", "active": True},
        ]
    )

    result = list(
        collection.aggregate(
            [
                {
                    "$group": {
                        "_id": "$team",
                        "n": {"$sum": 1},
                        "scoreTotal": {"$sum": "$score"},
                        "avgScore": {"$avg": "$score"},
                        "minScore": {"$min": "$score"},
                        "maxScore": {"$max": "$score"},
                        "firstId": {"$first": "$_id"},
                        "lastActive": {"$last": "$active"},
                    }
                },
                {"$sort": {"_id": 1}},
            ]
        )
    )

    assert result == [
        {
            "_id": "blue",
            "n": 2,
            "scoreTotal": 5,
            "avgScore": 5.0,
            "minScore": 5,
            "maxScore": 5,
            "firstId": "s2",
            "lastActive": True,
        },
        {
            "_id": "red",
            "n": 3,
            "scoreTotal": 18,
            "avgScore": 9.0,
            "minScore": 7,
            "maxScore": "bad",
            "firstId": "s1",
            "lastActive": False,
        },
    ]

    assert list(
        collection.aggregate(
            [
                {
                    "$group": {
                        "_id": {"team": "$team", "active": "$active"},
                        "n": {"$sum": 1},
                    }
                },
                {"$sort": {"n": -1}},
                {"$limit": 1},
            ]
        )
    ) == [{"_id": {"team": "red", "active": True}, "n": 2}]


def test_aggregate_unwind_group_array_accumulators_and_cursor(collection):
    collection.insert_many(
        [
            {"_id": "p1", "active": True, "tags": ["red", "blue"], "score": 7},
            {"_id": "p2", "active": True, "tags": ["red"], "score": 5},
            {"_id": "p3", "active": True, "tags": ["blue", "red"]},
            {"_id": "p4", "active": False, "tags": ["red"], "score": 99},
        ]
    )

    cursor = collection.aggregate(
        [
            {"$match": {"active": True}},
            {"$unwind": "$tags"},
            {
                "$group": {
                    "_id": "$tags",
                    "ids": {"$push": "$_id"},
                    "scores": {"$push": "$score"},
                    "uniqueIds": {"$addToSet": "$_id"},
                    "uniqueLiteral": {"$addToSet": "seen"},
                }
            },
            {"$sort": {"_id": 1}},
            {
                "$project": {
                    "_id": 1,
                    "ids": 1,
                    "scores": 1,
                    "uniqueIds": 1,
                    "uniqueLiteral": 1,
                }
            },
        ],
        batchSize=1,
    )

    assert list(cursor) == [
        {
            "_id": "blue",
            "ids": ["p1", "p3"],
            "scores": [7, None],
            "uniqueIds": ["p1", "p3"],
            "uniqueLiteral": ["seen"],
        },
        {
            "_id": "red",
            "ids": ["p1", "p2", "p3"],
            "scores": [7, 5, None],
            "uniqueIds": ["p1", "p2", "p3"],
            "uniqueLiteral": ["seen"],
        },
    ]


def test_aggregate_add_to_set_uses_whole_value_equality(collection):
    collection.insert_many(
        [
            {
                "_id": "a1",
                "case": "array-first",
                "value": [1, 2],
                "docValue": {"shape": "same", "nested": [1, 2]},
                "number": 1,
            },
            {
                "_id": "a2",
                "case": "array-first",
                "value": 1,
                "docValue": {"shape": "same", "nested": [1, 2]},
                "number": 1.0,
            },
            {
                "_id": "a3",
                "case": "array-first",
                "value": [1, 2],
                "docValue": {"shape": "same", "nested": [1, 2]},
                "number": 1,
            },
            {
                "_id": "a4",
                "case": "array-first",
                "value": [2, 1],
                "docValue": {"shape": "other", "nested": [1, 2]},
                "number": 2.0,
            },
            {
                "_id": "s1",
                "case": "scalar-first",
                "value": 1,
                "docValue": {"shape": "same", "nested": [1, 2]},
                "number": 1.0,
            },
            {
                "_id": "s2",
                "case": "scalar-first",
                "value": [1, 2],
                "docValue": {"shape": "same", "nested": [1, 2]},
                "number": 1,
            },
            {
                "_id": "s3",
                "case": "scalar-first",
                "value": 1,
                "docValue": {"shape": "same", "nested": [1, 2]},
                "number": 1,
            },
        ]
    )

    result = list(
        collection.aggregate(
            [
                {
                    "$group": {
                        "_id": "$case",
                        "values": {"$addToSet": "$value"},
                        "documents": {"$addToSet": "$docValue"},
                        "numbers": {"$addToSet": "$number"},
                        "pushed": {"$push": "$value"},
                    }
                },
                {"$sort": {"_id": 1}},
            ]
        )
    )

    assert result == [
        {
            "_id": "array-first",
            "values": [[1, 2], 1, [2, 1]],
            "documents": [
                {"shape": "same", "nested": [1, 2]},
                {"shape": "other", "nested": [1, 2]},
            ],
            "numbers": [1, 2.0],
            "pushed": [[1, 2], 1, [1, 2], [2, 1]],
        },
        {
            "_id": "scalar-first",
            "values": [1, [1, 2]],
            "documents": [{"shape": "same", "nested": [1, 2]}],
            "numbers": [1.0],
            "pushed": [1, [1, 2], 1],
        },
    ]


def test_aggregate_adversarial_errors_and_empty_groups_do_not_leak_state(collection):
    seed_scores(collection)

    assert (
        list(
            collection.aggregate(
                [
                    {"$match": {"team": "none"}},
                    {"$group": {"_id": "$team", "n": {"$sum": 1}}},
                ]
            )
        )
        == []
    )

    for pipeline, contains in [
        ([{"$group": {"_id": {"$add": ["$team", 1]}, "n": {"$sum": 1}}}], "$group"),
        ([{"$group": {"_id": ["$team"], "n": {"$sum": 1}}}], "$group"),
        ([{"$group": {"_id": "$team", "values": {"$push": ["$score"]}}}], "$group"),
        ([{"$group": {"_id": "$team", "values": {"$addToSet": {"score": "$score"}}}}], "$group"),
        ([{"$group": {"_id": {"$sum": 1}, "n": {"$sum": 1}}}], "$group"),
        ([{"$unwind": {"path": "$team", "includeArrayIndex": "team.idx"}}], "$unwind"),
    ]:
        with pytest.raises(OperationFailure) as excinfo:
            list(collection.aggregate(pipeline))
        assert contains in str(excinfo.value)

        assert list(collection.aggregate([{"$match": {"active": True}}, {"$count": "total"}])) == [
            {"total": 2}
        ]


def test_aggregate_unsupported_stage_is_explicit_error(collection):
    seed_scores(collection)

    with pytest.raises(OperationFailure) as excinfo:
        list(collection.aggregate([{"$lookup": {"from": "other"}}]))
    assert "$lookup" in str(excinfo.value)

    for group in [
        {"_id": 1, "n": {"$sum": 1, "extra": 1}},
        {"n": {"$sum": 1}},
        {"_id": "$team", "n": {"$median": "$score"}},
        {"_id": "$team", "n": {"$avg": 1}},
        {"_id": "$team", "n": {"$sum": "literal"}},
        {"_id": "$team", "n": {"$first": {"$add": [1, 2]}}},
    ]:
        with pytest.raises(OperationFailure) as excinfo:
            collection.database.command(
                {"aggregate": collection.name, "pipeline": [{"$group": group}], "cursor": {}}
            )
        assert "$group" in str(excinfo.value)


def test_aggregate_unwind_rejects_malformed_options(collection):
    seed_tagged_items(collection)

    for pipeline, contains in [
        ([{"$unwind": "$"}], "field path"),
        ([{"$unwind": {"path": "tags"}}], "field path"),
        (
            [{"$unwind": {"path": "$tags", "preserveNullAndEmptyArrays": "yes"}}],
            "preserveNullAndEmptyArrays",
        ),
        ([{"$unwind": {"path": "$tags", "includeArrayIndex": "$idx"}}], "includeArrayIndex"),
        ([{"$unwind": {"path": "$tags", "includeArrayIndex": "tags.idx"}}], "conflicting"),
        ([{"$unwind": {"path": "$tags", "unknown": True}}], "unknown"),
    ]:
        with pytest.raises(OperationFailure) as excinfo:
            list(collection.aggregate(pipeline))
        assert contains in str(excinfo.value)


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
