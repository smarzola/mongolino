import sqlite3
from datetime import datetime, timedelta, timezone

import pytest
from bson import BSON
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


def stored_ids(db_path, namespace):
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
        rows = conn.execute(
            "SELECT bson FROM documents WHERE namespace = ? ORDER BY created_at",
            (namespace,),
        ).fetchall()
    return [BSON(row[0]).decode()["_id"] for row in rows]


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


def test_aggregate_first_match_indexed_and_id_candidates(collection):
    collection.insert_many(
        [
            {"_id": "p1", "team": "red", "score": 7, "email": "ada@example.test"},
            {"_id": "p2", "team": "blue", "score": 5, "email": "grace@example.test"},
            {"_id": "p3", "team": "red", "score": 11, "email": "kat@example.test"},
        ]
    )
    collection.create_index("team", name="team_1")

    assert list(
        collection.aggregate(
            [
                {"$match": {"team": "red"}},
                {"$addFields": {"label": {"$concat": ["$team", ":", "$email"]}}},
                {"$sort": {"score": -1}},
                {"$project": {"_id": 1, "label": 1}},
            ]
        )
    ) == [
        {"_id": "p3", "label": "red:kat@example.test"},
        {"_id": "p1", "label": "red:ada@example.test"},
    ]

    assert list(
        collection.aggregate(
            [
                {"$match": {"_id": "p2"}},
                {
                    "$lookup": {
                        "from": collection.name,
                        "localField": "team",
                        "foreignField": "team",
                        "as": "sameTeam",
                    }
                },
                {"$project": {"_id": 1, "sameTeam": 1}},
            ]
        )
    ) == [
        {
            "_id": "p2",
            "sameTeam": [
                {"_id": "p2", "team": "blue", "score": 5, "email": "grace@example.test"}
            ],
        }
    ]


def test_aggregate_computed_project_add_set_and_unset(collection):
    collection.insert_many(
        [
            {
                "_id": "a1",
                "first": "Ada",
                "last": "Lovelace",
                "score": 7,
                "profile": {"city": "London", "hidden": True},
                "tags": ["math", "logic"],
            },
            {
                "_id": "a2",
                "first": "Grace",
                "last": "Hopper",
                "score": 9,
                "profile": {"city": "Arlington", "hidden": True},
                "tags": ["compiler"],
            },
        ]
    )

    assert list(
        collection.aggregate(
            [
                {"$match": {"_id": "a1"}},
                {
                    "$project": {
                        "_id": 0,
                        "first": 1,
                        "display": {"$concat": ["$first", " ", "$last"]},
                        "nested": {
                            "city": "$profile.city",
                            "scoreText": {"$toString": "$score"},
                        },
                        "rootCopy": "$$ROOT._id",
                    }
                },
                {
                    "$addFields": {
                        "nested.lower": {"$toLower": "$display"},
                        "computed.total": {"$add": [4, 6]},
                    }
                },
                {"$set": {"alias": "$nested.city"}},
                {"$unset": ["first", "nested.scoreText"]},
            ]
        )
    ) == [
        {
            "display": "Ada Lovelace",
            "nested": {"city": "London", "lower": "ada lovelace"},
            "rootCopy": "a1",
            "computed": {"total": 10},
            "alias": "London",
        }
    ]

    assert list(
        collection.aggregate(
            [
                {"$match": {"_id": "a2"}},
                {"$unset": "profile.hidden"},
                {"$project": {"tags": 0, "_id": 0}},
            ]
        )
    ) == [
        {
            "first": "Grace",
            "last": "Hopper",
            "score": 9,
            "profile": {"city": "Arlington"},
        }
    ]


def test_aggregate_replace_root_replace_with_and_group_computed_operands(collection):
    collection.insert_many(
        [
            {
                "_id": "o1",
                "customer": {"id": "c1", "name": "Ada"},
                "price": 7,
                "tax": 2,
                "status": "open",
            },
            {
                "_id": "o2",
                "customer": {"id": "c1", "name": "Ada"},
                "price": 5,
                "tax": 1,
                "status": "closed",
            },
            {
                "_id": "o3",
                "customer": {"id": "c2", "name": "Grace"},
                "price": 9,
                "tax": 3,
                "status": "open",
            },
        ]
    )

    assert list(
        collection.aggregate(
            [
                {"$match": {"_id": "o1"}},
                {"$replaceRoot": {"newRoot": "$customer"}},
            ]
        )
    ) == [{"id": "c1", "name": "Ada"}]

    assert list(
        collection.aggregate(
            [
                {"$match": {"_id": "o3"}},
                {
                    "$replaceWith": {
                        "customerId": "$customer.id",
                        "label": {"$concat": ["$customer.name", ":", "$status"]},
                        "total": {"$add": ["$price", "$tax"]},
                    }
                },
            ]
        )
    ) == [{"customerId": "c2", "label": "Grace:open", "total": 12}]

    assert list(
        collection.aggregate(
            [
                {
                    "$group": {
                        "_id": {
                            "customer": "$customer.id",
                            "open": {"$eq": ["$status", "open"]},
                        },
                        "gross": {"$sum": {"$add": ["$price", "$tax"]}},
                        "avgGross": {"$avg": {"$add": ["$price", "$tax"]}},
                        "labels": {"$push": {"$concat": ["$customer.name", ":", "$status"]}},
                        "snapshots": {
                            "$addToSet": {
                                "status": "$status",
                                "total": {"$add": ["$price", "$tax"]},
                            }
                        },
                        "firstTotal": {"$first": {"$add": ["$price", "$tax"]}},
                        "lastUpper": {"$last": {"$toUpper": "$status"}},
                        "minTotal": {"$min": {"$add": ["$price", "$tax"]}},
                        "maxTotal": {"$max": {"$add": ["$price", "$tax"]}},
                    }
                },
                {"$sort": {"gross": -1}},
                {"$limit": 1},
            ]
        )
    ) == [
        {
            "_id": {"customer": "c2", "open": True},
            "gross": 12,
            "avgGross": 12.0,
            "labels": ["Grace:open"],
            "snapshots": [{"status": "open", "total": 12}],
            "firstTotal": 12,
            "lastUpper": "OPEN",
            "minTotal": 12,
            "maxTotal": 12,
        }
    ]


def test_aggregate_lookup_simple_equality_arrays_null_collation_self_and_cursor(collection):
    collection.insert_many(
        [
            {"_id": "o1", "profileId": "p1", "profileIds": ["p2", "missing"], "owner": "ADA"},
            {"_id": "o2", "profileId": None, "owner": "grace"},
            {"_id": "o3", "owner": "missing"},
        ]
    )
    profiles = collection.database[f"{collection.name}_profiles"]
    profiles.insert_many(
        [
            {"_id": "p1", "name": "Ada", "owner": "ada"},
            {"_id": "p2", "name": "Grace", "owner": "GRACE"},
            {"_id": "p3", "name": "Nullish", "profileId": None},
            {"_id": "p4", "name": "MissingForeign"},
        ]
    )

    cursor = collection.aggregate(
        [
            {
                "$lookup": {
                    "from": profiles.name,
                    "localField": "profileId",
                    "foreignField": "_id",
                    "as": "profile",
                }
            },
            {
                "$lookup": {
                    "from": profiles.name,
                    "localField": "profileIds",
                    "foreignField": "_id",
                    "as": "arrayMatches",
                }
            },
            {
                "$lookup": {
                    "from": profiles.name,
                    "localField": "profileId",
                    "foreignField": "profileId",
                    "as": "nullMatches",
                }
            },
            {"$sort": {"_id": 1}},
            {"$project": {"_id": 1, "profile": 1, "arrayMatches": 1, "nullMatches": 1}},
        ],
        batchSize=2,
    )

    assert list(cursor) == [
        {
            "_id": "o1",
            "profile": [{"_id": "p1", "name": "Ada", "owner": "ada"}],
            "arrayMatches": [{"_id": "p2", "name": "Grace", "owner": "GRACE"}],
            "nullMatches": [],
        },
        {
            "_id": "o2",
            "profile": [],
            "arrayMatches": [],
            "nullMatches": [
                {"_id": "p1", "name": "Ada", "owner": "ada"},
                {"_id": "p2", "name": "Grace", "owner": "GRACE"},
                {"_id": "p3", "name": "Nullish", "profileId": None},
                {"_id": "p4", "name": "MissingForeign"},
            ],
        },
        {
            "_id": "o3",
            "profile": [],
            "arrayMatches": [],
            "nullMatches": [
                {"_id": "p1", "name": "Ada", "owner": "ada"},
                {"_id": "p2", "name": "Grace", "owner": "GRACE"},
                {"_id": "p3", "name": "Nullish", "profileId": None},
                {"_id": "p4", "name": "MissingForeign"},
            ],
        },
    ]

    assert list(
        collection.aggregate(
            [
                {
                    "$lookup": {
                        "from": profiles.name,
                        "localField": "owner",
                        "foreignField": "owner",
                        "as": "owners",
                    }
                },
                {"$sort": {"_id": 1}},
                {"$project": {"_id": 1, "owners": 1}},
            ],
            collation={"locale": "en", "strength": 2},
        )
    ) == [
        {"_id": "o1", "owners": [{"_id": "p1", "name": "Ada", "owner": "ada"}]},
        {"_id": "o2", "owners": [{"_id": "p2", "name": "Grace", "owner": "GRACE"}]},
        {"_id": "o3", "owners": []},
    ]

    assert list(
        collection.aggregate(
            [
                {"$match": {"_id": "o1"}},
                {
                    "$lookup": {
                        "from": collection.name,
                        "localField": "_id",
                        "foreignField": "_id",
                        "as": "self",
                    }
                },
                {"$project": {"_id": 1, "self": 1}},
            ]
        )
    ) == [
        {
            "_id": "o1",
            "self": [
                {
                    "_id": "o1",
                    "profileId": "p1",
                    "profileIds": ["p2", "missing"],
                    "owner": "ADA",
                }
            ],
        }
    ]


def test_aggregate_lookup_sweeps_foreign_ttl_before_join(collection, mongolino_server):
    now = datetime.now(timezone.utc)
    past = now - timedelta(days=1)
    future = now + timedelta(days=1)
    profiles = collection.database[f"{collection.name}_profiles"]

    collection.insert_many(
        [
            {"_id": "o1", "profileId": "live"},
            {"_id": "o2", "profileId": "expired"},
        ]
    )
    profiles.create_index("expiresAt", name="expires_ttl", expireAfterSeconds=60)
    profiles.insert_many(
        [
            {"_id": "live", "expiresAt": future, "name": "Live"},
            {"_id": "expired", "expiresAt": past, "name": "Expired"},
        ]
    )

    joined = list(
        collection.aggregate(
            [
                {
                    "$lookup": {
                        "from": profiles.name,
                        "localField": "profileId",
                        "foreignField": "_id",
                        "as": "profile",
                    }
                },
                {"$sort": {"_id": 1}},
                {"$project": {"_id": 1, "profile": 1}},
            ]
        )
    )
    assert joined[0]["_id"] == "o1"
    assert joined[0]["profile"][0]["_id"] == "live"
    assert joined[0]["profile"][0]["name"] == "Live"
    assert joined[1] == {"_id": "o2", "profile": []}
    assert stored_ids(mongolino_server.db_path, f"{profiles.database.name}.{profiles.name}") == [
        "live"
    ]


def test_aggregate_match_sort_and_count_with_collation(collection):
    collection.insert_many(
        [
            {"_id": "s1", "team": "Red", "name": "bravo"},
            {"_id": "s2", "team": "red", "name": "Alpha"},
            {"_id": "s3", "team": "BLUE", "name": "charlie"},
        ]
    )
    collation = {"locale": "en", "strength": 2}

    result = list(
        collection.aggregate(
            [
                {"$match": {"team": "RED"}},
                {"$sort": {"name": 1}},
                {"$project": {"_id": 1, "name": 1}},
            ],
            collation=collation,
        )
    )
    assert result == [{"_id": "s2", "name": "Alpha"}, {"_id": "s1", "name": "bravo"}]

    assert list(
        collection.aggregate(
            [{"$match": {"team": "red"}}, {"$count": "total"}],
            collation=collation,
        )
    ) == [{"total": 2}]


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


def test_aggregate_match_count_uses_sparse_and_partial_membership_filters(collection):
    collection.insert_many(
        [
            {"_id": "u1", "email": "same@example.test", "active": True, "handle": "ada"},
            {"_id": "u2", "email": "same@example.test", "active": False},
            {"_id": "u3", "name": "missing"},
            {"_id": "u4", "email": "other@example.test", "active": True, "handle": "grace"},
        ]
    )
    collection.create_index("email", name="email_sparse", sparse=True)
    collection.create_index(
        "email",
        name="email_active_partial",
        partialFilterExpression={"active": True},
    )

    assert list(
        collection.aggregate(
            [{"$match": {"email": "same@example.test", "active": True}}, {"$count": "total"}]
        )
    ) == [{"total": 1}]
    assert list(
        collection.aggregate(
            [{"$match": {"email": "same@example.test", "active": False}}, {"$count": "total"}]
        )
    ) == [{"total": 1}]
    assert list(collection.aggregate([{"$match": {"active": True}}, {"$count": "total"}])) == [
        {"total": 2}
    ]


def test_aggregate_match_count_uses_scalar_multikey_entries(collection):
    collection.insert_many(
        [
            {"_id": "s1", "tags": ["red", "red"], "active": True},
            {"_id": "s2", "tags": "red", "active": True},
            {"_id": "s3", "tags": "red", "active": False},
            {"_id": "s4", "scores": [1, 2]},
        ]
    )
    collection.create_index("tags", name="tags_1")
    collection.create_index([("tags", 1), ("active", 1)], name="tags_active_1")
    collection.create_index("scores", name="scores_1")

    assert list(collection.aggregate([{"$match": {"tags": "red"}}, {"$count": "total"}])) == [
        {"total": 3}
    ]
    assert list(collection.aggregate([{"$match": {"tags": "red", "active": True}}, {"$count": "total"}])) == [
        {"total": 2}
    ]
    assert list(collection.aggregate([{"$match": {"scores": 1}}, {"$count": "total"}])) == [
        {"total": 1}
    ]


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
        ([{"$group": {"_id": {"$add": ["$team", 1]}, "n": {"$sum": 1}}}], "$add"),
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
        list(collection.aggregate([{"$facet": {"scores": [{"$match": {}}]}}]))
    assert "$facet" in str(excinfo.value)

    for group in [
        {"_id": 1, "n": {"$sum": 1, "extra": 1}},
        {"n": {"$sum": 1}},
        {"_id": "$team", "n": {"$median": "$score"}},
        {"_id": "$team", "n": {"$avg": 1}},
        {"_id": "$team", "n": {"$sum": "literal"}},
        {
            "_id": "$team",
            "n": {
                "$first": {
                    "$dateDiff": {
                        "startDate": "$created",
                        "endDate": "$updated",
                        "unit": "day",
                    }
                }
            },
        },
    ]:
        with pytest.raises(OperationFailure) as excinfo:
            collection.database.command(
                {"aggregate": collection.name, "pipeline": [{"$group": group}], "cursor": {}}
            )
        assert "$group" in str(excinfo.value)


def test_aggregate_replace_root_rejects_malformed_and_non_document_results(collection):
    seed_scores(collection)

    for pipeline, contains in [
        ([{"$replaceRoot": "$meta"}], "newRoot"),
        ([{"$replaceRoot": {}}], "newRoot"),
        ([{"$replaceRoot": {"newRoot": "$meta", "extra": True}}], "extra"),
        ([{"$replaceWith": {"$dateDiff": {}}}], "$dateDiff"),
        ([{"$replaceRoot": {"newRoot": "$missing"}}], "document"),
        ([{"$replaceWith": "$score"}], "document"),
    ]:
        with pytest.raises(OperationFailure) as excinfo:
            list(collection.aggregate(pipeline))
        assert contains in str(excinfo.value)


def test_aggregate_lookup_rejects_malformed_and_unsupported_forms(collection):
    seed_scores(collection)

    for pipeline, contains in [
        ([{"$lookup": "bad"}], "$lookup"),
        ([{"$lookup": {"from": "profiles"}}], "localField"),
        (
            [
                {
                    "$lookup": {
                        "from": "other.profiles",
                        "localField": "profileId",
                        "foreignField": "_id",
                        "as": "profile",
                    }
                }
            ],
            "cross-database",
        ),
        (
            [
                {
                    "$lookup": {
                        "from": "profiles",
                        "localField": "profileId",
                        "foreignField": "_id",
                        "as": "profile",
                        "pipeline": [],
                    }
                }
            ],
            "pipeline",
        ),
        (
            [
                {
                    "$lookup": {
                        "from": "profiles",
                        "localField": "profileId",
                        "foreignField": "_id",
                        "as": "profile",
                        "let": {},
                    }
                }
            ],
            "let",
        ),
        (
            [
                {
                    "$lookup": {
                        "from": "profiles",
                        "localField": "profile..id",
                        "foreignField": "_id",
                        "as": "profile",
                    }
                }
            ],
            "empty segment",
        ),
        (
            [
                {
                    "$lookup": {
                        "from": "profiles",
                        "localField": "profileId",
                        "foreignField": "_id",
                        "as": "$profile",
                    }
                }
            ],
            "$",
        ),
    ]:
        with pytest.raises(OperationFailure) as excinfo:
            list(collection.aggregate(pipeline))
        assert contains in str(excinfo.value)


def test_aggregate_shaping_rejects_malformed_paths_and_runtime_errors(collection):
    seed_scores(collection)

    for pipeline, contains in [
        ([{"$project": {"team": 1, "team.name": "$team"}}], "conflicting"),
        ([{"$project": {"team": 0, "display": {"$literal": 1}}}], "computed"),
        ([{"$project": {"": "$team"}}], "empty"),
        ([{"$project": {"$bad": "$team"}}], "$"),
        ([{"$addFields": {"profile": "$team", "profile.city": "$team"}}], "conflicting"),
        ([{"$set": {"profile.$bad": "$team"}}], "$"),
        ([{"$unset": ["profile", "profile.city"]}], "conflicting"),
        ([{"$unset": [1]}], "strings"),
        ([{"$addFields": {"ratio": {"$divide": [10, 0]}}}], "divide"),
    ]:
        with pytest.raises(OperationFailure) as excinfo:
            list(collection.aggregate(pipeline))
        assert contains in str(excinfo.value)


def test_aggregate_static_expression_errors_do_not_sweep_ttl(collection, mongolino_server):
    now = datetime.now(timezone.utc)
    past = now - timedelta(days=1)
    future = now + timedelta(days=1)
    collection.create_index("expiresAt", name="expires_ttl", expireAfterSeconds=60)
    collection.insert_many(
        [
            {"_id": "expired", "expiresAt": past, "category": "old"},
            {"_id": "future", "expiresAt": future, "category": "new"},
        ]
    )

    for pipeline, contains in [
        ([{"$addFields": {"ratio": {"$divide": [10, 0]}}}], "divide"),
        ([{"$addFields": {"total": {"$add": [1, "bad"]}}}], "$add"),
        ([{"$set": {"total": {"$multiply": [2, "bad"]}}}], "$multiply"),
        ([{"$project": {"lower": {"$toLower": 1}}}], "$toLower"),
        ([{"$project": {"label": {"$concat": ["ok", 1]}}}], "$concat"),
        ([{"$replaceRoot": {"newRoot": 1}}], "document"),
        ([{"$replaceWith": "literal"}], "document"),
    ]:
        with pytest.raises(OperationFailure) as excinfo:
            list(collection.aggregate(pipeline))
        assert contains in str(excinfo.value)
        assert stored_ids(
            mongolino_server.db_path,
            f"{collection.database.name}.{collection.name}",
        ) == ["expired", "future"]

    assert list(collection.find({}, {"_id": 1}).sort("_id", 1)) == [{"_id": "future"}]


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
