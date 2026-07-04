import sqlite3

import pytest
from bson.int64 import Int64
from pymongo import ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError, DuplicateKeyError, OperationFailure


pytestmark = pytest.mark.e2e


def index_names(collection):
    return [index["name"] for index in collection.list_indexes()]


def ids(cursor):
    return [doc["_id"] for doc in cursor]


def multikey_omission_count(db_path, namespace):
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM index_multikey_omissions WHERE namespace = ?",
            (namespace,),
        ).fetchone()[0]


def index_entry_count(db_path, namespace, index_name):
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as conn:
        return conn.execute(
            "SELECT COUNT(*) FROM index_entries WHERE namespace = ? AND index_name = ?",
            (namespace, index_name),
        ).fetchone()[0]


def test_create_list_and_drop_indexes(collection):
    assert index_names(collection) == ["_id_"]

    email = collection.create_index([("email", ASCENDING)], name="email_1", unique=True)
    city = collection.create_index([("profile.city", DESCENDING)])
    compound = collection.create_index(
        [("profile.city", ASCENDING), ("active", DESCENDING)],
        name="city_active_1",
    )

    assert email == "email_1"
    assert city == "profile.city_-1"
    assert compound == "city_active_1"
    assert index_names(collection) == ["_id_", "city_active_1", "email_1", "profile.city_-1"]
    assert any(index.get("unique") for index in collection.list_indexes() if index["name"] == "email_1")
    assert any(
        list(index["key"].items()) == [("profile.city", 1), ("active", -1)]
        for index in collection.list_indexes()
        if index["name"] == "city_active_1"
    )

    collection.drop_index("email_1")

    assert index_names(collection) == ["_id_", "city_active_1", "profile.city_-1"]


def test_sparse_and_partial_index_metadata_roundtrip(collection):
    collection.create_index([("email", ASCENDING)], name="email_sparse", sparse=True)
    collection.create_index(
        [("email", ASCENDING)],
        name="email_active_partial",
        partialFilterExpression={"active": True},
    )
    collection.create_index(
        [("handle", ASCENDING)],
        name="handle_exists_partial",
        partialFilterExpression={"handle": {"$exists": True}},
    )

    indexes = {index["name"]: index for index in collection.list_indexes()}

    assert indexes["email_sparse"]["sparse"] is True
    assert indexes["email_active_partial"]["partialFilterExpression"] == {"active": True}
    assert indexes["handle_exists_partial"]["partialFilterExpression"] == {
        "handle": {"$exists": True}
    }

    collection.drop_index("email_active_partial")
    assert "email_active_partial" not in index_names(collection)


def test_duplicate_index_create_is_idempotent_and_conflict_errors(collection):
    collection.create_index([("email", ASCENDING)], name="email_1")
    assert collection.create_index([("email", ASCENDING)], name="email_1") == "email_1"

    with pytest.raises(OperationFailure) as excinfo:
        collection.create_index([("email", DESCENDING)], name="email_1")

    assert excinfo.value.code == 85


def test_drop_indexes_all_preserves_id_index(collection, mongolino_server):
    collection.insert_one({"_id": "u1", "tags": ["math"]})
    collection.create_index([("email", ASCENDING)], name="email_1")
    collection.create_index([("name", ASCENDING)], name="name_1")
    collection.create_index([("tags", ASCENDING)], name="tags_1")

    assert multikey_omission_count(
        mongolino_server.db_path,
        f"{collection.database.name}.{collection.name}",
    ) == 1

    response = collection.database.command(
        {"dropIndexes": collection.name, "index": "*"}
    )

    assert response["ok"] == 1.0
    assert index_names(collection) == ["_id_"]
    assert multikey_omission_count(
        mongolino_server.db_path,
        f"{collection.database.name}.{collection.name}",
    ) == 0


def test_unsupported_index_options_are_explicit(collection):
    with pytest.raises(OperationFailure) as text_error:
        collection.create_index([("name", "text")], name="name_text")
    assert text_error.value.code == 72
    assert "text indexes are not supported" in str(text_error.value)

    with pytest.raises(OperationFailure) as partial_error:
        collection.create_index(
            [("email", ASCENDING)],
            name="email_partial",
            partialFilterExpression={"age": {"$gt": 30}},
        )
    assert partial_error.value.code == 72
    assert "partialFilterExpression operator $gt is not supported" in str(partial_error.value)

    with pytest.raises(OperationFailure) as numeric_partial_error:
        collection.create_index(
            [("email", ASCENDING)],
            name="email_numeric_partial",
            partialFilterExpression={"age": 30},
        )
    assert numeric_partial_error.value.code == 72
    assert "non-numeric scalar" in str(numeric_partial_error.value)

    with pytest.raises(OperationFailure) as id_error:
        collection.drop_index("_id_")
    assert id_error.value.code == 67


def test_unique_index_creation_rejects_existing_duplicates(collection):
    collection.insert_many(
        [
            {"_id": "u1", "email": "same@example.test"},
            {"_id": "u2", "email": "same@example.test"},
        ]
    )

    with pytest.raises(OperationFailure) as excinfo:
        collection.create_index([("email", ASCENDING)], name="email_1", unique=True)

    assert excinfo.value.code == 11000
    assert index_names(collection) == ["_id_"]


def test_unique_index_enforces_insert_update_and_upsert(collection):
    collection.insert_many(
        [
            {"_id": "u1", "email": "ada@example.test"},
            {"_id": "u2", "email": "grace@example.test"},
        ]
    )
    collection.create_index([("email", ASCENDING)], name="email_1", unique=True)

    with pytest.raises(DuplicateKeyError):
        collection.insert_one({"_id": "u3", "email": "ada@example.test"})

    with pytest.raises(DuplicateKeyError):
        collection.update_one({"_id": "u2"}, {"$set": {"email": "ada@example.test"}})
    assert collection.find_one({"_id": "u2"})["email"] == "grace@example.test"

    with pytest.raises(DuplicateKeyError):
        collection.update_one(
            {"_id": "u4"},
            {"$set": {"email": "ada@example.test"}},
            upsert=True,
        )
    assert collection.find_one({"_id": "u4"}) is None


def test_numeric_unique_index_conflicts_across_bson_number_types(collection):
    collection.insert_many([{"_id": "u1", "n": 1}, {"_id": "u2", "n": 2}])
    collection.create_index([("n", ASCENDING)], name="n_1", unique=True)

    with pytest.raises(DuplicateKeyError):
        collection.insert_one({"_id": "u3", "n": Int64(1)})

    with pytest.raises(DuplicateKeyError):
        collection.update_one({"_id": "u2"}, {"$set": {"n": 1.0}})
    assert collection.find_one({"_id": "u2"})["n"] == 2

    with pytest.raises(DuplicateKeyError):
        collection.update_one({"_id": "u4"}, {"$set": {"n": Int64(1)}}, upsert=True)
    assert collection.find_one({"_id": "u4"}) is None


def test_unique_compound_index_enforces_safe_and_fallback_values(collection):
    collection.insert_many(
        [
            {"_id": "u1", "email": "ada@example.test", "role": "admin", "n": 1},
            {"_id": "u2", "email": "grace@example.test", "role": "admin", "n": 2},
        ]
    )
    collection.create_index([("email", ASCENDING), ("role", ASCENDING)], name="email_role_1", unique=True)
    collection.create_index([("email", ASCENDING), ("n", ASCENDING)], name="email_n_1", unique=True)

    with pytest.raises(DuplicateKeyError):
        collection.insert_one({"_id": "u3", "email": "ada@example.test", "role": "admin", "n": 3})

    collection.insert_one({"_id": "u4", "email": "ada@example.test", "role": "viewer", "n": 4})
    assert collection.find_one({"_id": "u4"})["role"] == "viewer"

    with pytest.raises(DuplicateKeyError):
        collection.update_one(
            {"_id": "u4"},
            {"$set": {"role": "admin"}},
        )
    assert collection.find_one({"_id": "u4"})["role"] == "viewer"

    with pytest.raises(DuplicateKeyError):
        collection.insert_one({"_id": "u5", "email": "ada@example.test", "role": "numeric", "n": 1.0})

    with pytest.raises(DuplicateKeyError):
        collection.update_one(
            {"_id": "u6"},
            {"$set": {"email": "ada@example.test", "role": "numeric", "n": Int64(1)}},
            upsert=True,
        )
    assert collection.find_one({"_id": "u6"}) is None


def test_unique_index_missing_and_null_fallback_semantics(collection):
    collection.insert_one({"_id": "u1", "name": "missing"})
    collection.create_index([("email", ASCENDING)], name="email_1", unique=True)

    with pytest.raises(DuplicateKeyError):
        collection.insert_one({"_id": "u2", "email": None})

    collection.delete_one({"_id": "u1"})
    collection.insert_one({"_id": "u3", "email": None})

    with pytest.raises(DuplicateKeyError):
        collection.update_one({"_id": "u4"}, {"$set": {"name": "missing"}}, upsert=True)

    with pytest.raises(DuplicateKeyError):
        collection.update_one({"_id": "u4"}, {"$set": {"email": None}}, upsert=True)


def test_unique_sparse_index_membership_and_null_semantics(collection, mongolino_server):
    collection.insert_many(
        [
            {"_id": "u1", "name": "missing-a"},
            {"_id": "u2", "name": "missing-b"},
            {"_id": "u3", "email": None},
            {"_id": "u4", "email": "ada@example.test"},
        ]
    )
    collection.create_index([("email", ASCENDING)], name="email_sparse", unique=True, sparse=True)

    namespace = f"{collection.database.name}.{collection.name}"
    assert index_entry_count(mongolino_server.db_path, namespace, "email_sparse") == 2

    collection.insert_one({"_id": "u5", "name": "missing-c"})
    with pytest.raises(DuplicateKeyError):
        collection.insert_one({"_id": "u6", "email": None})
    with pytest.raises(DuplicateKeyError):
        collection.insert_one({"_id": "u7", "email": "ada@example.test"})

    collection.update_one({"_id": "u5"}, {"$set": {"email": "grace@example.test"}})
    assert index_entry_count(mongolino_server.db_path, namespace, "email_sparse") == 3
    with pytest.raises(DuplicateKeyError):
        collection.update_one({"_id": "u2"}, {"$set": {"email": "grace@example.test"}})

    collection.update_one({"_id": "u5"}, {"$unset": {"email": ""}})
    assert index_entry_count(mongolino_server.db_path, namespace, "email_sparse") == 2


def test_unique_compound_sparse_requires_all_fields(collection, mongolino_server):
    collection.insert_many(
        [
            {"_id": "u1", "email": "ada@example.test"},
            {"_id": "u2", "role": "admin"},
            {"_id": "u3", "email": "ada@example.test", "role": "admin"},
            {"_id": "u4", "email": "grace@example.test", "role": "admin"},
        ]
    )
    collection.create_index(
        [("email", ASCENDING), ("role", ASCENDING)],
        name="email_role_sparse",
        unique=True,
        sparse=True,
    )

    namespace = f"{collection.database.name}.{collection.name}"
    assert index_entry_count(mongolino_server.db_path, namespace, "email_role_sparse") == 2

    collection.insert_one({"_id": "u5", "email": "ada@example.test"})
    collection.insert_one({"_id": "u6", "role": "admin"})
    with pytest.raises(DuplicateKeyError):
        collection.insert_one({"_id": "u7", "email": "ada@example.test", "role": "admin"})


def test_unique_partial_index_membership_and_supported_predicates(collection, mongolino_server):
    collection.insert_many(
        [
            {"_id": "u1", "email": "same@example.test", "active": False},
            {"_id": "u2", "email": "same@example.test"},
            {"_id": "u3", "email": "same@example.test", "active": True},
            {"_id": "u4", "email": "other@example.test", "active": True, "handle": "other"},
        ]
    )
    collection.create_index(
        [("email", ASCENDING)],
        name="email_active_partial",
        unique=True,
        partialFilterExpression={"active": True},
    )
    collection.create_index(
        [("handle", ASCENDING)],
        name="handle_active_partial",
        unique=True,
        partialFilterExpression={
            "$and": [{"active": {"$eq": True}}, {"handle": {"$exists": True}}]
        },
    )

    namespace = f"{collection.database.name}.{collection.name}"
    assert index_entry_count(mongolino_server.db_path, namespace, "email_active_partial") == 2
    assert index_entry_count(mongolino_server.db_path, namespace, "handle_active_partial") == 1

    collection.insert_one({"_id": "u5", "email": "same@example.test", "active": False})
    with pytest.raises(DuplicateKeyError):
        collection.insert_one({"_id": "u6", "email": "same@example.test", "active": True})

    collection.update_one({"_id": "u5"}, {"$set": {"email": "new@example.test", "active": True}})
    assert index_entry_count(mongolino_server.db_path, namespace, "email_active_partial") == 3
    with pytest.raises(DuplicateKeyError):
        collection.update_one({"_id": "u2"}, {"$set": {"active": True}})

    collection.update_one({"_id": "u5"}, {"$set": {"active": False}})
    assert index_entry_count(mongolino_server.db_path, namespace, "email_active_partial") == 2


def test_unique_unordered_bulk_partial_success_and_drop_index(collection):
    collection.create_index([("email", ASCENDING)], name="email_1", unique=True)

    with pytest.raises(BulkWriteError) as excinfo:
        collection.insert_many(
            [
                {"_id": "u1", "email": "same@example.test"},
                {"_id": "u2", "email": "same@example.test"},
                {"_id": "u3", "email": "other@example.test"},
            ],
            ordered=False,
        )

    assert excinfo.value.details["nInserted"] == 2
    assert excinfo.value.details["writeErrors"][0]["index"] == 1

    collection.drop_index("email_1")
    collection.insert_one({"_id": "u4", "email": "same@example.test"})
    assert collection.count_documents({"email": "same@example.test"}) == 2


def test_unique_index_rejects_array_values(collection):
    collection.insert_one({"_id": "u1", "emails": ["a@example.test"]})

    with pytest.raises(OperationFailure) as excinfo:
        collection.create_index([("emails", ASCENDING)], name="emails_1", unique=True)

    assert excinfo.value.code == 72
    assert "does not support array value" in str(excinfo.value)


def test_indexed_find_falls_back_when_single_field_index_has_array_omissions(collection):
    collection.insert_many(
        [
            {"_id": "u1", "tags": ["math", "logic"], "nested": [{"kind": "first"}, {"kind": "second"}]},
            {"_id": "u2", "tags": "math", "nested": {"kind": "second"}},
            {"_id": "u3", "tags": "systems", "nested": {"kind": "first"}},
        ]
    )
    collection.create_index([("tags", ASCENDING)], name="tags_1")
    collection.create_index([("nested.kind", ASCENDING)], name="nested_kind_1")

    assert [doc["_id"] for doc in collection.find({"tags": "math"}).sort("_id", ASCENDING)] == [
        "u1",
        "u2",
    ]
    assert [doc["_id"] for doc in collection.find({"nested.kind": "second"}).sort("_id", ASCENDING)] == [
        "u1",
        "u2",
    ]


def test_indexed_find_falls_back_when_compound_index_has_array_omissions(collection):
    collection.insert_many(
        [
            {"_id": "u1", "tags": ["math"], "active": True},
            {"_id": "u2", "tags": "math", "active": True},
            {"_id": "u3", "tags": "math", "active": False},
        ]
    )
    collection.create_index([("tags", ASCENDING), ("active", ASCENDING)], name="tags_active_1")

    assert [doc["_id"] for doc in collection.find({"tags": "math", "active": True}).sort("_id", ASCENDING)] == [
        "u1",
        "u2",
    ]


def test_indexed_query_results_stay_correct_after_mutations(collection):
    collection.insert_many(
        [
            {"_id": "u1", "name": "Ada", "profile": {"city": "Rome"}},
            {"_id": "u2", "name": "Grace", "profile": {"city": "London"}},
            {"_id": "u3", "name": "Katherine", "profile": {"city": "Rome"}},
        ]
    )
    collection.create_index([("profile.city", ASCENDING)], name="city_1")

    assert [doc["_id"] for doc in collection.find({"profile.city": "Rome"}).sort("_id", 1)] == [
        "u1",
        "u3",
    ]
    assert collection.count_documents({"profile.city": "Rome"}) == 2
    assert collection.count_documents({"profile.city": {"$eq": "Rome"}}, skip=1, limit=1) == 1

    collection.update_one({"_id": "u1"}, {"$set": {"profile.city": "Milan"}})
    assert [doc["_id"] for doc in collection.find({"profile.city": "Rome"}).sort("_id", 1)] == [
        "u3"
    ]
    assert [doc["_id"] for doc in collection.find({"profile.city": "Milan"}).sort("_id", 1)] == [
        "u1"
    ]
    assert collection.count_documents({"profile.city": "Rome"}) == 1
    assert collection.count_documents({"profile.city": "Milan"}) == 1

    collection.delete_one({"_id": "u3"})
    assert list(collection.find({"profile.city": "Rome"})) == []
    assert collection.count_documents({"profile.city": "Rome"}) == 0

    collection.drop_index("city_1")
    assert [doc["_id"] for doc in collection.find({"profile.city": "Milan"}).sort("_id", 1)] == [
        "u1"
    ]
    assert collection.count_documents({"profile.city": "Milan"}) == 1


def test_sparse_and_partial_indexed_find_preserves_membership_safety(collection):
    collection.insert_many(
        [
            {"_id": "u1", "email": "same@example.test", "active": True, "handle": "ada"},
            {"_id": "u2", "email": "same@example.test", "active": False},
            {"_id": "u3", "name": "missing"},
            {"_id": "u4", "email": "other@example.test", "active": True, "handle": "grace"},
        ]
    )
    collection.create_index([("email", ASCENDING)], name="email_sparse", sparse=True)
    collection.create_index(
        [("email", ASCENDING)],
        name="email_active_partial",
        partialFilterExpression={"active": True},
    )
    collection.create_index(
        [("email", ASCENDING)],
        name="email_active_handle_partial",
        partialFilterExpression={
            "$and": [{"active": {"$eq": True}}, {"handle": {"$exists": True}}]
        },
    )

    assert ids(collection.find({"email": "same@example.test"})) == ["u1", "u2"]
    assert ids(collection.find({"email": "same@example.test", "active": True})) == ["u1"]
    assert ids(
        collection.find({"email": "same@example.test", "active": True, "handle": "ada"})
    ) == ["u1"]
    assert ids(collection.find({"email": "same@example.test", "active": False})) == ["u2"]
    assert ids(collection.find({"active": True})) == ["u1", "u4"]


def test_compound_indexed_query_results_stay_correct_after_mutations(collection):
    collection.insert_many(
        [
            {"_id": "u1", "email": "ada@example.test", "profile": {"city": "Rome"}, "active": True},
            {"_id": "u2", "email": "grace@example.test", "profile": {"city": "London"}, "active": True},
            {"_id": "u3", "email": "katherine@example.test", "profile": {"city": "Rome"}, "active": True},
            {"_id": "u4", "email": "unsafe@example.test", "profile": {"city": "Rome"}, "active": 1},
        ]
    )
    collection.create_index([("profile.city", ASCENDING), ("active", ASCENDING)], name="city_active_1")

    assert [doc["_id"] for doc in collection.find({"profile.city": "Rome", "active": True}).sort("_id", 1)] == [
        "u1",
        "u3",
    ]
    assert collection.count_documents({"profile.city": "Rome", "active": True}) == 2
    assert collection.count_documents({"profile.city": "Rome", "active": 1}) == 1

    collection.update_one({"_id": "u1"}, {"$set": {"profile.city": "Milan"}})
    assert [doc["_id"] for doc in collection.find({"profile.city": "Rome", "active": True}).sort("_id", 1)] == [
        "u3"
    ]
    assert [doc["_id"] for doc in collection.find({"profile.city": "Milan", "active": True}).sort("_id", 1)] == [
        "u1"
    ]

    collection.update_one(
        {"email": "grace@example.test"},
        {"$set": {"profile.city": "Rome"}},
    )
    assert [doc["_id"] for doc in collection.find({"profile.city": "Rome", "active": True}).sort("_id", 1)] == [
        "u2",
        "u3",
    ]

    collection.delete_one({"profile.city": "Rome", "active": True})
    assert [doc["_id"] for doc in collection.find({"profile.city": "Rome", "active": True}).sort("_id", 1)] == [
        "u3"
    ]

    collection.drop_index("city_active_1")
    assert [doc["_id"] for doc in collection.find({"profile.city": "Rome", "active": True}).sort("_id", 1)] == [
        "u3"
    ]


def test_indexed_scalar_write_targeting_keeps_entries_fresh(collection):
    collection.insert_many(
        [
            {"_id": "u1", "email": "ada@example.test", "active": True},
            {"_id": "u2", "email": "grace@example.test", "active": True},
            {"_id": "u3", "email": "katherine@example.test", "active": False},
        ]
    )
    collection.create_index([("email", ASCENDING)], name="email_1")
    collection.create_index([("active", ASCENDING)], name="active_1")

    one = collection.update_one(
        {"email": "ada@example.test"},
        {"$set": {"email": "ada.lovelace@example.test"}},
    )
    assert one.matched_count == 1
    assert one.modified_count == 1
    assert collection.find_one({"email": "ada@example.test"}) is None
    assert collection.find_one({"email": "ada.lovelace@example.test"})["_id"] == "u1"

    many = collection.delete_many({"active": True})
    assert many.deleted_count == 2
    assert collection.count_documents({"active": True}) == 0
    assert [doc["_id"] for doc in collection.find({"active": False})] == ["u3"]
