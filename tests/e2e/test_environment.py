import pymongo


def test_pymongo_is_available():
    assert pymongo.version_tuple >= (4, 0)
