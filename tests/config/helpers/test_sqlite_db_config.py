import sqlite3

import pytest

from lonelypss.config.helpers.sqlite_db_config import (
    _ExistingGlobsIter,
    _ExistingTopicsIter,
)


@pytest.mark.parametrize(
    ("iterator_cls", "create_sql", "insert_sql", "expected"),
    [
        pytest.param(
            _ExistingTopicsIter,
            "CREATE TABLE httppubsub_subscription_exacts (url TEXT, exact BLOB)",
            "INSERT INTO httppubsub_subscription_exacts (url, exact) VALUES (?, ?)",
            [index.to_bytes(2, "big") for index in range(17)],
            id="topics",
        ),
        pytest.param(
            _ExistingGlobsIter,
            "CREATE TABLE httppubsub_subscription_globs (url TEXT, glob TEXT)",
            "INSERT INTO httppubsub_subscription_globs (url, glob) VALUES (?, ?)",
            [f"topic-{index:02d}" for index in range(17)],
            id="globs",
        ),
    ],
)
def test_existing_subscription_iterators_advance_between_batches(
    iterator_cls, create_sql, insert_sql, expected
):
    connection = sqlite3.connect(":memory:")
    connection.execute(create_sql)
    connection.executemany(
        insert_sql, [("https://example.com", subscription) for subscription in expected]
    )

    iterator = iterator_cls(connection.cursor(), "https://example.com")
    actual = [next(iterator) for _ in expected]

    assert actual == expected
