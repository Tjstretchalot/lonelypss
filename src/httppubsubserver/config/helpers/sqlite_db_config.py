import sqlite3
from typing import TYPE_CHECKING, AsyncIterable, Literal, Optional, Type
from httppubsubserver.config.config import (
    SubscriberInfo,
    SubscriberInfoExact,
    SubscriberInfoGlob,
)

if TYPE_CHECKING:
    from httppubsubserver.config.config import DBConfig


class SqliteDBConfig:
    """Implements the DBConfig protocol using a locally hosted SQLite database."""

    def __init__(self, database: str) -> None:
        self.database = database
        """The database url. You can pass `:memory:` to create a SQLite database that 
        exists only in memory, otherwise, this is typically the path to a sqlite file
        (usually has the `db` extension).
        """

        self.connection: Optional[sqlite3.Connection] = None
        """If we've been setup, the current connection to the database."""

        self.cursor: Optional[sqlite3.Cursor] = None
        """If we've been setup, the current cursor to the database."""

    async def setup_db(self) -> None:
        assert self.connection is None, "sqlite db config is not re-entrant"
        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()

        cursor.execute(
            """
CREATE TABLE IF NOT EXISTS httppubsub_subscription_exacts (
    id INTEGER PRIMARY KEY,
    url TEXT NOT NULL,
    exact BLOB NOT NULL
)
            """
        )
        cursor.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_httppubsub_subscription_exacts_exact_url ON httppubsub_subscription_exacts (exact, url)"
        )

        cursor.execute(
            """
CREATE TABLE IF NOT EXISTS httppubsub_subscription_globs (
    id INTEGER PRIMARY KEY,
    url TEXT NOT NULL,
    glob TEXT NOT NULL
)
            """
        )
        cursor.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_httppubsub_subscription_globs_glob_url ON httppubsub_subscription_globs (glob, url)"
        )

        connection.commit()

        self.connection = connection
        self.cursor = cursor

    async def teardown_db(self) -> None:
        exc1: Optional[BaseException] = None
        exc2: Optional[BaseException] = None
        if self.cursor is not None:
            try:
                self.cursor.close()
            except BaseException as e:
                exc1 = e
            self.cursor = None

        if self.connection is not None:
            try:
                self.connection.close()
            except BaseException as e:
                exc2 = e
            self.connection = None

        if exc2 is not None:
            raise exc2
        if exc1 is not None:
            raise exc1

    async def subscribe_exact(
        self, /, *, url: str, exact: bytes
    ) -> Literal["success", "conflict", "unavailable"]:
        assert self.cursor is not None and self.connection is not None, "db not setup"
        self.cursor.execute(
            """
INSERT INTO httppubsub_subscription_exacts (url, exact)
SELECT ?, ?
WHERE
    NOT EXISTS (
        SELECT 1 FROM httppubsub_subscription_exacts AS hse
        WHERE hse.url = ? AND hse.exact = ?
    )
            """,
            (url, exact, url, exact),
        )
        inserted = self.cursor.rowcount > 0
        self.connection.commit()

        return "success" if inserted else "conflict"

    async def unsubscribe_exact(
        self, /, *, url: str, exact: bytes
    ) -> Literal["success", "not_found", "unavailable"]:
        assert self.cursor is not None and self.connection is not None, "db not setup"

        self.cursor.execute(
            "DELETE FROM httppubsub_subscription_exacts WHERE url = ? AND exact = ?",
            (url, exact),
        )
        deleted = self.cursor.rowcount > 0
        self.connection.commit()

        return "success" if deleted else "not_found"

    async def subscribe_glob(
        self, /, *, url: str, glob: str
    ) -> Literal["success", "conflict", "unavailable"]:
        assert self.cursor is not None and self.connection is not None, "db not setup"
        self.cursor.execute(
            """
INSERT INTO httppubsub_subscription_globs (url, glob)
SELECT ?, ?
WHERE
    NOT EXISTS (
        SELECT 1 FROM httppubsub_subscription_globs AS hsg
        WHERE hsg.url = ? AND hsg.glob = ?
    )
            """,
            (url, glob, url, glob),
        )
        inserted = self.cursor.rowcount > 0
        self.connection.commit()

        return "success" if inserted else "conflict"

    async def unsubscribe_glob(
        self, /, *, url: str, glob: str
    ) -> Literal["success", "not_found", "unavailable"]:
        assert self.cursor is not None and self.connection is not None, "db not setup"
        self.cursor.execute(
            "DELETE FROM httppubsub_subscription_globs WHERE url = ? AND glob = ?",
            (url, glob),
        )
        deleted = self.cursor.rowcount > 0
        self.connection.commit()
        return "success" if deleted else "not_found"

    async def get_subscribers(
        self, /, *, topic: bytes
    ) -> AsyncIterable[SubscriberInfo]:
        assert self.connection is not None, "db not setup"
        cursor = self.connection.cursor()
        try:
            cursor.execute(
                "SELECT url FROM httppubsub_subscription_exacts WHERE exact = ?"
            )
            while True:
                row = cursor.fetchone()
                if row is None:
                    break
                yield SubscriberInfoExact(type="exact", url=row[0])

            cursor.execute(
                "SELECT glob, url FROM httppubsub_subscription_globs WHERE glob GLOB ?",
                (topic,),
            )
            while True:
                row = cursor.fetchone()
                if row is None:
                    break
                yield SubscriberInfoGlob(type="glob", glob=row[0], url=row[1])
        finally:
            cursor.close()


if TYPE_CHECKING:
    _: Type[DBConfig] = SqliteDBConfig
