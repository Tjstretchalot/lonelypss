import sqlite3
from typing import (
    TYPE_CHECKING,
    AsyncIterable,
    List,
    Literal,
    Optional,
    Tuple,
    Type,
    cast,
)

from lonelypsp.stateless.make_strong_etag import (
    StrongEtag,
    create_strong_etag_generator,
)

from lonelypss.config.config import (
    SubscriberInfo,
    SubscriberInfoExact,
    SubscriberInfoGlob,
)
from lonelypss.config.set_subscriptions_info import SetSubscriptionsInfo

if TYPE_CHECKING:
    from lonelypss.config.config import DBConfig


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
                "SELECT url FROM httppubsub_subscription_exacts WHERE exact = ?",
                (topic,),
            )
            while True:
                row = cursor.fetchone()
                if row is None:
                    break
                yield SubscriberInfoExact(type="exact", url=row[0])

            cursor.execute(
                "SELECT glob, url FROM httppubsub_subscription_globs WHERE ? GLOB glob",
                (topic,),
            )
            while True:
                row = cursor.fetchone()
                if row is None:
                    break
                yield SubscriberInfoGlob(type="glob", glob=row[0], url=row[1])
        finally:
            cursor.close()

    async def check_subscriptions(self, /, *, url: str) -> StrongEtag:
        assert self.connection is not None, "db not setup"

        topics_gen = create_strong_etag_generator(url)

        cursor = self.connection.cursor()
        try:
            last_topic: Optional[bytes] = None
            while True:
                cursor.execute(
                    "SELECT exact "
                    "FROM httppubsub_subscription_exacts "
                    "WHERE url = ?"
                    " AND (? IS NULL OR exact > ?) "
                    "ORDER BY exact ASC "
                    "LIMIT 100",
                    (url, last_topic, last_topic),
                )

                topics_batch = cast(List[Tuple[bytes]], cursor.fetchall())
                if not topics_batch:
                    break

                topics_gen.add_topic(*(topic[0] for topic in topics_batch))

                last_topic = topics_batch[-1][0]

            globs_gen = topics_gen.finish_topics()
            del topics_gen

            last_glob: Optional[str] = None
            while True:
                cursor.execute(
                    "SELECT glob "
                    "FROM httppubsub_subscription_globs "
                    "WHERE url = ?"
                    " AND (? IS NULL OR glob > ?) "
                    "ORDER BY glob ASC "
                    "LIMIT 100",
                    (url, last_glob, last_glob),
                )

                globs_batch = cast(List[Tuple[str]], cursor.fetchall())
                if not globs_batch:
                    break

                globs_gen.add_glob(*(glob[0] for glob in globs_batch))

                last_glob = globs_batch[-1][0]

            return globs_gen.finish()
        finally:
            cursor.close()

    async def set_subscriptions(
        self,
        /,
        *,
        url: str,
        strong_etag: StrongEtag,
        subscriptions: SetSubscriptionsInfo,
    ) -> Literal["success", "unavailable"]:
        assert self.connection is not None, "db not setup"

        cursor = self.connection.cursor()
        try:
            cursor.execute(
                "DELETE FROM httppubsub_subscription_exacts WHERE url = ?", (url,)
            )
            cursor.execute(
                "DELETE FROM httppubsub_subscription_globs WHERE url = ?", (url,)
            )
            self.connection.commit()

            topics_batch: List[bytes] = []
            topics_iter = subscriptions.topics()
            saw_stop = False
            while True:
                if saw_stop or len(topics_batch) >= 100:
                    topics_batch.extend([url, url])  # type: ignore
                    cursor.execute(
                        "WITH batch(exact) AS (VALUES "
                        + ",".join(["(?)"] * (len(topics_batch) - 2))
                        + ") INSERT INTO httppubsub_subscription_exacts (url, exact) "
                        "SELECT ?, batch.exact FROM batch "
                        "WHERE"
                        " NOT EXISTS ("
                        "SELECT 1 FROM httppubsub_subscription_exacts AS hse "
                        "WHERE hse.url = ? AND hse.exact = batch.exact"
                        ")",
                        topics_batch,
                    )
                    self.connection.commit()
                    topics_batch.clear()

                    if saw_stop:
                        break

                try:
                    topics_batch.append(await topics_iter.__anext__())
                except StopAsyncIteration:
                    saw_stop = True
                    if not topics_batch:
                        break

            globs_batch: List[str] = []
            globs_iter = subscriptions.globs()
            saw_stop = False
            while True:
                if saw_stop or len(globs_batch) >= 100:
                    globs_batch.extend([url, url])
                    cursor.execute(
                        "WITH batch(glob) AS (VALUES "
                        + ",".join(["(?)"] * (len(globs_batch) - 2))
                        + ") INSERT INTO httppubsub_subscription_globs (url, glob) "
                        "SELECT ?, batch.glob FROM batch "
                        "WHERE"
                        " NOT EXISTS ("
                        "SELECT 1 FROM httppubsub_subscription_globs AS hsg "
                        "WHERE hsg.url = ? AND hsg.glob = batch.glob"
                        ")",
                        globs_batch,
                    )
                    self.connection.commit()
                    globs_batch.clear()

                    if saw_stop:
                        break

                try:
                    globs_batch.append(await globs_iter.__anext__())
                except StopAsyncIteration:
                    saw_stop = True
                    if not globs_batch:
                        break

            return "success"
        finally:
            cursor.close()


if TYPE_CHECKING:
    _: Type[DBConfig] = SqliteDBConfig
