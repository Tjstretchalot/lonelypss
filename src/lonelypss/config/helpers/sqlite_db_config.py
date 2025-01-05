import sqlite3
from typing import (
    TYPE_CHECKING,
    AsyncIterable,
    AsyncIterator,
    Iterable,
    List,
    Literal,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

from lonelypsp.stateless.make_strong_etag import (
    StrongEtag,
    create_strong_etag_generator,
)
from lonelypsp.util.bounded_deque import BoundedDeque

from lonelypss.config.config import (
    SubscriberInfo,
    SubscriberInfoExact,
    SubscriberInfoGlob,
)
from lonelypss.config.set_subscriptions_info import SetSubscriptionsInfo

if TYPE_CHECKING:
    from lonelypss.config.config import DBConfig

import sys

if sys.version_info < (3, 10):
    from enum import Enum, auto
    from typing import TypeVar, overload

    class _NotSet(Enum):
        NOT_SET = auto()

    T = TypeVar("T")
    D = TypeVar("D")

    @overload
    async def anext(iterable: AsyncIterable[T]) -> T: ...

    @overload
    async def anext(iterable: AsyncIterable[T], default: D, /) -> Union[T, D]: ...

    async def anext(
        iterable: AsyncIterable[T], default: Optional[D] = _NotSet.NOT_SET, /
    ) -> Union[T, D]:
        try:
            return await iterable.__anext__()
        except StopAsyncIteration:
            if default is _NotSet.NOT_SET:
                raise
            return cast(D, default)


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
            await self._set_topics(
                cursor=cursor, url=url, desired_topics_iter=subscriptions.topics()
            )
            await self._set_globs(
                cursor=cursor, url=url, desired_globs_iter=subscriptions.globs()
            )
            return "success"
        finally:
            cursor.close()

    async def _set_topics(
        self,
        /,
        *,
        cursor: sqlite3.Cursor,
        url: str,
        desired_topics_iter: AsyncIterator[bytes],
    ) -> None:
        actual_topics_iter = _ExistingTopicsIter(cursor, url)

        next_desired: Optional[bytes] = await anext(desired_topics_iter, None)
        next_actual: Optional[bytes] = next(actual_topics_iter, None)

        while next_desired is not None or next_actual is not None:
            if next_desired is None:
                cursor.execute(
                    "DELETE FROM httppubsub_subscription_exacts "
                    "WHERE url = ? AND exact >= ?",
                    (url, next_actual),
                )
                next_actual = None
            elif next_actual is None:
                await self.subscribe_exact(url=url, exact=next_desired)
                next_desired = await anext(desired_topics_iter, None)
            elif next_desired < next_actual:
                await self.subscribe_exact(url=url, exact=next_desired)
                next_desired = await anext(desired_topics_iter, None)
            elif next_desired > next_actual:
                await self.unsubscribe_exact(url=url, exact=next_actual)
                next_actual = next(actual_topics_iter, None)
            else:
                next_desired = await anext(desired_topics_iter, None)
                next_actual = next(actual_topics_iter, None)

    async def _set_globs(
        self,
        /,
        *,
        cursor: sqlite3.Cursor,
        url: str,
        desired_globs_iter: AsyncIterator[str],
    ) -> None:
        actual_globs_iter = _ExistingGlobsIter(cursor, url)

        next_desired: Optional[str] = await anext(desired_globs_iter, None)
        next_actual: Optional[str] = next(actual_globs_iter, None)

        while next_desired is not None or next_actual is not None:
            if next_desired is None:
                cursor.execute(
                    "DELETE FROM httppubsub_subscription_globs "
                    "WHERE url = ? AND glob >= ?",
                    (url, next_actual),
                )
                next_actual = None
            elif next_actual is None:
                await self.subscribe_glob(url=url, glob=next_desired)
                next_desired = await anext(desired_globs_iter, None)
            elif next_desired < next_actual:
                await self.subscribe_glob(url=url, glob=next_desired)
                next_desired = await anext(desired_globs_iter, None)
            elif next_desired > next_actual:
                await self.unsubscribe_glob(url=url, glob=next_actual)
                next_actual = next(actual_globs_iter, None)
            else:
                next_desired = await anext(desired_globs_iter, None)
                next_actual = next(actual_globs_iter, None)


class _ExistingTopicsIter:
    def __init__(self, cursor: sqlite3.Cursor, url: str, batch_size: int = 16) -> None:
        self.cursor = cursor
        self.url = url
        self.batch_size = batch_size

        self.batch_remaining: BoundedDeque[bytes] = BoundedDeque(maxlen=batch_size)
        self.last_topic: Optional[bytes] = None
        self.at_last_batch: bool = False

    def __iter__(self) -> "_ExistingTopicsIter":
        return self

    def __next__(self) -> bytes:
        if not self.batch_remaining and not self.at_last_batch:
            self._get_next_batch()

        if not self.batch_remaining:
            raise StopIteration

        return self.batch_remaining.popleft()

    def _get_next_batch(self) -> None:
        self.cursor.execute(
            "SELECT exact "
            "FROM httppubsub_subscription_exacts "
            "WHERE url = ?"
            " AND (? IS NULL OR exact > ?) "
            "ORDER BY exact ASC "
            "LIMIT ?",
            (self.url, self.last_topic, self.last_topic, self.batch_size),
        )
        raw_batch: List[Tuple[bytes]] = self.cursor.fetchall()
        self.batch_remaining.ensure_space_for(len(raw_batch))
        for row in raw_batch:
            self.batch_remaining.append(row[0])
        self.at_last_batch = len(raw_batch) < self.batch_size


class _ExistingGlobsIter:
    def __init__(self, cursor: sqlite3.Cursor, url: str, batch_size: int = 16) -> None:
        self.cursor = cursor
        self.url = url
        self.batch_size = batch_size

        self.batch_remaining: BoundedDeque[str] = BoundedDeque(maxlen=batch_size)
        self.last_glob: Optional[str] = None
        self.at_last_batch: bool = False

    def __iter__(self) -> "_ExistingGlobsIter":
        return self

    def __next__(self) -> str:
        if not self.batch_remaining and not self.at_last_batch:
            self._get_next_batch()

        if not self.batch_remaining:
            raise StopIteration

        return self.batch_remaining.popleft()

    def _get_next_batch(self) -> None:
        self.cursor.execute(
            "SELECT glob "
            "FROM httppubsub_subscription_globs "
            "WHERE url = ?"
            " AND (? IS NULL OR glob > ?) "
            "ORDER BY glob ASC "
            "LIMIT ?",
            (self.url, self.last_glob, self.last_glob, self.batch_size),
        )
        raw_batch: List[Tuple[str]] = self.cursor.fetchall()
        self.batch_remaining.ensure_space_for(len(raw_batch))
        for row in raw_batch:
            self.batch_remaining.append(row[0])
        self.at_last_batch = len(raw_batch) < self.batch_size


if TYPE_CHECKING:
    _: Type[DBConfig] = SqliteDBConfig
    __: Type[Iterable[bytes]] = _ExistingTopicsIter
    ___: Type[Iterable[str]] = _ExistingGlobsIter
