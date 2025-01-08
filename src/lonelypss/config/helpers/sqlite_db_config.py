import math
import secrets
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
    GlobAndRecovery,
    StrongEtag,
    TopicAndRecovery,
    create_strong_etag_generator,
)
from lonelypsp.util.bounded_deque import BoundedDeque

from lonelypss.config.config import (
    LockedMissedInfo,
    MissedInfo,
    MutableMissedInfo,
    SubscriberInfo,
    SubscriberInfoExact,
    SubscriberInfoGlob,
    SubscriberInfoType,
)
from lonelypss.config.set_subscriptions_info import SetSubscriptionsInfo
from lonelypss.ws.handlers.open.errors import combine_multiple_exceptions

if TYPE_CHECKING:
    from lonelypss.config.config import DBConfig

import sys

if sys.version_info < (3, 10):
    from enum import Enum, auto
    from typing import Any, TypeVar, overload

    class _NotSet(Enum):
        NOT_SET = auto()

    T = TypeVar("T")
    D = TypeVar("D")

    @overload
    async def anext(iterable: AsyncIterator[T], /) -> T: ...

    @overload
    async def anext(iterable: AsyncIterator[T], default: D, /) -> Union[T, D]: ...

    async def anext(
        iterable: AsyncIterator[T],
        default: Any = _NotSet.NOT_SET,
        /,
    ) -> Any:
        try:
            return await iterable.__anext__()
        except StopAsyncIteration:
            if default is _NotSet.NOT_SET:
                raise
            return default


class SqliteLockedMissedInfo:
    """Implementation of LockedMissedInfo for Sqlite"""

    def __init__(
        self, parent: "SqliteDBConfig", info: MissedInfo, lock_id: int
    ) -> None:
        self.parent = parent
        self.info = info
        self.lock_id = lock_id
        self.released = False

    async def release(
        self, /, *, new_info: Optional[MutableMissedInfo] = None
    ) -> Literal["conflict", "unavailable", "ok"]:
        if self.released:
            return "conflict"

        assert self.parent.cursor is not None, "db not setup"

        if self.info.subscriber_info.type == SubscriberInfoType.EXACT:
            if new_info is None:
                return self._release_exact_with_delete(
                    self.parent.cursor,
                    self.info.subscriber_info,
                )
            return self._release_exact_with_update(
                self.parent.cursor,
                self.info.subscriber_info,
                new_info,
            )

        if new_info is None:
            return self._release_glob_with_delete(
                self.parent.cursor,
                self.info.subscriber_info,
            )

        return self._release_glob_with_update(
            self.parent.cursor,
            self.info.subscriber_info,
            new_info,
        )

    def _release_exact_with_delete(
        self,
        cursor: sqlite3.Cursor,
        subscriber_info: SubscriberInfoExact,
    ) -> Literal["conflict", "unavailable", "ok"]:
        try:
            cursor.execute("BEGIN IMMEDIATE TRANSACTION")
        except sqlite3.Error:
            return "unavailable"

        cursor.execute(
            "DELETE FROM httppubsub_subscription_exacts "
            "WHERE url=? AND exact=? AND missed_lock_id=?",
            (subscriber_info.url, self.info.topic, self.lock_id),
        )
        deleted = cursor.rowcount > 0
        cursor.execute("COMMIT")

        self.released = True
        return "ok" if deleted else "conflict"

    def _release_exact_with_update(
        self,
        cursor: sqlite3.Cursor,
        subscriber_info: SubscriberInfoExact,
        new_info: MutableMissedInfo,
    ) -> Literal["conflict", "unavailable", "ok"]:
        try:
            cursor.execute("BEGIN IMMEDIATE TRANSACTION")
        except sqlite3.Error:
            return "unavailable"

        cursor.execute(
            "UPDATE httppubsub_subscription_exacts "
            "SET missed_attempts=?, missed_next_retry_at=?, missed_lock_id=NULL, missed_lock_expires_at=NULL "
            "WHERE url=? AND exact=? AND missed_lock_id=?",
            (
                new_info.attempts,
                math.ceil(new_info.next_retry_at),
                subscriber_info.url,
                self.info.topic,
                self.lock_id,
            ),
        )
        updated = cursor.rowcount > 0
        cursor.execute("COMMIT")

        self.released = True
        return "ok" if updated else "conflict"

    def _release_glob_with_delete(
        self,
        cursor: sqlite3.Cursor,
        subscriber_info: SubscriberInfoGlob,
    ) -> Literal["conflict", "unavailable", "ok"]:
        try:
            cursor.execute("BEGIN IMMEDIATE TRANSACTION")
        except sqlite3.Error:
            return "unavailable"

        cursor.execute(
            "DELETE FROM httppubsub_subscription_globs "
            "WHERE url=? AND glob=? AND missed_lock_id=?",
            (subscriber_info.url, subscriber_info.glob, self.lock_id),
        )
        deleted = cursor.rowcount > 0
        cursor.execute("COMMIT")

        self.released = True
        return "ok" if deleted else "conflict"

    def _release_glob_with_update(
        self,
        cursor: sqlite3.Cursor,
        subscriber_info: SubscriberInfoGlob,
        new_info: MutableMissedInfo,
    ) -> Literal["conflict", "unavailable", "ok"]:
        try:
            cursor.execute("BEGIN IMMEDIATE TRANSACTION")
        except sqlite3.Error:
            return "unavailable"

        cursor.execute(
            "UPDATE httppubsub_subscription_globs "
            "SET missed_attempts=?, missed_next_retry_at=?, missed_lock_id=NULL, missed_lock_expires_at=NULL "
            "WHERE url=? AND glob=? AND missed_lock_id=?",
            (
                new_info.attempts,
                math.ceil(new_info.next_retry_at),
                subscriber_info.url,
                subscriber_info.glob,
                self.lock_id,
            ),
        )
        updated = cursor.rowcount > 0
        cursor.execute("COMMIT")

        self.released = True
        return "ok" if updated else "conflict"


class SqliteDBConfig:
    """Implements the DBConfig protocol using a locally hosted SQLite database."""

    def __init__(self, database: str, missed_lock_time: int = 30) -> None:
        self.database = database
        """The database url. You can pass `:memory:` to create a SQLite database that 
        exists only in memory, otherwise, this is typically the path to a sqlite file
        (usually has the `db` extension).
        """

        self.missed_lock_time = missed_lock_time
        """The number of seconds a missed lock should be held for before it is
        considered expired and can be stolen by other processes or coroutines,
        which could lead to duplicate MISSED messages but will generally not
        be harmful
        """

        self.connection: Optional[sqlite3.Connection] = None
        """If we've been setup, the current connection to the database."""

        self.cursor: Optional[sqlite3.Cursor] = None
        """If we've been setup, the current cursor to the database."""

    async def setup_db(self) -> None:
        assert self.connection is None, "sqlite db config is not re-entrant"
        connection = sqlite3.connect(self.database, isolation_level=None)
        cursor = connection.cursor()

        try:
            cursor.execute("BEGIN IMMEDIATE TRANSACTION")
            cursor.execute(
                """
    CREATE TABLE IF NOT EXISTS httppubsub_subscription_exacts (
        id INTEGER PRIMARY KEY,
        url TEXT NOT NULL,
        exact BLOB NOT NULL,
        missed_url TEXT NULL,
        missed_lock_id INTEGER NULL,
        missed_lock_expires_at INTEGER NULL,
        missed_attempts INTEGER NULL,
        missed_next_retry_at INTEGER NULL
    )
                """
            )
            cursor.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_httppubsub_subscription_exacts_exact_url ON httppubsub_subscription_exacts (exact, url)"
            )
            # get_overdue_missed_with_lock
            cursor.execute(
                """
    CREATE INDEX IF NOT EXISTS idx_httppubsub_subscription_exacts_next_retry_at 
    ON httppubsub_subscription_exacts (missed_next_retry_at) 
    WHERE missed_next_retry_at IS NOT NULL
                """
            )
            # delete_missed
            cursor.execute(
                """
    CREATE INDEX IF NOT EXISTS idx_httppubsub_subscription_exacts_missed_url_topic
    ON httppubsub_subscription_exacts (missed_url, exact) WHERE missed_url IS NOT NULL
                """
            )

            cursor.execute(
                """
    CREATE TABLE IF NOT EXISTS httppubsub_subscription_globs (
        id INTEGER PRIMARY KEY,
        url TEXT NOT NULL,
        glob TEXT NOT NULL,
        missed_url TEXT NULL,
        missed_lock_id INTEGER NULL,
        missed_lock_expires_at INTEGER NULL,
        missed_topic BLOB NULL,
        missed_attempts INTEGER NULL,
        missed_next_retry_at INTEGER NULL
    )
                """
            )
            cursor.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_httppubsub_subscription_globs_glob_url ON httppubsub_subscription_globs (glob, url)"
            )
            # get_overdue_missed_with_lock
            cursor.execute(
                """
    CREATE INDEX IF NOT EXISTS idx_httppubsub_subscription_globs_next_retry_at
    ON httppubsub_subscription_globs (missed_next_retry_at)
    WHERE missed_next_retry_at IS NOT NULL
                """
            )
            # delete_missed
            cursor.execute(
                """
    CREATE INDEX IF NOT EXISTS idx_httppubsub_subscription_globs_missed_url_topic
    ON httppubsub_subscription_globs (missed_url, missed_topic) WHERE missed_url IS NOT NULL
                """
            )
            cursor.execute("COMMIT")
        except BaseException as og_cause:
            related: List[BaseException] = []
            try:
                cursor.close()
            except BaseException as e:
                related.append(e)

            try:
                connection.close()
            except BaseException as e:
                related.append(e)

            if related:
                raise combine_multiple_exceptions(
                    "failed to initialize sqlite db, then failed to close",
                    [og_cause, *related],
                )
            raise

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
        self, /, *, url: str, recovery: Optional[str], exact: bytes
    ) -> Literal["success", "conflict", "unavailable"]:
        assert self.cursor is not None, "db not setup"
        try:
            self.cursor.execute("BEGIN IMMEDIATE TRANSACTION")
        except sqlite3.Error:
            return "unavailable"

        self.cursor.execute(
            """
INSERT INTO httppubsub_subscription_exacts (
    url, 
    exact, 
    missed_url,
    missed_lock_id,
    missed_lock_expires_at,
    missed_attempts, 
    missed_next_retry_at
)
SELECT ?, ?, ?, NULL, NULL, NULL, NULL
WHERE
    NOT EXISTS (
        SELECT 1 FROM httppubsub_subscription_exacts AS hse
        WHERE hse.url = ? AND hse.exact = ?
    )
            """,
            (url, exact, recovery, url, exact),
        )
        inserted = self.cursor.rowcount > 0
        self.cursor.execute("COMMIT")

        return "success" if inserted else "conflict"

    async def unsubscribe_exact(
        self, /, *, url: str, exact: bytes
    ) -> Literal["success", "not_found", "unavailable"]:
        assert self.cursor is not None, "db not setup"

        try:
            self.cursor.execute("BEGIN IMMEDIATE TRANSACTION")
        except sqlite3.Error:
            return "unavailable"

        self.cursor.execute(
            "DELETE FROM httppubsub_subscription_exacts WHERE url = ? AND exact = ?",
            (url, exact),
        )
        deleted = self.cursor.rowcount > 0
        self.cursor.execute("COMMIT")

        return "success" if deleted else "not_found"

    async def subscribe_glob(
        self, /, *, url: str, recovery: Optional[str], glob: str
    ) -> Literal["success", "conflict", "unavailable"]:
        assert self.cursor is not None, "db not setup"
        try:
            self.cursor.execute("BEGIN IMMEDIATE TRANSACTION")
        except sqlite3.Error:
            return "unavailable"

        self.cursor.execute(
            """
INSERT INTO httppubsub_subscription_globs (
    url,
    glob,
    missed_url,
    missed_lock_id,
    missed_lock_expires_at,
    missed_topic,
    missed_attempts,
    missed_next_retry_at
)
SELECT ?, ?, ?, NULL, NULL, NULL, NULL, NULL
WHERE
    NOT EXISTS (
        SELECT 1 FROM httppubsub_subscription_globs AS hsg
        WHERE hsg.url = ? AND hsg.glob = ?
    )
            """,
            (url, glob, recovery, url, glob),
        )
        inserted = self.cursor.rowcount > 0
        self.cursor.execute("COMMIT")

        return "success" if inserted else "conflict"

    async def unsubscribe_glob(
        self, /, *, url: str, glob: str
    ) -> Literal["success", "not_found", "unavailable"]:
        assert self.cursor is not None, "db not setup"
        try:
            self.cursor.execute("BEGIN IMMEDIATE TRANSACTION")
        except sqlite3.Error:
            return "unavailable"

        self.cursor.execute(
            "DELETE FROM httppubsub_subscription_globs WHERE url = ? AND glob = ?",
            (url, glob),
        )
        deleted = self.cursor.rowcount > 0
        self.cursor.execute("COMMIT")
        return "success" if deleted else "not_found"

    async def get_subscribers(
        self, /, *, topic: bytes
    ) -> AsyncIterable[SubscriberInfo]:
        assert self.connection is not None, "db not setup"
        cursor = self.connection.cursor()
        try:
            cursor.execute("BEGIN DEFERRED TRANSACTION")
            cursor.execute(
                "SELECT url, missed_url FROM httppubsub_subscription_exacts WHERE exact = ?",
                (topic,),
            )
            topic_rows = cast(List[Tuple[str, Optional[str]]], cursor.fetchall())
            cursor.execute("COMMIT")

            for url, recovery in topic_rows:
                yield SubscriberInfoExact(
                    type=SubscriberInfoType.EXACT, url=url, recovery=recovery
                )

            cursor.execute("BEGIN DEFERRED TRANSACTION")
            cursor.execute(
                "SELECT glob, url, missed_url FROM httppubsub_subscription_globs WHERE ? GLOB glob",
                (topic,),
            )
            glob_rows = cast(List[Tuple[str, str, Optional[str]]], cursor.fetchall())
            cursor.execute("COMMIT")
            for glob, url, recovery in glob_rows:
                yield SubscriberInfoGlob(
                    type=SubscriberInfoType.GLOB, glob=glob, url=url, recovery=recovery
                )
        finally:
            cursor.close()

    async def check_subscriptions(self, /, *, url: str) -> StrongEtag:
        assert self.connection is not None, "db not setup"

        topics_gen = create_strong_etag_generator(url)

        cursor = self.connection.cursor()
        try:
            last_topic: Optional[bytes] = None
            while True:
                cursor.execute("BEGIN DEFERRED TRANSACTION")
                cursor.execute(
                    "SELECT exact, missed_url "
                    "FROM httppubsub_subscription_exacts "
                    "WHERE url = ?"
                    " AND (? IS NULL OR exact > ?) "
                    "ORDER BY exact ASC "
                    "LIMIT 100",
                    (url, last_topic, last_topic),
                )

                topics_batch = cast(
                    List[Tuple[bytes, Optional[str]]], cursor.fetchall()
                )
                cursor.execute("COMMIT")
                if not topics_batch:
                    break

                topics_gen.add_topic(
                    *(TopicAndRecovery(*topic) for topic in topics_batch)
                )

                last_topic = topics_batch[-1][0]

            globs_gen = topics_gen.finish_topics()
            del topics_gen

            last_glob: Optional[str] = None
            while True:
                cursor.execute("BEGIN DEFERRED TRANSACTION")
                cursor.execute(
                    "SELECT glob, missed_url "
                    "FROM httppubsub_subscription_globs "
                    "WHERE url = ?"
                    " AND (? IS NULL OR glob > ?) "
                    "ORDER BY glob ASC "
                    "LIMIT 100",
                    (url, last_glob, last_glob),
                )

                globs_batch = cast(List[Tuple[str, Optional[str]]], cursor.fetchall())
                cursor.execute("COMMIT")
                if not globs_batch:
                    break

                globs_gen.add_glob(*(GlobAndRecovery(*glob) for glob in globs_batch))

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
        desired_topics_iter: AsyncIterator[TopicAndRecovery],
    ) -> None:
        actual_topics_iter = _ExistingTopicsIter(cursor, url)

        next_desired: Optional[TopicAndRecovery] = await anext(
            desired_topics_iter, None
        )
        next_actual: Optional[bytes] = next(actual_topics_iter, None)

        while next_desired is not None or next_actual is not None:
            if next_desired is None:
                cursor.execute("BEGIN IMMEDIATE TRANSACTION")
                cursor.execute(
                    "DELETE FROM httppubsub_subscription_exacts "
                    "WHERE url = ? AND exact >= ?",
                    (url, next_actual),
                )
                cursor.execute("COMMIT")
                next_actual = None
            elif next_actual is None or next_desired.topic < next_actual:
                await self.subscribe_exact(
                    url=url, recovery=next_desired.recovery, exact=next_desired.topic
                )
                next_desired = await anext(desired_topics_iter, None)
            elif next_desired.topic > next_actual:
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
        desired_globs_iter: AsyncIterator[GlobAndRecovery],
    ) -> None:
        actual_globs_iter = _ExistingGlobsIter(cursor, url)

        next_desired: Optional[GlobAndRecovery] = await anext(desired_globs_iter, None)
        next_actual: Optional[str] = next(actual_globs_iter, None)

        while next_desired is not None or next_actual is not None:
            if next_desired is None:
                cursor.execute("BEGIN IMMEDIATE TRANSACTION")
                cursor.execute(
                    "DELETE FROM httppubsub_subscription_globs "
                    "WHERE url = ? AND glob >= ?",
                    (url, next_actual),
                )
                cursor.execute("COMMIT")
                next_actual = None
            elif next_actual is None or next_desired.glob < next_actual:
                await self.subscribe_glob(
                    url=url, recovery=next_desired.recovery, glob=next_desired.glob
                )
                next_desired = await anext(desired_globs_iter, None)
            elif next_desired.glob > next_actual:
                await self.unsubscribe_glob(url=url, glob=next_actual)
                next_actual = next(actual_globs_iter, None)
            else:
                next_desired = await anext(desired_globs_iter, None)
                next_actual = next(actual_globs_iter, None)

    async def upsert_missed(
        self, /, *, info: MissedInfo
    ) -> Literal["success", "unavailable"]:
        assert self.cursor is not None and self.connection is not None, "db not setup"

        cursor = self.cursor

        if info.subscriber_info.type == SubscriberInfoType.EXACT:
            try:
                cursor.execute("BEGIN IMMEDIATE TRANSACTION")
            except sqlite3.Error:
                return "unavailable"

            cursor.execute(
                """
UPDATE httppubsub_subscription_exacts
SET
    missed_url = ?,
    missed_attempts = ?,
    missed_next_retry_at = ?,
    missed_lock_id = NULL,
    missed_lock_expires_at = NULL
WHERE
    url = ? AND exact = ?
                """,
                (
                    info.subscriber_info.recovery,
                    info.attempts,
                    math.ceil(info.next_retry_at),
                    info.subscriber_info.url,
                    info.topic,
                ),
            )
            cursor.execute("COMMIT")
            return "success"

        try:
            cursor.execute("BEGIN IMMEDIATE TRANSACTION")
        except sqlite3.Error:
            return "unavailable"
        cursor.execute(
            """
UPDATE httppubsub_subscription_globs
SET
    missed_url = ?,
    missed_topic = ?,
    missed_attempts = ?,
    missed_next_retry_at = ?,
    missed_lock_id = NULL,
    missed_lock_expires_at = NULL
WHERE
    url = ? AND glob = ?
            """,
            (
                info.subscriber_info.recovery,
                info.topic,
                info.attempts,
                math.ceil(info.next_retry_at),
                info.subscriber_info.url,
                info.subscriber_info.glob,
            ),
        )
        cursor.execute("COMMIT")
        return "success"

    def _make_missed_id(self) -> int:
        return int.from_bytes(secrets.token_bytes(8), "big", signed=True)

    async def get_overdue_missed_with_lock(
        self, /, *, now: float
    ) -> AsyncIterable[LockedMissedInfo]:
        assert self.connection is not None, "db not setup"

        cursor = self.connection.cursor()
        try:
            last_next_retry_at: Optional[float] = None
            while True:
                # most of the time this will need a write lock, and it's more obvious
                # what the possible implications are if we error SQLITE_BUSY here rather
                # than later
                cursor.execute("BEGIN IMMEDIATE TRANSACTION")
                cursor.execute(
                    """
SELECT id, url, exact, missed_url, missed_attempts, missed_next_retry_at
FROM httppubsub_subscription_exacts
WHERE
    missed_next_retry_at IS NOT NULL
    AND missed_next_retry_at <= ?
    AND (? IS NULL OR missed_next_retry_at > ?)
    AND (missed_lock_expires_at IS NULL OR missed_lock_expires_at < ?)
ORDER BY missed_next_retry_at ASC
LIMIT 1
                    """,
                    (now, last_next_retry_at, last_next_retry_at, now),
                )
                exact_row = cast(
                    Optional[Tuple[int, str, bytes, str, int, int]], cursor.fetchone()
                )
                if exact_row is None:
                    cursor.execute("COMMIT")
                    break

                row_id, url, exact, recovery, attempts, next_retry_at = exact_row
                lock_id = self._make_missed_id()
                cursor.execute(
                    "UPDATE httppubsub_subscription_exacts "
                    "SET"
                    " missed_lock_id = ?,"
                    " missed_lock_expires_at = ? "
                    "WHERE id = ?",
                    (lock_id, now + self.missed_lock_time, row_id),
                )
                if cursor.rowcount != 1:
                    cursor.execute("ROLLBACK")
                    raise RuntimeError("failed to lock row despite held transaction")
                cursor.execute("COMMIT")
                yield SqliteLockedMissedInfo(
                    parent=self,
                    info=MissedInfo(
                        topic=exact,
                        attempts=attempts,
                        next_retry_at=next_retry_at,
                        subscriber_info=SubscriberInfoExact(
                            type=SubscriberInfoType.EXACT, url=url, recovery=recovery
                        ),
                    ),
                    lock_id=lock_id,
                )

            last_next_retry_at = None
            while True:
                cursor.execute("BEGIN IMMEDIATE TRANSACTION")
                cursor.execute(
                    """
SELECT id, url, glob, missed_url, missed_topic, missed_attempts, missed_next_retry_at
FROM httppubsub_subscription_globs
WHERE
    missed_next_retry_at IS NOT NULL
    AND missed_next_retry_at <= ?
    AND (? IS NULL OR missed_next_retry_at > ?)
    AND (missed_lock_expires_at IS NULL OR missed_lock_expires_at < ?)
ORDER BY missed_next_retry_at ASC
LIMIT 1
                    """,
                    (now, last_next_retry_at, last_next_retry_at, now),
                )
                glob_row = cast(
                    Optional[Tuple[int, str, str, str, bytes, int, int]],
                    cursor.fetchone(),
                )
                if glob_row is None:
                    cursor.execute("COMMIT")
                    break

                row_id, url, glob, recovery, topic, attempts, next_retry_at = glob_row
                lock_id = self._make_missed_id()
                cursor.execute(
                    "UPDATE httppubsub_subscription_globs "
                    "SET"
                    " missed_lock_id = ?,"
                    " missed_lock_expires_at = ? "
                    "WHERE id = ?",
                    (lock_id, now + self.missed_lock_time, row_id),
                )
                if cursor.rowcount != 1:
                    cursor.execute("ROLLBACK")
                    raise RuntimeError("failed to lock row despite held transaction")
                cursor.execute("COMMIT")
                yield SqliteLockedMissedInfo(
                    parent=self,
                    info=MissedInfo(
                        topic=topic,
                        attempts=attempts,
                        next_retry_at=next_retry_at,
                        subscriber_info=SubscriberInfoGlob(
                            type=SubscriberInfoType.GLOB,
                            glob=glob,
                            url=url,
                            recovery=recovery,
                        ),
                    ),
                    lock_id=lock_id,
                )

        finally:
            cursor.close()


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
    _a: Type[LockedMissedInfo] = SqliteLockedMissedInfo
    _b: Type[DBConfig] = SqliteDBConfig
    _c: Type[Iterable[bytes]] = _ExistingTopicsIter
    _d: Type[Iterable[str]] = _ExistingGlobsIter
