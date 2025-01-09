import asyncio
import base64
import hmac
import math
import secrets
import sqlite3
import time
from enum import Enum, IntEnum, auto
from typing import TYPE_CHECKING, Literal, Optional, Protocol, Type, Union, cast

from lonelypsp.compat import fast_dataclass
from lonelypsp.stateful.messages.configure import S2B_Configure
from lonelypsp.stateless.make_strong_etag import StrongEtag

from lonelypss.config.set_subscriptions_info import SetSubscriptionsInfo

if TYPE_CHECKING:
    from lonelypss.config.auth_config import (
        ToBroadcasterAuthConfig,
        ToSubscriberAuthConfig,
    )


class IncomingHmacAuthDBConfig(Protocol):
    """Describes how to verify that incoming hash-based message authentication
    codes are not being reused.
    """

    async def setup_hmac_auth_db(self) -> None: ...
    async def teardown_hmac_auth_db(self) -> None: ...

    async def mark_code_used(self, /, *, code: bytes) -> Literal["conflict", "ok"]: ...


class IncomingHmacAuthDBReentrantConfig:
    """Delegates to another implementation without forwarding all but the
    outermost setup/teardown, making it reentrant
    """

    def __init__(self, delegate: IncomingHmacAuthDBConfig) -> None:
        self.delegate = delegate
        self.depth = 0
        self.lock = asyncio.Lock()

    async def setup_hmac_auth_db(self) -> None:
        async with self.lock:
            if self.depth == 0:
                await self.delegate.setup_hmac_auth_db()
            self.depth += 1

    async def teardown_hmac_auth_db(self) -> None:
        async with self.lock:
            if self.depth <= 0:
                return

            self.depth -= 1
            if self.depth == 0:
                await self.delegate.teardown_hmac_auth_db()

    async def mark_code_used(self, /, *, code: bytes) -> Literal["conflict", "ok"]:
        return await self.delegate.mark_code_used(code=code)


class IncomingHmacAuthNoneDBConfig:
    """Does not store recent tokens and thus cannot check if they've been recently
    used. Technically, this is vulnerable to replay attacks, though the scope is
    rather limited.
    """

    async def setup_hmac_auth_db(self) -> None: ...
    async def teardown_hmac_auth_db(self) -> None: ...

    async def mark_code_used(self, /, *, code: bytes) -> Literal["conflict", "ok"]:
        return "ok"


if TYPE_CHECKING:
    _: Type[IncomingHmacAuthDBConfig] = IncomingHmacAuthNoneDBConfig


class IncomingHmacAuthSqliteDBConfig:
    """Stores recent tokens in a sqlite database, cleaning them in the background
    occassionally. This is only effective if there is only one broadcaster, though
    it will help detect replay attacks even if there are multiple broadcasters.
    """

    def __init__(
        self,
        database: str,
        *,
        token_lifetime: int = 180,
        cleanup_batch_delay: float = 10.0,
    ) -> None:
        self.database = database
        """The database url. You can pass `:memory:` to create a SQLite database that
        exists only in memory, otherwise, this is typically the path to a sqlite file
        (usually has the `db` extension).
        """

        self.token_lifetime: int = token_lifetime
        """The minimum time in seconds before we forget about a token, must be at
        least as long as the token is accepted for this to be effective at preventing
        replay attacks.
        """

        self.cleanup_batch_delay: float = cleanup_batch_delay
        """The minimum time in seconds between cleaning up tokens that have expired"""

        self.conn: Optional[sqlite3.Connection] = None
        """The connection to the database"""

        self.cursor: Optional[sqlite3.Cursor] = None
        """The cursor that can be used iff you will be done using it before yielding
        to the event loop
        """

        self.background_task: Optional[asyncio.Task[None]] = None
        """The cleanup task that runs in the background"""

        self.cleanup_wakeup: asyncio.Event = asyncio.Event()
        """An event that is set whenever a token is created and will be waited on
        when there are no tokens in the store
        """

    async def setup_hmac_auth_db(self) -> None:
        assert self.background_task is None, "already entered, not re-entrant"
        conn = sqlite3.connect(self.database, isolation_level=None)
        try:
            cursor = conn.cursor()
            try:
                cursor.execute("BEGIN IMMEDIATE TRANSACTION")
                cursor.execute(
                    "CREATE TABLE IF NOT EXISTS httppubsub_hmacs (code BLOB PRIMARY KEY, expires_at INTEGER NOT NULL) WITHOUT ROWID"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_httppubsub_hmacs_expires_at ON httppubsub_hmacs (expires_at)"
                )
                cursor.execute("COMMIT")
                self.conn = conn
                self.cursor = cursor
                self.background_task = asyncio.create_task(self._cleanup_codes())
            except BaseException:
                cursor.close()
                raise
        except BaseException:
            conn.close()
            raise

    async def teardown_hmac_auth_db(self) -> None:
        task = self.background_task
        conn = self.conn
        cursor = self.cursor
        self.background_task = None
        self.conn = None
        self.cursor = None
        try:
            if task is not None:
                task.cancel()
                await asyncio.wait([task])
        finally:
            try:
                if cursor is not None:
                    cursor.close()
            finally:
                if conn is not None:
                    conn.close()

    async def mark_code_used(self, /, *, code: bytes) -> Literal["conflict", "ok"]:
        assert self.conn is not None and self.cursor is not None, "not entered"
        self.cursor.execute("BEGIN IMMEDIATE TRANSACTION")
        try:
            self.cursor.execute(
                "INSERT INTO httppubsub_hmacs (code, expires_at) "
                "SELECT ?, ? "
                "WHERE"
                " NOT EXISTS ("
                "SELECT 1 FROM httppubsub_hmacs WHERE code = ?"
                ")",
                (code, math.ceil(time.time() + self.token_lifetime), code),
            )
            is_conflict = self.cursor.rowcount == 0
            self.cursor.execute("COMMIT")
        except BaseException:
            self.cursor.execute("ROLLBACK")
            raise
        if not is_conflict:
            self.cleanup_wakeup.set()
        return "conflict" if is_conflict else "ok"

    async def _cleanup_codes(self) -> None:
        assert self.conn is not None and self.cursor is not None, "not entered"
        cursor = self.cursor

        while True:
            now = time.time()
            cursor.execute("BEGIN IMMEDIATE TRANSACTION")
            try:
                cursor.execute(
                    "DELETE FROM httppubsub_hmac_auth_tokens WHERE expires_at < ?",
                    (math.floor(now),),
                )
                cursor.execute(
                    "SELECT expires_at FROM httppubsub_hmac_auth_tokens ORDER BY expires_at ASC LIMIT 1"
                )
                next_expires_at = cast(Optional[int], cursor.fetchone())
                cursor.execute("COMMIT")
            except BaseException:
                cursor.execute("ROLLBACK")
                raise

            if next_expires_at is None:
                self.cleanup_wakeup.clear()
                await self.cleanup_wakeup.wait()
                await asyncio.sleep(self.token_lifetime + self.cleanup_batch_delay)
                continue

            await asyncio.sleep(max(next_expires_at - now, self.cleanup_batch_delay))


class TokenInfoType(Enum):
    """What we can discover when extracting token information"""

    UNAUTHORIZED = auto()
    """the information was missing"""
    FORBIDDEN = auto()
    """the information was malformed or definitely not acceptable"""
    FOUND = auto()
    """the information was found but not checked"""


@fast_dataclass
class TokenInfoUnauthorized:
    type: Literal[TokenInfoType.UNAUTHORIZED]


@fast_dataclass
class TokenInfoForbidden:
    type: Literal[TokenInfoType.FORBIDDEN]


@fast_dataclass
class TokenInfoFound:
    type: Literal[TokenInfoType.FOUND]
    timestamp: int
    """approximately when the token was created"""
    nonce: str
    """a unique value to avoid ever resigning the exact same data"""
    hmac: bytes
    """the provided hash-based message authentication code"""


def get_token(
    authorization: Optional[str], /, *, now: float, token_lifetime: float
) -> Union[TokenInfoUnauthorized, TokenInfoForbidden, TokenInfoFound]:
    """Extracts the hash-based message authentication code from the authorization
    header and performs sanity checks that don't depend on the data being
    signed or the database
    """
    if authorization is None:
        return TokenInfoUnauthorized(type=TokenInfoType.UNAUTHORIZED)

    if not authorization.startswith("X-HMAC "):
        return TokenInfoForbidden(type=TokenInfoType.FORBIDDEN)

    timestamp_nonce_and_token = authorization[len("X-HMAC ") :]
    sep_index = timestamp_nonce_and_token.find(":")
    if sep_index == -1:
        return TokenInfoForbidden(type=TokenInfoType.FORBIDDEN)

    timestamp_str = timestamp_nonce_and_token[:sep_index]
    try:
        timestamp = int(timestamp_str)
    except ValueError:
        return TokenInfoForbidden(type=TokenInfoType.FORBIDDEN)

    # clock drift means the time could be in the future
    if abs(now - timestamp) > token_lifetime:
        return TokenInfoForbidden(type=TokenInfoType.FORBIDDEN)

    nonce_and_token = timestamp_nonce_and_token[sep_index + 1 :]
    sep_index = nonce_and_token.find(":")
    if sep_index == -1:
        return TokenInfoForbidden(type=TokenInfoType.FORBIDDEN)

    nonce = nonce_and_token[:sep_index]

    code_str = nonce_and_token[sep_index + 1 :]
    try:
        code = base64.b64decode(code_str + "==")
    except ValueError:
        return TokenInfoForbidden(type=TokenInfoType.FORBIDDEN)

    if len(code) != 64:
        return TokenInfoForbidden(type=TokenInfoType.FORBIDDEN)

    return TokenInfoFound(
        type=TokenInfoType.FOUND, timestamp=timestamp, nonce=nonce, hmac=code
    )


def make_nonce() -> str:
    """standard way to generate a nonce for HMAC tokens"""
    return secrets.token_urlsafe(4)


async def check_code(
    *, secret: bytes, to_sign: bytes, code: bytes, db: IncomingHmacAuthDBConfig
) -> Literal["ok", "forbidden"]:
    """Checks if the provided hash-based message authentication code matches
    whats expected given it should have signed `to_sign` with the given
    secret and should not be a replay of a recent token (via db)
    """
    expected_hmac = hmac.new(secret, to_sign, "sha512").digest()
    if not hmac.compare_digest(code, expected_hmac):
        return "forbidden"

    if await db.mark_code_used(code=code) == "conflict":
        return "forbidden"

    return "ok"


def sign(*, secret: bytes, to_sign: bytes, nonce: str, now: float) -> str:
    """Generates the hash-based message authentication code for the given data then
    formats it in the standard way for transfer
    """
    hmac_token = hmac.new(secret, to_sign, "sha512").digest()
    return f"X-HMAC {int(now)}:{nonce}:{base64.b64encode(hmac_token).decode('ascii')}"


class AuthMessageType(IntEnum):
    """In order to protect against a man in the middle which convinces
    a trusted party to sign a token with arbitrary data that makes it look
    like a different type of message, all messages to sign are prefixed with
    an int specifying how they are intending to be parsed so that confusion
    will always result in a wrong digest

    Example of this attack:

    suppose there are two messages, one which is:
    - 2 bytes: length of url (N)
    - N bytes: url
    - 2 bytes: length of topic (M)
    - M bytes: topic

    and another message which is just
    - 2 bytes: length of url (N)
    - N bytes: url

    if the attacker can convince the subscriber to sign the second message
    where the url is specifically crafted to look like a url prefix followed
    by 2 utf-8 bytes that look like the length of the remaining topic, then
    a valid topic, then the attacker can use that signature and replace the body
    to get a different result
    """

    SUBSCRIBE_EXACT = auto()
    SUBSCRIBE_GLOB = auto()
    NOTIFY = auto()
    WEBSOCKET_CONFIGURE = auto()
    CHECK_SUBSCRIPTIONS = auto()
    SET_SUBSCRIPTIONS = auto()
    RECEIVE = auto()
    MISSED = auto()
    WEBSOCKET_CONFIRM_CONFIGURE = auto()


class ToBroadcasterHmacAuth:
    """Sets up and allows requests to the broadcaster which include hash-based
    message authentication codes. These tokens can only be generated if the
    sender knows the shared secret.

    The authorization header is formatted as follows:
    `X-HMAC <timestamp>:<nonce>:<token>`, where timestamp is integer seconds
    from the epoch, the nonce is to ensure uniqueness, and the token is the HMAC
    of the relevant information signed with the shared secret.

    This is a secure way to verify requests even if the underlying message
    and headers are not encrypted, as the shared secret cannot be discovered
    by an attacker who can only see the messages. However, it will not do
    anything to prevent the contents of the messages from being read

    Generally, this is an appropriate defense-in-depth measure to use in
    internal networks where you cannot setup TLS. It is also effective when TLS
    is available, though token authorization will require less CPU time for both
    broadcasters and subscribers and is secure if headers are encrypted.
    """

    def __init__(
        self,
        *,
        secret: str,
        token_lifetime: float = 120,
        db_config: IncomingHmacAuthDBConfig,
    ) -> None:
        self.secret = base64.urlsafe_b64decode(secret + "==")
        """The shared secret used to generate the HMAC tokens"""
        assert len(self.secret) == 64, "subscriber_secret must be 64 bytes long"
        self.token_lifetime = token_lifetime
        """How long after a token is created that we still accept it"""
        self.db_config = db_config
        """The configuration for the recent codes database"""

    async def setup_to_broadcaster_auth(self) -> None:
        await self.db_config.setup_hmac_auth_db()

    async def teardown_to_broadcaster_auth(self) -> None:
        await self.db_config.teardown_hmac_auth_db()

    async def is_subscribe_exact_allowed(
        self,
        /,
        *,
        url: str,
        recovery: Optional[str],
        exact: bytes,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        token = get_token(authorization, now=now, token_lifetime=self.token_lifetime)
        if token.type == TokenInfoType.UNAUTHORIZED:
            return "unauthorized"
        if token.type == TokenInfoType.FORBIDDEN:
            return "forbidden"

        encoded_url = url.encode("utf-8")
        encoded_recovery = b"" if recovery is None else recovery.encode("utf-8")
        encoded_timestamp = token.timestamp.to_bytes(8, "big")
        encoded_nonce = token.nonce.encode("utf-8")

        to_sign = b"".join(
            [
                int(AuthMessageType.SUBSCRIBE_EXACT).to_bytes(1, "big"),
                encoded_timestamp,
                len(encoded_nonce).to_bytes(1, "big"),
                encoded_nonce,
                len(encoded_url).to_bytes(2, "big"),
                encoded_url,
                len(encoded_recovery).to_bytes(2, "big"),
                encoded_recovery,
                len(exact).to_bytes(2, "big"),
                exact,
            ]
        )
        return await check_code(
            secret=self.secret, to_sign=to_sign, code=token.hmac, db=self.db_config
        )

    async def is_subscribe_glob_allowed(
        self,
        /,
        *,
        url: str,
        recovery: Optional[str],
        glob: str,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        token = get_token(authorization, now=now, token_lifetime=self.token_lifetime)
        if token.type == TokenInfoType.UNAUTHORIZED:
            return "unauthorized"
        if token.type == TokenInfoType.FORBIDDEN:
            return "forbidden"

        encoded_url = url.encode("utf-8")
        encoded_recovery = b"" if recovery is None else recovery.encode("utf-8")
        encoded_timestamp = token.timestamp.to_bytes(8, "big")
        encoded_glob = glob.encode("utf-8")
        encoded_nonce = token.nonce.encode("utf-8")

        to_sign = b"".join(
            [
                int(AuthMessageType.SUBSCRIBE_GLOB).to_bytes(1, "big"),
                encoded_timestamp,
                len(encoded_nonce).to_bytes(1, "big"),
                encoded_nonce,
                len(encoded_url).to_bytes(2, "big"),
                encoded_url,
                len(encoded_recovery).to_bytes(2, "big"),
                encoded_recovery,
                len(encoded_glob).to_bytes(2, "big"),
                encoded_glob,
            ]
        )
        return await check_code(
            secret=self.secret, to_sign=to_sign, code=token.hmac, db=self.db_config
        )

    async def is_notify_allowed(
        self,
        /,
        *,
        topic: bytes,
        message_sha512: bytes,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        assert len(message_sha512) == 64, "message_sha512 must be 64 bytes long"
        token = get_token(authorization, now=now, token_lifetime=self.token_lifetime)
        if token.type == TokenInfoType.UNAUTHORIZED:
            return "unauthorized"
        if token.type == TokenInfoType.FORBIDDEN:
            return "forbidden"

        encoded_timestamp = token.timestamp.to_bytes(8, "big")
        encoded_nonce = token.nonce.encode("utf-8")

        to_sign = b"".join(
            [
                int(AuthMessageType.NOTIFY).to_bytes(1, "big"),
                encoded_timestamp,
                len(encoded_nonce).to_bytes(1, "big"),
                encoded_nonce,
                len(topic).to_bytes(2, "big"),
                topic,
                message_sha512,
            ]
        )
        return await check_code(
            secret=self.secret, to_sign=to_sign, code=token.hmac, db=self.db_config
        )

    async def is_websocket_configure_allowed(
        self, /, *, message: S2B_Configure, now: float
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        token = get_token(
            message.authorization, now=now, token_lifetime=self.token_lifetime
        )
        if token.type == TokenInfoType.UNAUTHORIZED:
            return "unauthorized"
        if token.type == TokenInfoType.FORBIDDEN:
            return "forbidden"

        encoded_timestamp = token.timestamp.to_bytes(8, "big")
        encoded_nonce = token.nonce.encode("utf-8")

        to_sign = b"".join(
            [
                int(AuthMessageType.WEBSOCKET_CONFIGURE).to_bytes(1, "big"),
                encoded_timestamp,
                len(encoded_nonce).to_bytes(1, "big"),
                encoded_nonce,
                len(message.subscriber_nonce).to_bytes(1, "big"),
                message.subscriber_nonce,
                b"\1" if message.enable_zstd else b"\0",
                b"\1" if message.enable_training else b"\0",
                message.initial_dict.to_bytes(2, "big"),
            ]
        )
        return await check_code(
            secret=self.secret, to_sign=to_sign, code=token.hmac, db=self.db_config
        )

    async def is_check_subscriptions_allowed(
        self, /, *, url: str, now: float, authorization: Optional[str]
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        token = get_token(authorization, now=now, token_lifetime=self.token_lifetime)
        if token.type == TokenInfoType.UNAUTHORIZED:
            return "unauthorized"
        if token.type == TokenInfoType.FORBIDDEN:
            return "forbidden"

        encoded_url = url.encode("utf-8")
        encoded_timestamp = token.timestamp.to_bytes(8, "big")
        encoded_nonce = token.nonce.encode("utf-8")

        to_sign = b"".join(
            [
                int(AuthMessageType.CHECK_SUBSCRIPTIONS).to_bytes(1, "big"),
                encoded_timestamp,
                len(encoded_nonce).to_bytes(1, "big"),
                encoded_nonce,
                len(encoded_url).to_bytes(2, "big"),
                encoded_url,
            ]
        )
        return await check_code(
            secret=self.secret, to_sign=to_sign, code=token.hmac, db=self.db_config
        )

    async def is_set_subscriptions_allowed(
        self,
        /,
        *,
        url: str,
        strong_etag: StrongEtag,
        subscriptions: SetSubscriptionsInfo,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        token = get_token(authorization, now=now, token_lifetime=self.token_lifetime)
        if token.type == TokenInfoType.UNAUTHORIZED:
            return "unauthorized"
        if token.type == TokenInfoType.FORBIDDEN:
            return "forbidden"

        encoded_url = url.encode("utf-8")
        encoded_etag = strong_etag.format.to_bytes(1, "big") + strong_etag.etag
        encoded_timestamp = token.timestamp.to_bytes(8, "big")
        encoded_nonce = token.nonce.encode("utf-8")

        to_sign = b"".join(
            [
                int(AuthMessageType.SET_SUBSCRIPTIONS).to_bytes(1, "big"),
                encoded_timestamp,
                len(encoded_nonce).to_bytes(1, "big"),
                encoded_nonce,
                len(encoded_url).to_bytes(2, "big"),
                encoded_url,
                encoded_etag,
            ]
        )
        return await check_code(
            secret=self.secret, to_sign=to_sign, code=token.hmac, db=self.db_config
        )


class ToSubscriberHmacAuth:
    """Sets up and allows requests to the subscriber which include hash-based
    message authentication codes. These tokens can only be generated if the
    sender knows the shared secret.

    Specifically, the authorization header is formatted as follows:
    `X-HMAC <timestamp>:<nonce>:<token>`, where timestamp is integer seconds from the epoch,
    and the token incorporates the relevant information and is signed with the
    shared secret.
    """

    def __init__(
        self,
        *,
        secret: str,
        token_lifetime: float = 120,
        db_config: IncomingHmacAuthDBConfig,
    ) -> None:
        self.secret = base64.urlsafe_b64decode(secret + "==")
        """The shared secret used to generate the HMAC tokens"""
        assert len(self.secret) == 64, "secret must be 64 bytes long"

        self.token_lifetime = token_lifetime
        """How long after a token is created that we still accept it"""

        self.db_config = db_config
        """The configuration for the recent codes database"""

    async def setup_to_subscriber_auth(self) -> None:
        await self.db_config.setup_hmac_auth_db()

    async def teardown_to_subscriber_auth(self) -> None:
        await self.db_config.teardown_hmac_auth_db()

    async def authorize_receive(
        self, /, *, url: str, topic: bytes, message_sha512: bytes, now: float
    ) -> Optional[str]:
        assert len(message_sha512) == 64, "message_sha512 must be 64 bytes long"
        nonce = make_nonce()
        encoded_url = url.encode("utf-8")
        encoded_timestamp = int(now).to_bytes(8, "big")
        encoded_nonce = nonce.encode("utf-8")

        to_sign = b"".join(
            [
                int(AuthMessageType.RECEIVE).to_bytes(1, "big"),
                encoded_timestamp,
                len(encoded_nonce).to_bytes(1, "big"),
                encoded_nonce,
                len(encoded_url).to_bytes(2, "big"),
                encoded_url,
                len(topic).to_bytes(2, "big"),
                topic,
                message_sha512,
            ]
        )
        return sign(secret=self.secret, to_sign=to_sign, nonce=nonce, now=now)

    async def is_receive_allowed(
        self,
        /,
        *,
        url: str,
        topic: bytes,
        message_sha512: bytes,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        assert len(message_sha512) == 64, "message_sha512 must be 64 bytes long"
        token = get_token(authorization, now=now, token_lifetime=self.token_lifetime)
        if token.type == TokenInfoType.UNAUTHORIZED:
            return "unauthorized"
        if token.type == TokenInfoType.FORBIDDEN:
            return "forbidden"

        encoded_url = url.encode("utf-8")
        encoded_timestamp = token.timestamp.to_bytes(8, "big")
        encoded_nonce = token.nonce.encode("utf-8")
        to_sign = b"".join(
            [
                int(AuthMessageType.RECEIVE).to_bytes(1, "big"),
                encoded_timestamp,
                len(encoded_nonce).to_bytes(1, "big"),
                encoded_nonce,
                len(encoded_url).to_bytes(2, "big"),
                encoded_url,
                len(topic).to_bytes(2, "big"),
                topic,
                message_sha512,
            ]
        )
        return await check_code(
            secret=self.secret, to_sign=to_sign, code=token.hmac, db=self.db_config
        )

    async def is_missed_allowed(
        self,
        /,
        *,
        recovery: str,
        topic: bytes,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        token = get_token(authorization, now=now, token_lifetime=self.token_lifetime)
        if token.type == TokenInfoType.UNAUTHORIZED:
            return "unauthorized"
        if token.type == TokenInfoType.FORBIDDEN:
            return "forbidden"

        encoded_recovery = recovery.encode("utf-8")
        encoded_timestamp = token.timestamp.to_bytes(8, "big")
        encoded_nonce = token.nonce.encode("utf-8")

        to_sign = b"".join(
            [
                int(AuthMessageType.MISSED).to_bytes(1, "big"),
                encoded_timestamp,
                len(encoded_nonce).to_bytes(1, "big"),
                encoded_nonce,
                len(encoded_recovery).to_bytes(2, "big"),
                encoded_recovery,
                len(topic).to_bytes(2, "big"),
                topic,
            ]
        )
        return await check_code(
            secret=self.secret, to_sign=to_sign, code=token.hmac, db=self.db_config
        )

    async def authorize_missed(
        self, /, *, recovery: str, topic: bytes, now: float
    ) -> Optional[str]:
        nonce = make_nonce()
        encoded_recovery = recovery.encode("utf-8")
        encoded_timestamp = int(now).to_bytes(8, "big")
        encoded_nonce = nonce.encode("utf-8")

        to_sign = b"".join(
            [
                int(AuthMessageType.MISSED).to_bytes(1, "big"),
                encoded_timestamp,
                len(encoded_nonce).to_bytes(1, "big"),
                encoded_nonce,
                len(encoded_recovery).to_bytes(2, "big"),
                encoded_recovery,
                len(topic).to_bytes(2, "big"),
                topic,
            ]
        )
        return sign(secret=self.secret, to_sign=to_sign, nonce=nonce, now=now)

    async def authorize_websocket_confirm_configure(
        self, /, *, broadcaster_nonce: bytes, now: float
    ) -> Optional[str]:
        nonce = make_nonce()
        encoded_timestamp = int(now).to_bytes(8, "big")
        encoded_nonce = nonce.encode("utf-8")

        to_sign = b"".join(
            [
                int(AuthMessageType.WEBSOCKET_CONFIRM_CONFIGURE).to_bytes(1, "big"),
                encoded_timestamp,
                len(encoded_nonce).to_bytes(1, "big"),
                encoded_nonce,
                len(broadcaster_nonce).to_bytes(1, "big"),
                broadcaster_nonce,
            ]
        )
        return sign(secret=self.secret, to_sign=to_sign, nonce=nonce, now=now)


if TYPE_CHECKING:
    __: Type[ToBroadcasterAuthConfig] = ToBroadcasterHmacAuth
    ___: Type[ToSubscriberAuthConfig] = ToSubscriberHmacAuth
