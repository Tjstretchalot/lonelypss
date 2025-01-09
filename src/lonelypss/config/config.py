import asyncio
import enum
import importlib
import random
from typing import (
    TYPE_CHECKING,
    AsyncIterable,
    Dict,
    List,
    Literal,
    Optional,
    Protocol,
    Tuple,
    Type,
    Union,
)

from lonelypsp.auth.config import AuthConfig
from lonelypsp.compat import fast_dataclass
from lonelypsp.stateful.messages.configure import S2B_Configure
from lonelypsp.stateful.messages.confirm_configure import B2S_ConfirmConfigure
from lonelypsp.stateless.make_strong_etag import StrongEtag

from lonelypss.config.set_subscriptions_info import SetSubscriptionsInfo

try:
    import zstandard
except ImportError:
    ...


class SubscriberInfoType(enum.Enum):
    EXACT = enum.auto()
    GLOB = enum.auto()
    UNAVAILABLE = enum.auto()


@fast_dataclass
class SubscriberInfoExact:
    type: Literal[SubscriberInfoType.EXACT]
    """Indicates we found a subscriber on this exact topic"""
    url: str
    """The url to reach the subscriber"""
    recovery: Optional[str]
    """The recovery url to later tell the subscriber if we fail to
    send them this message, if any
    """


@fast_dataclass
class SubscriberInfoGlob:
    type: Literal[SubscriberInfoType.GLOB]
    """Indicates we found a subscriber for this topic via a matching glob subscription"""
    glob: str
    """The glob that matched the topic"""
    url: str
    """The url to reach the subscriber"""
    recovery: Optional[str]
    """The recovery url to later tell the subscriber if we fail to
    send them this message, if any
    """


@fast_dataclass
class SubscriberInfoUnavailable:
    type: Literal[SubscriberInfoType.UNAVAILABLE]
    """Indicates that the database for subscriptions is unavailable
    and the request should be aborted with a 503
    """


SubscriberInfo = Union[
    SubscriberInfoExact, SubscriberInfoGlob, SubscriberInfoUnavailable
]


@fast_dataclass
class MissedInfo:
    """information about a url we are trying to send a missed message to"""

    topic: bytes
    """the topic that the message was supposed to be sent to"""
    attempts: int
    """how many times we have tried to send this message"""
    next_retry_at: float
    """the next time we should try to send this message"""
    subscriber_info: Union[SubscriberInfoExact, SubscriberInfoGlob]
    """the subscriber that was supposed to receive the message; if deleted,
    the missed row MUST also be deleted
    """


@fast_dataclass
class MutableMissedInfo:
    """just the actually changable parts of MissedInfo after an attempt"""

    attempts: int
    """how many times we have tried to send this message"""
    next_retry_at: float
    """the next time we should try to send this message"""


class LockedMissedInfo(Protocol):
    """Used in `get_overdue_missed_with_lock`; a MissedInfo item that is temporarily
    prevented from being returned in concurrent calls to `get_overdue_missed_with_lock`
    (both in other processes connected to the same database and in other
    coroutines in the same async thread) until the lock time has expired
    or `release` has been called.
    """

    @property
    def info(self) -> MissedInfo:
        """the potentially still protected MissedInfo"""

    async def release(
        self,
        /,
        *,
        new_info: Optional[MutableMissedInfo] = None,
    ) -> Literal["conflict", "unavailable", "ok"]:
        """Releases the lock on this item and sets the new information or
        deletes the record based on `new_info`. This should not raise an
        exception if the lock has already expired or `release` has already
        been called, instead, return `conflict`

        Args:
            new_info (Optional[MutableMissedInfo], None): the new information
                to set, or None to delete the record

        Returns:
            `conflict`: if the record is no longer the same as it was so
                this did nothing
            `unavailable`: if the database for missed messages is unavailable
            `ok`: the record was the same as it was and it was updated/deleted
                as requested
        """


class DBConfig(Protocol):
    async def setup_db(self) -> None:
        """Prepares the database for use. If the database is not re-entrant, it must
        check for re-entrant calls and error out
        """

    async def teardown_db(self) -> None:
        """Cleans up the database after use. This is called when the server is done
        using the database, and should release any resources it acquired during
        `setup_db`.
        """

    async def subscribe_exact(
        self, /, *, url: str, recovery: Optional[str], exact: bytes
    ) -> Literal["success", "conflict", "unavailable"]:
        """Subscribes the given URL to the given exact match.

        Args:
            url (str): the url that will receive notifications
            recovery (str, None): if the subscriber wishes for a MISSED message
                to be posted to a url when the broadcaster fails to send them a
                message on this topic (later, with more spread out retries),
                this is the url that will receive that message. If None, no
                MISSED message will be sent. NOTE: missed messages do not
                contain the actual message missed
            exact (bytes): the exact topic they want to receive messages from

        Returns:
            `success`: if the subscription was added
            `conflict`: if the subscription already exists. the recovery url MUST NOT
                have been changed (if it differs)
            `unavailable`: if a service is required to check this isn't available
        """

    async def unsubscribe_exact(
        self, /, *, url: str, exact: bytes
    ) -> Literal["success", "not_found", "unavailable"]:
        """Unsubscribes the given URL from the given exact match

        Args:
            url (str): the url that will receive notifications
            exact (bytes): the exact topic they want to receive messages from

        Returns:
            `success`: if the subscription was removed
            `not_found`: if the subscription didn't exist
            `unavailable`: if the database for subscriptions is unavailable
        """

    async def subscribe_glob(
        self, /, *, url: str, recovery: Optional[str], glob: str
    ) -> Literal["success", "conflict", "unavailable"]:
        """Subscribes the given URL to the given glob-style match

        Args:
            url (str): the url that will receive notifications
            recovery (str, None): if the subscriber wishes for a MISSED message
                to be posted to a url when the broadcaster fails to send them a
                message on this glob (later, with more spread out retries), this
                is the url that will receive that message. If None, no MISSED
                message will be sent. NOTE: missed messages do not contain the
                actual message missed
            glob (str): a glob for the topics that they want to receive notifications from

        Returns:
            `success`: if the subscription was added
            `conflict`: if the subscription already exists. the recovery url MUST NOT
                have been changed (if it differs)
            `unavailable`: if the database for subscriptions is unavailable
        """

    async def unsubscribe_glob(
        self, /, *, url: str, glob: str
    ) -> Literal["success", "not_found", "unavailable"]:
        """Unsubscribes the given URL from the given glob-style match

        Args:
            url (str): the url that will receive notifications
            glob (str): a glob for the topics that they want to receive notifications from

        Returns:
            `success`: if the subscription was removed
            `not_found`: if the subscription didn't exist
            `unavailable`: if the database for subscriptions is unavailable
        """

    async def check_subscriptions(self, /, *, url: str) -> StrongEtag:
        """Determines what the current subscriptions are for a given URL, computing
        the strong etag corresponding to that url and set of subscriptions, and
        returning that.

        NOTE: the strong etag format is designed to be computable without loading
        all the subscriptions into memory but does require a linear pass over the
        relevant subscriptions.

        NOTE: typically, paginate through results and use create_strong_etag_generator

        NOTE: if the subscriptions are mutated while this is running the result must
        be an etag that includes every item that was in the database the entire
        iteration, must not include any item that was not in the database the entire
        iteration, and must not include duplicates. Items that were added or removed
        during the iteration may be arbitrarily included or excluded from the etag.

        Args:
            url (str): the url that we are checking

        Returns:
            StrongEtag: the strong etag for the subscriptions
        """

    async def set_subscriptions(
        self,
        /,
        *,
        url: str,
        strong_etag: StrongEtag,
        subscriptions: SetSubscriptionsInfo,
    ) -> Literal["success", "unavailable"]:
        """Sets the subscriptions for a given URL to the given topics and globs.
        This does not need to be completely atomic, though it is desirable. If
        it is not atomic, the rules within lonelypsp's documentation for
        `SET_SUBSCRIPTION` must be followed

        Args:
            url (str): the url that will receive notifications
            strong_etag (StrongEtag): the etag that was returned from the last check_subscriptions call
            subscriptions (SetSubscriptionsInfo): the topics and globs that the
                subscriber wants to receive. this list may be large, so a more
                restrictive interface is provided to provide flexibility in its
                implementation. the list is guarranteed to be in ascending lexicographic
                order for the topics/globs

        Returns:
            `success`: if the subscriptions were set
            `conflict`: if the etag didn't match the current state of the database
            `unavailable`: if the database for subscriptions is unavailable
        """

    def get_subscribers(self, /, *, topic: bytes) -> AsyncIterable[SubscriberInfo]:
        """Streams back the subscriber urls that match the given topic. We will post messages
        to these urls as they are provided. This should return duplicates if multiple subscriptions
        match with the same url.

        Args:
            topic (bytes): the topic that we are looking for

        Yields:
            (SubscriberInfo): the subscriber that was found, or a special value indicating
                that the database is unavailable.
        """

    async def upsert_missed(
        self, /, *, info: MissedInfo
    ) -> Literal["success", "unavailable"]:
        """Updates the given missed information, which should be looked up using
        the subscriber information, with the new attempt/next attempt time. If
        the subscriber is a GLOB subscriber, the topic should also be updated

        Should clear any locks from `get_overdue_missed_with_lock` for the same
        `info.subscriber_info.url` and `info.topic`

        Args:
            info (MissedInfo): the information about the missed message

        Returns:
            `success`: if the missed message was updated
            `unavailable`: if the database for missed messages is unavailable
        """

    def get_overdue_missed_with_lock(
        self, /, *, now: float
    ) -> AsyncIterable[LockedMissedInfo]:
        """Streams back the missed messages that are overdue for retrying. This
        has important concurrency requirements: when this is called in parallel,
        async or from different processes, it should not repeat messages until
        the lock time is elapsed without `__aexit__` being called on the item.
        The choice of lock time is implementation-defined, but should be at least
        enough time to attempt contacting the subscriber and getting a response.

        Args:
            now (float): the current time in seconds since the epoch

        Yields:
            (LockedMissedInfo): the missed message that is overdue
        """


class GenericConfig(Protocol):
    @property
    def message_body_spool_size(self) -> int:
        """If the message body exceeds this size we always switch to a temporary file.

        In general, unless there is another specific configuration option for it, this
        is the maximum size of any single arbitrary length item (e.g., the decompressed
        body of a compressed message) that is held in memory before spooling to file.
        """

    @property
    def outgoing_http_timeout_total(self) -> Optional[float]:
        """The total timeout for outgoing http requests in seconds"""

    @property
    def outgoing_http_timeout_connect(self) -> Optional[float]:
        """The timeout for connecting to the server in seconds, which may include multiple
        socket attempts
        """

    @property
    def outgoing_http_timeout_sock_read(self) -> Optional[float]:
        """The timeout for reading from a socket to the server in seconds before the socket is
        considered dead
        """

    @property
    def outgoing_http_timeout_sock_connect(self) -> Optional[float]:
        """The timeout for a single socket connecting to the server before we give up in seconds"""

    @property
    def websocket_accept_timeout(self) -> Optional[float]:
        """The timeout for accepting a websocket connection in seconds"""

    @property
    def websocket_max_pending_sends(self) -> Optional[int]:
        """The maximum number of pending sends (not yet sent to the ASGI server) before we
        disconnect the websocket forcibly. This mainly protects against (accidental) tarpitting
        when the subscriber cannot keep up
        """

    @property
    def websocket_max_unprocessed_receives(self) -> Optional[int]:
        """The maximum number of unprocessed received websocket messages before we disconnect
        the websocket forcibly. Note that this does not include any websocket messages buffered
        above us (such as in the ASGI server). This is primarily intended to help improve recovery
        when the broadcaster cannot keep up by resetting connections we are very far behind on
        """

    @property
    def websocket_large_direct_send_timeout(self) -> Optional[float]:
        """How long we are willing to wait for a websocket.send to complete while holding
        an exclusive file handle to the message being sent before copying the remainder of
        the message to memory and progressing the other sends. This value is in seconds,
        and a reasonable choice is 0.3 (300ms)

        A value of 0 means it must complete within one event loop
        """

    @property
    def websocket_send_max_unacknowledged(self) -> Optional[int]:
        """The maximum number of NOTIFY/NOTIFY STREAM messages we will have outgoing before
        waiting for them to be acknowledged by the client. This is intended to help alleviate
        issues related to clients that cannot keep up due to processing issues rather than
        network issues, as having full websocket buffers tends to result in a poor debugging
        experience.

        A reasonable value is 3
        """

    @property
    def websocket_minimal_headers(self) -> bool:
        """True if all messages from the broadcaster to the subscriber should use
        minimal headers, which are faster to parse and more compact but require
        that the subscriber and broadcaster precisely agree on the headers for
        each message. False if all messages from the broadcaster to the
        subscriber use expanded headers, which are more flexible and easier to
        debug but slower to parse and more verbose.

        If you are trying to understand the lonelypss protocol via something
        like wireshark, setting this to False will make messages somewhat easier
        to understand.

        Note that broadcasters and subscribers do not need to agree on this
        setting. It is ok if the broadcaster is sending expanded headers and the
        subscriber is sending minimal headers, or vice versa, as this only
        configures the outgoing messages but they both always accept either
        version for incoming messages.

        Generally, this should be True except when in the process of updating
        the lonelypss/lonelypsc libraries, in which case it should be changed to
        false on the broadcaster and subscribers, then they should be updated
        one at a time, then set to true.
        """

    @property
    def sweep_missed_interval(self) -> float:
        """The interval in seconds between sweeps of pending MISSED messages,
        which are retried for a significant period of time (more than enough
        to get past any transient network issues) so that subscribers can
        trigger their reboot recovery mechanism if they did not detect that
        they were not reachable
        """


class GenericConfigFromValues:
    """Convenience class that allows you to create a GenericConfig protocol
    satisfying object from values"""

    def __init__(
        self,
        message_body_spool_size: int,
        outgoing_http_timeout_total: Optional[float],
        outgoing_http_timeout_connect: Optional[float],
        outgoing_http_timeout_sock_read: Optional[float],
        outgoing_http_timeout_sock_connect: Optional[float],
        websocket_accept_timeout: Optional[float],
        websocket_max_pending_sends: Optional[int],
        websocket_max_unprocessed_receives: Optional[int],
        websocket_large_direct_send_timeout: Optional[float],
        websocket_send_max_unacknowledged: Optional[int],
        websocket_minimal_headers: bool,
        sweep_missed_interval: float,
    ):
        self.message_body_spool_size = message_body_spool_size
        self.outgoing_http_timeout_total = outgoing_http_timeout_total
        self.outgoing_http_timeout_connect = outgoing_http_timeout_connect
        self.outgoing_http_timeout_sock_read = outgoing_http_timeout_sock_read
        self.outgoing_http_timeout_sock_connect = outgoing_http_timeout_sock_connect
        self.websocket_accept_timeout = websocket_accept_timeout
        self.websocket_max_pending_sends = websocket_max_pending_sends
        self.websocket_max_unprocessed_receives = websocket_max_unprocessed_receives
        self.websocket_large_direct_send_timeout = websocket_large_direct_send_timeout
        self.websocket_send_max_unacknowledged = websocket_send_max_unacknowledged
        self.websocket_minimal_headers = websocket_minimal_headers
        self.sweep_missed_interval = sweep_missed_interval


class MissedRetryConfig(Protocol):
    async def get_delay_for_next_missed_retry(
        self, /, *, receive_url: str, missed_url: str, topic: bytes, attempts: int
    ) -> Optional[float]:
        """Determines the number of fractional seconds to wait before retrying
        a missed message sent to a subscriber that failed to receive a message
        on a topic.

        Essentially, the flow on failure when a recovery url is specified is

        ```
        Broadcaster -> Subscriber: RECEIVE (error)
        (get_delay_for_next_missed_retry(attempts=0) seconds pass)
        Broadcaster -> Subscriber: MISSED (error)
        (get_delay_for_next_missed_retry(attempts=1) seconds pass)
        Broadcaster -> Subscriber: MISSED (error)
        (get_delay_for_next_missed_retry(attempts=2) seconds pass)
        ...
        ```

        May return None to never retry again.

        Args:
            receive_url (str): the url that was supposed to get the RECEIVE
            missed_url (str): the url that gets the MISSED
            topic (bytes): the topic the message was posted to
            attempts (int): the number of attempts that have been made so far
                to send the MISSED message

        Returns:
            None: no more retries
            float: the minimum time in seconds to wait before retrying
        """


class MissedRetryStandard:
    """Standard implementation of the MissedRetryConfig protocol

    `expo_factor * (expo_base ** min(expo_max, attempts)) + constant + random() * jitter`
    """

    def __init__(
        self,
        expo_factor: float,
        expo_base: float,
        expo_max: int,
        max_retries: int,
        constant: float,
        jitter: float,
    ) -> None:
        """
        `expo_factor * (expo_base ** min(expo_max, attempts)) + constant + random() * jitter`

        Args:
            expo_factor (float): the factor to multiply the base by to get the
                delay between retries
            expo_base (float): the base delay between retries
            expo_max (int): the maximum number of retries before we stop
            max_retries (int): the maximum number of retries before we stop
            constant (float): the constant delay to add to the exponential delay
            jitter (float): the amount of jitter to add to the delay
        """
        self.expo_factor = expo_factor
        self.expo_base = expo_base
        self.expo_max = expo_max
        self.max_retries = max_retries
        self.constant = constant
        self.jitter = jitter

    async def get_delay_for_next_missed_retry(
        self, /, *, receive_url: str, missed_url: str, topic: bytes, attempts: int
    ) -> Optional[float]:
        if attempts >= self.max_retries:
            return None

        return (
            self.expo_factor * (self.expo_base ** min(self.expo_max, attempts))
            + self.constant
            + random.random() * self.jitter
        )


class CompressionConfig(Protocol):
    """Configuration for compression over websockets"""

    @property
    def compression_allowed(self) -> bool:
        """True to allow zstandard compression for websocket messages when supported
        by the client, false to disable service-level compression entirely.
        """

    async def get_compression_dictionary_by_id(
        self, dictionary_id: int, /
    ) -> "Optional[Tuple[zstandard.ZstdCompressionDict, int]]":
        """If a precomputed zstandard compression dictionary is available with the
        given id, the dictionary and the compression level to use should be
        returned. If the dictionary is not available, return None.

        This is generally only useful if you are using short-lived websocket
        connections where trained dictionaries won't kick in, or you want to go
        through the effort of hand-building a dictionary for a specific
        use-case.

        The returned dict should have its data precomputed as if by `precompute_compress`
        """

    @property
    def outgoing_max_ws_message_size(self) -> Optional[int]:
        """We will try not to send websocket messages over this size. If this is at least
        64kb, then we will guarrantee we won't go over this value.

        Generally, breaking websocket messages apart is redundant: the websocket protocol
        already has a concept of frames which can be used to send messages in parts. Further,
        it's almost certainly more performant to break messages at a lower level in the stack.

        However, in practice, the default settings of most websocket servers and
        clients will not accept arbitrarily large messages, and the entire
        message is often kept in memory, so we break them up to avoid issues. Note that when
        we break messages we will spool to disk as they come in.

        A reasonable value is 16mb

        Return None for no limit
        """

    @property
    def allow_training(self) -> bool:
        """True to allow training dictionaries in websockets, false to completely disable
        that feature
        """

    @property
    def compression_min_size(self) -> int:
        """The smallest message size we will try to compress

        A reasonable size is 32 bytes
        """

    @property
    def compression_trained_max_size(self) -> int:
        """The largest message size we will try to use a custom trained dictionary for; for
        messages larger than this, we will not use a shared compression dictionary to reduce
        overhead, as theres enough context within the message to generate its own dictionary,
        and the relative overhead of including that dictionary will be low.

        A reasonable value is 16kb
        """

    @property
    def compression_training_low_watermark(self) -> int:
        """How much data we get before we make the first pass at the custom compression dictionary

        A reasonable value is 100kb
        """

    async def train_compression_dict_low_watermark(
        self, /, samples: List[bytes]
    ) -> "Tuple[zstandard.ZstdCompressionDict, int]":
        """Trains a compression dictionary using the samples whose combined size is at least the
        `compression_training_low_watermark` size, then tells us what level compression to use

        Typically, this is something like

        ```python
        import zstandard
        import asyncio

        async def train_compression_dict_low_watermark(samples: List[bytes]) -> zstandard.ZstdCompressionDict:
            zdict = await asyncio.to_thread(
                zstandard.train_dictionary,
                16384,
                samples
            )
            await asyncio.to_thread(zdict.precompute_compress, level=3)
            return (zdict, 3)
        ```
        """

    @property
    def compression_training_high_watermark(self) -> int:
        """After we reach the low watermark and coordinate a compression dictionary, we retain
        those samples and wait until we reach this watermark to train the dictionary again.

        The low watermark gets us to some level of compression reasonably quickly, and the high
        watermark gets us to a more accurate dictionary once there's enough data to work with.

        A reasonable value is 10mb
        """

    async def train_compression_dict_high_watermark(
        self, /, samples: List[bytes]
    ) -> "Tuple[zstandard.ZstdCompressionDict, int]":
        """Trains a compression dictionary using the samples whose combined size is at least the
        `compression_training_high_watermark` size and tells us what compression level to use.

        Typically, this is something like

        ```python
        import zstandard
        import asyncio

        async def train_compression_dict_low_watermark(samples: List[bytes]) -> zstandard.ZstdCompressionDict:
            zdict = await asyncio.to_thread(
                zstandard.train_dictionary,
                65536,
                samples
            )
            await asyncio.to_thread(zdict.precompute_compress, level=10)
            return (zdict, 10)
        ```
        """

    @property
    def compression_retrain_interval_seconds(self) -> int:
        """How long in seconds between rebuilding a compression dictionary for very long-lived
        websocket connections.

        Especially when there are timestamps within the message body, the compression dictionary
        needs occasional refreshing to remain effective.

        A reasonable value is 1 day
        """

    @property
    def decompression_max_window_size(self) -> int:
        """
        Sets an upper limit on the window size for decompression operations
        in kibibytes. This setting can be used to prevent large memory
        allocations for inputs using large compression windows.

        Use 0 for no limit.

        A reasonable value is 0 for no limit. Alternatively, it should be 8mb if
        trying to match the zstandard minimum decoder requirements. The
        remaining alternative would be as high as the server can bear, noting
        that this much memory may be allocated by every websocket connection up
        to 3 times (once for standard decompression without a custom dictionary,
        once for the most recent custom dictionary, and once for the second most
        recent custom dictionary). If disabling training, a websocket will use
        up to 1x this memory on decompression buffers.

        WARN:
            This should not be considered a security measure. Authorization
            is already passed prior to decompression, and if that is not enough
            to eliminate adversarial payloads, then disable compression.
        """


class CompressionConfigFromParts:
    """Convenience class that allows you to create a CompressionConfig protocol
    satisfying object from values, using default implementations for the methods
    """

    def __init__(
        self,
        compression_allowed: bool,
        compression_dictionary_by_id: "Dict[int, Tuple[zstandard.ZstdCompressionDict, int]]",
        outgoing_max_ws_message_size: Optional[int],
        allow_training: bool,
        compression_min_size: int,
        compression_trained_max_size: int,
        compression_training_low_watermark: int,
        compression_training_high_watermark: int,
        compression_retrain_interval_seconds: int,
        decompression_max_window_size: int,
    ):
        if compression_allowed:
            try:
                importlib.import_module("zstandard")
            except ImportError:
                raise ValueError(
                    "Compression is allowed, but zstandard is not available. "
                    "Set compression_allowed=False to disable compression, or "
                    "`pip install zstandard` to enable it."
                )

        if 0 in compression_dictionary_by_id:
            raise ValueError("Dictionary ID 0 is reserved for no compression")

        if 1 in compression_dictionary_by_id:
            raise ValueError(
                "Dictionary ID 1 is reserved for not using a compression dictionary"
            )

        self.compression_allowed = compression_allowed
        self.compression_dictionary_by_id = compression_dictionary_by_id
        self.outgoing_max_ws_message_size = outgoing_max_ws_message_size
        self.allow_training = allow_training
        self.compression_min_size = compression_min_size
        self.compression_trained_max_size = compression_trained_max_size
        self.compression_training_low_watermark = compression_training_low_watermark
        self.compression_training_high_watermark = compression_training_high_watermark
        self.compression_retrain_interval_seconds = compression_retrain_interval_seconds
        self.decompression_max_window_size = decompression_max_window_size

    async def get_compression_dictionary_by_id(
        self, dictionary_id: int, /
    ) -> "Optional[Tuple[zstandard.ZstdCompressionDict, int]]":
        return self.compression_dictionary_by_id.get(dictionary_id)

    async def train_compression_dict_low_watermark(
        self, /, samples: List[bytes]
    ) -> "Tuple[zstandard.ZstdCompressionDict, int]":
        zdict = await asyncio.to_thread(zstandard.train_dictionary, 16384, samples)
        await asyncio.to_thread(zdict.precompute_compress, level=3)
        return (zdict, 3)

    async def train_compression_dict_high_watermark(
        self, /, samples: List[bytes]
    ) -> "Tuple[zstandard.ZstdCompressionDict, int]":
        zdict = await asyncio.to_thread(zstandard.train_dictionary, 65536, samples)
        await asyncio.to_thread(zdict.precompute_compress, level=10)
        return (zdict, 10)


class Config(
    AuthConfig, DBConfig, GenericConfig, MissedRetryConfig, CompressionConfig, Protocol
):
    """The injected behavior required for the lonelypss to operate. This is
    generally generated for you using one of the templates, see the readme for details
    """


class ConfigFromParts:
    """Convenience class that combines the three parts of the config into a single object."""

    def __init__(
        self,
        auth: AuthConfig,
        db: DBConfig,
        generic: GenericConfig,
        missed: MissedRetryConfig,
        compression: CompressionConfig,
    ):
        self.auth = auth
        self.db = db
        self.generic = generic
        self.missed = missed
        self.compression = compression

    async def setup_to_broadcaster_auth(self) -> None:
        await self.auth.setup_to_broadcaster_auth()

    async def teardown_to_broadcaster_auth(self) -> None:
        await self.auth.teardown_to_broadcaster_auth()

    async def setup_to_subscriber_auth(self) -> None:
        await self.auth.setup_to_subscriber_auth()

    async def teardown_to_subscriber_auth(self) -> None:
        await self.auth.teardown_to_subscriber_auth()

    async def setup_db(self) -> None:
        await self.db.setup_db()

    async def teardown_db(self) -> None:
        await self.db.teardown_db()

    async def authorize_subscribe_exact(
        self, /, *, url: str, recovery: Optional[str], exact: bytes, now: float
    ) -> Optional[str]:
        return await self.auth.authorize_subscribe_exact(
            url=url, recovery=recovery, exact=exact, now=now
        )

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
        return await self.auth.is_subscribe_exact_allowed(
            url=url,
            recovery=recovery,
            exact=exact,
            now=now,
            authorization=authorization,
        )

    async def authorize_subscribe_glob(
        self, /, *, url: str, recovery: Optional[str], glob: str, now: float
    ) -> Optional[str]:
        return await self.auth.authorize_subscribe_glob(
            url=url, recovery=recovery, glob=glob, now=now
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
        return await self.auth.is_subscribe_glob_allowed(
            url=url, recovery=recovery, glob=glob, now=now, authorization=authorization
        )

    async def authorize_notify(
        self, /, *, topic: bytes, message_sha512: bytes, now: float
    ) -> Optional[str]:
        return await self.auth.authorize_notify(
            topic=topic, message_sha512=message_sha512, now=now
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
        return await self.auth.is_notify_allowed(
            topic=topic,
            message_sha512=message_sha512,
            now=now,
            authorization=authorization,
        )

    async def authorize_stateful_configure(
        self,
        /,
        *,
        subscriber_nonce: bytes,
        enable_zstd: bool,
        enable_training: bool,
        initial_dict: int,
    ) -> Optional[str]:
        return await self.auth.authorize_stateful_configure(
            subscriber_nonce=subscriber_nonce,
            enable_zstd=enable_zstd,
            enable_training=enable_training,
            initial_dict=initial_dict,
        )

    async def is_stateful_configure_allowed(
        self, /, *, message: S2B_Configure, now: float
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return await self.auth.is_stateful_configure_allowed(message=message, now=now)

    async def authorize_check_subscriptions(
        self, /, *, url: str, now: float
    ) -> Optional[str]:
        return await self.auth.authorize_check_subscriptions(url=url, now=now)

    async def is_check_subscriptions_allowed(
        self, /, *, url: str, now: float, authorization: Optional[str]
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return await self.auth.is_check_subscriptions_allowed(
            url=url, now=now, authorization=authorization
        )

    async def authorize_set_subscriptions(
        self, /, *, url: str, strong_etag: StrongEtag, now: float
    ) -> Optional[str]:
        return await self.auth.authorize_set_subscriptions(
            url=url, strong_etag=strong_etag, now=now
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
        return await self.auth.is_set_subscriptions_allowed(
            url=url,
            strong_etag=strong_etag,
            subscriptions=subscriptions,
            now=now,
            authorization=authorization,
        )

    async def authorize_receive(
        self, /, *, url: str, topic: bytes, message_sha512: bytes, now: float
    ) -> Optional[str]:
        return await self.auth.authorize_receive(
            url=url, topic=topic, message_sha512=message_sha512, now=now
        )

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
        return await self.auth.is_receive_allowed(
            url=url,
            topic=topic,
            message_sha512=message_sha512,
            now=now,
            authorization=authorization,
        )

    async def authorize_missed(
        self, /, *, recovery: str, topic: bytes, now: float
    ) -> Optional[str]:
        return await self.auth.authorize_missed(recovery=recovery, topic=topic, now=now)

    async def is_missed_allowed(
        self,
        /,
        *,
        recovery: str,
        topic: bytes,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return await self.auth.is_missed_allowed(
            recovery=recovery, topic=topic, now=now, authorization=authorization
        )

    async def authorize_stateful_confirm_configure(
        self, /, *, broadcaster_nonce: bytes, now: float
    ) -> Optional[str]:
        return await self.auth.authorize_stateful_confirm_configure(
            broadcaster_nonce=broadcaster_nonce, now=now
        )

    async def is_stateful_confirm_configure_allowed(
        self, /, *, message: B2S_ConfirmConfigure, now: float
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return await self.auth.is_stateful_confirm_configure_allowed(
            message=message, now=now
        )

    async def subscribe_exact(
        self, /, *, url: str, recovery: Optional[str], exact: bytes
    ) -> Literal["success", "conflict", "unavailable"]:
        return await self.db.subscribe_exact(url=url, recovery=recovery, exact=exact)

    async def unsubscribe_exact(
        self, /, *, url: str, exact: bytes
    ) -> Literal["success", "not_found", "unavailable"]:
        return await self.db.unsubscribe_exact(url=url, exact=exact)

    async def subscribe_glob(
        self, /, *, url: str, recovery: Optional[str], glob: str
    ) -> Literal["success", "conflict", "unavailable"]:
        return await self.db.subscribe_glob(url=url, recovery=recovery, glob=glob)

    async def unsubscribe_glob(
        self, /, *, url: str, glob: str
    ) -> Literal["success", "not_found", "unavailable"]:
        return await self.db.unsubscribe_glob(url=url, glob=glob)

    def get_subscribers(self, /, *, topic: bytes) -> AsyncIterable[SubscriberInfo]:
        return self.db.get_subscribers(topic=topic)

    async def check_subscriptions(self, /, *, url: str) -> StrongEtag:
        return await self.db.check_subscriptions(url=url)

    async def set_subscriptions(
        self,
        /,
        *,
        url: str,
        strong_etag: StrongEtag,
        subscriptions: SetSubscriptionsInfo,
    ) -> Literal["success", "unavailable"]:
        return await self.db.set_subscriptions(
            url=url, strong_etag=strong_etag, subscriptions=subscriptions
        )

    async def upsert_missed(
        self, /, *, info: MissedInfo
    ) -> Literal["success", "unavailable"]:
        return await self.db.upsert_missed(info=info)

    def get_overdue_missed_with_lock(
        self, /, *, now: float
    ) -> AsyncIterable[LockedMissedInfo]:
        return self.db.get_overdue_missed_with_lock(now=now)

    @property
    def message_body_spool_size(self) -> int:
        return self.generic.message_body_spool_size

    @property
    def outgoing_http_timeout_total(self) -> Optional[float]:
        return self.generic.outgoing_http_timeout_total

    @property
    def outgoing_http_timeout_connect(self) -> Optional[float]:
        return self.generic.outgoing_http_timeout_connect

    @property
    def outgoing_http_timeout_sock_read(self) -> Optional[float]:
        return self.generic.outgoing_http_timeout_sock_read

    @property
    def outgoing_http_timeout_sock_connect(self) -> Optional[float]:
        return self.generic.outgoing_http_timeout_sock_connect

    @property
    def websocket_accept_timeout(self) -> Optional[float]:
        return self.generic.websocket_accept_timeout

    @property
    def websocket_max_pending_sends(self) -> Optional[int]:
        return self.generic.websocket_max_pending_sends

    @property
    def websocket_max_unprocessed_receives(self) -> Optional[int]:
        return self.generic.websocket_max_unprocessed_receives

    @property
    def websocket_large_direct_send_timeout(self) -> Optional[float]:
        return self.generic.websocket_large_direct_send_timeout

    @property
    def websocket_send_max_unacknowledged(self) -> Optional[int]:
        return self.generic.websocket_send_max_unacknowledged

    async def get_delay_for_next_missed_retry(
        self, /, *, receive_url: str, missed_url: str, topic: bytes, attempts: int
    ) -> Optional[float]:
        return await self.missed.get_delay_for_next_missed_retry(
            receive_url=receive_url,
            missed_url=missed_url,
            topic=topic,
            attempts=attempts,
        )

    @property
    def compression_allowed(self) -> bool:
        return self.compression.compression_allowed

    async def get_compression_dictionary_by_id(
        self, dictionary_id: int, /
    ) -> "Optional[Tuple[zstandard.ZstdCompressionDict, int]]":
        return await self.compression.get_compression_dictionary_by_id(dictionary_id)

    @property
    def outgoing_max_ws_message_size(self) -> Optional[int]:
        return self.compression.outgoing_max_ws_message_size

    @property
    def allow_training(self) -> bool:
        return self.compression.allow_training

    @property
    def compression_min_size(self) -> int:
        return self.compression.compression_min_size

    @property
    def compression_trained_max_size(self) -> int:
        return self.compression.compression_trained_max_size

    @property
    def compression_training_low_watermark(self) -> int:
        return self.compression.compression_training_low_watermark

    async def train_compression_dict_low_watermark(
        self, /, samples: List[bytes]
    ) -> "Tuple[zstandard.ZstdCompressionDict, int]":
        return await self.compression.train_compression_dict_low_watermark(samples)

    @property
    def compression_training_high_watermark(self) -> int:
        return self.compression.compression_training_high_watermark

    async def train_compression_dict_high_watermark(
        self, /, samples: List[bytes]
    ) -> "Tuple[zstandard.ZstdCompressionDict, int]":
        return await self.compression.train_compression_dict_high_watermark(samples)

    @property
    def compression_retrain_interval_seconds(self) -> int:
        return self.compression.compression_retrain_interval_seconds

    @property
    def decompression_max_window_size(self) -> int:
        return self.compression.decompression_max_window_size

    @property
    def websocket_minimal_headers(self) -> bool:
        return self.generic.websocket_minimal_headers

    @property
    def sweep_missed_interval(self) -> float:
        return self.generic.sweep_missed_interval


if TYPE_CHECKING:
    _a: Type[GenericConfig] = GenericConfigFromValues
    _b: Type[CompressionConfig] = CompressionConfigFromParts
    _c: Type[MissedRetryConfig] = MissedRetryStandard
    _d: Type[Config] = ConfigFromParts
