import asyncio
import dataclasses
import io
import os
import re
from dataclasses import dataclass
from tempfile import SpooledTemporaryFile, TemporaryFile
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Protocol,
    Type,
    cast,
)

from lonelypsp.compat import fast_dataclass

from lonelypss.config.config import DBConfig
from lonelypss.util.sync_io import (
    SyncIOBaseLikeIO,
    SyncReadableBytesIO,
)
from lonelypss.ws.handlers.open.errors import combine_multiple_exceptions

try:
    from glob import translate as _glob_translate  # type: ignore

    def translate(pat: str) -> str:
        return _glob_translate(pat, recursive=True, include_hidden=True)

except ImportError:
    from fnmatch import translate


class BaseWSReceiver(Protocol):
    """Something capable of processing a received message only after it has
    been verified and is ready for rapid consumption (e.g., in memory or on
    a local disk)
    """

    def is_relevant(self, topic: bytes) -> bool:
        """Allows the caller to skip calls to on_large_exclusive_incoming and
        on_small_incoming if the topic is not relevant to this receiver. If
        async is required for this check, it must be done in the implementation
        of on_large_exclusive_incoming and on_small_incoming instead of here.
        """
        ...

    async def on_large_exclusive_incoming(
        self,
        stream: SyncReadableBytesIO,
        /,
        *,
        topic: bytes,
        sha512: bytes,
        length: int,
    ) -> int:
        """Handles the incoming message on the given topic. It can be assumed
        the stream can be consumed quickly and is not being consumed
        concurrently, but it must not be closed, as the caller may afterward
        seek back to the beginning and reuse the stream.

        The implementation should try to return quickly in all circumstances. For
        example, if the goal is to write the data to a websocket, there should be
        a timeout on a send after which point you copy whatever is remaining to
        a place you control then return back so the stream can be used by the next
        receiver (if any)

        Returns the number of subscribers that were forwarded the message
        """

    async def on_small_incoming(
        self,
        data: bytes,
        /,
        *,
        topic: bytes,
        sha512: bytes,
    ) -> int:
        """Handles the incoming message on the given topic. This is used if
        the data is small enough that spooling is not necessary which allows
        us to concurrently call this function safely (since all the arguments
        are immutable)

        Returns the number of subscribers that were forwarded the message
        """

    async def on_missed(
        self,
        /,
        *,
        topic: bytes,
    ) -> int:
        """Handles that this receiver may have missed some messages on the
        given topic

        Returns the number of subscribers that were forwarded the message
        """


class FanoutWSReceiver(BaseWSReceiver, Protocol):
    """Deduplicates messages and forwards onto the receivers. Note that because
    of this deduplication step, the notifications received over the websocket
    connection are different than those received over the http connection when
    using overlapping globs. This is because the deduplication overhead cannot
    be avoided within the broadcaster in order to avoid receiving multiple
    notifications when multiple websockets are using overlapping globs, which
    is much more reasonable/likely than a single subscriber using overlapping
    globs. It would then take even more effort to reduplicate these messages
    to replicate the one-subscriber scenario.

    A simple async-safe implementation is provided via SimpleFanoutWSReceiver
    """

    @property
    def receiver_url(self) -> str:
        """The URL that this receiver expects to receive messages from"""

    @property
    def missed_url(self) -> Optional[str]:
        """The URL that this receiver expects to receive missed messages from"""

    async def register_receiver(self, receiver: BaseWSReceiver) -> int:
        """Registers a receiver to receive messages from this fanout receiver.
        Returns the id that can be passed to `unregister_receiver`
        """

    async def unregister_receiver(self, receiver_id: int) -> None:
        """Unregisters a receiver from receiving messages from this fanout receiver"""

    async def increment_exact(self, topic: bytes, /) -> None:
        """Increments the count of internal for the given exact topic. If this
        causes the first subscriber to be added, must register us as a local subscriber
        """

    async def decrement_exact(self, topic: bytes, /) -> None:
        """Decrements the count of internal for the given exact topic. If this
        causes the last subscriber to be removed, must unregister us as a local subscriber
        """

    async def increment_glob(self, glob: str, /) -> None:
        """Increments the count of internal for the given glob topic. If this
        causes the first subscriber to be added, must register us as a local subscriber
        """

    async def decrement_glob(self, glob: str, /) -> None:
        """Decrements the count of internal for the given glob topic. If this
        causes the last subscriber to be removed, must unregister us as a local subscriber
        """


@dataclass
class ReceiverNode:
    """the linked-list node for SimpleFanoutWSReceiver"""

    previous: Optional["ReceiverNode"]
    """the previous node or None if this is the head of the list"""
    next: Optional["ReceiverNode"]
    """the next node or None if this is the tail of the list"""
    receiver: BaseWSReceiver
    """the receiver"""
    identifier: int
    """the arbitrary identifier assigned by the fanout receiver"""


@fast_dataclass
class GlobSubscription:
    glob: str
    """the original glob pattern"""
    pattern: re.Pattern
    """the regex that can be used to test the glob pattern"""
    count: int
    """the number of subscribers to this glob pattern"""


class SimpleFanoutWSReceiver:
    """Acts as an async context manager, generally should be initialized as
    a global object but then entered within the appropriate lifespan function.

    Allows subscribing before entering, if desired, and will combine errors
    in that case
    """

    def __init__(
        self, receiver_url: str, recovery: Optional[str], db: DBConfig
    ) -> None:
        self.receiver_url = receiver_url
        """the url we can and expect to receive messages on, i.e, how the
        other broadcasters can reach this broadcaster. Typically,
        `http://<broadcaster_ip>:<broadcaster_port>/v1/receive_for_websockets`
        """
        self.recovery = recovery
        """the url we can and expect to receive missed messages on, i.e, how the
        other broadcasters tell this broadcaster they previously failed to reach
        them; None to indicate we are not requesting MISSED messages
        """
        self.receivers_head: Optional[ReceiverNode] = None
        """the receivers are stored as a double-linked list with a dict
        lookup by id; this points to the head of the list
        """
        self.receivers_tail: Optional[ReceiverNode] = None
        """the receivers are stored as a double-linked list with a dict
        lookup by id; this points to the tail of the list
        """
        self.receivers_list: Optional[List[BaseWSReceiver]] = None
        """if a list of just the base receivers has been materialized to speedup
        iteration, that list, otherwise None.
        """
        self.receivers_iter_counter: int = 0
        """how many times the receivers list has been iterated over since it was
        last mutated; after a threshold which is based on the length of the list,
        i.e., the cost to materialize, materializes the list to speedup iteration
        """
        self.receivers_id_counter: int = 0
        """the counter for assigning unique identifiers to receivers"""
        self.receivers_by_id: Dict[int, ReceiverNode] = dict()
        """a lookup from the identifier assigned to a receiver to the corresponding reciever"""
        self.exact_subscriptions: Dict[bytes, int] = dict()
        """the exact subscriptions that have been registered, mapped to the current
        count, which when it hits 0 means the subscription can be removed
        """
        self.glob_subscriptions: Dict[str, GlobSubscription] = dict()
        """the glob subscriptions that have been registered, mapped to the corresponding
        information about the subscription, including the count
        """
        self.db = db
        """the database connection for telling other broadcasters to send messages to us;
        assumed to be setup by something else whenever this object is entered. Assumed
        to be async-safe so this does not need to coordinate with other users
        """
        self.entered: bool = False
        """True if entered, false if not"""
        self._lock: asyncio.Lock = asyncio.Lock()
        """the lock guarding mutations to receivers, exact_subscriptions, and glob_subscriptions"""

    @property
    def missed_url(self) -> Optional[str]:
        return self.recovery

    async def __aenter__(self) -> "SimpleFanoutWSReceiver":
        async with self._lock:
            assert not self.entered, "already entered and not reentrant safe"

            try_unsubscribes: List[bytes] = []
            try_globs: List[str] = []

            try:
                for topic in self.exact_subscriptions:
                    try_unsubscribes.append(topic)
                    result = await self.db.subscribe_exact(
                        url=self.receiver_url, recovery=self.recovery, exact=topic
                    )
                    if result != "success" and result != "conflict":
                        raise ValueError(f"failed to subscribe to {topic!r}: {result}")

                for glob in self.glob_subscriptions.keys():
                    try_globs.append(glob)
                    result = await self.db.subscribe_glob(
                        url=self.receiver_url, recovery=self.recovery, glob=glob
                    )
                    if result != "success" and result != "conflict":
                        raise ValueError(f"failed to subscribe to {glob}: {result}")

            except BaseException as exc:
                exceptions: List[BaseException] = [exc]

                for topic in try_unsubscribes:
                    try:
                        unsub_res = await self.db.unsubscribe_exact(
                            url=self.receiver_url, exact=topic
                        )
                        if unsub_res != "success" and unsub_res != "not_found":
                            raise ValueError(
                                f"failed to unsubscribe from {topic!r}: {result}"
                            )
                    except BaseException as exc:
                        exceptions.append(exc)

                for glob in try_globs:
                    try:
                        unsub_res = await self.db.unsubscribe_glob(
                            url=self.receiver_url, glob=glob
                        )
                        if unsub_res != "success" and unsub_res != "not_found":
                            raise ValueError(
                                f"failed to unsubscribe from {glob}: {result}"
                            )
                    except BaseException as exc:
                        exceptions.append(exc)

                raise combine_multiple_exceptions(
                    "failed to setup subscriptions", exceptions
                )

            self.entered = True
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        async with self._lock:
            assert self.entered, "not entered and not reentrant safe"

            try_unsubscribes: List[bytes] = list(self.exact_subscriptions)
            try_globs: List[str] = list(glob for glob in self.glob_subscriptions.keys())

            self.exact_subscriptions.clear()
            self.glob_subscriptions.clear()

            exceptions: List[BaseException] = []

            for topic in try_unsubscribes:
                try:
                    result = await self.db.unsubscribe_exact(
                        url=self.receiver_url, exact=topic
                    )
                    if result != "success" and result != "not_found":
                        raise ValueError(
                            f"failed to unsubscribe from {topic!r}: {result}"
                        )
                except BaseException as exc:
                    exceptions.append(exc)

            for glob in try_globs:
                try:
                    result = await self.db.unsubscribe_glob(
                        url=self.receiver_url, glob=glob
                    )
                    if result != "success" and result != "not_found":
                        raise ValueError(f"failed to unsubscribe from {glob}: {result}")
                except BaseException as exc:
                    exceptions.append(exc)

            self.entered = False
            if exceptions:
                raise combine_multiple_exceptions(
                    "failed to teardown subscriptions", exceptions
                )

    async def register_receiver(self, receiver: BaseWSReceiver) -> int:
        async with self._lock:
            identifier = self.receivers_id_counter
            self.receivers_id_counter += 1
            self.receivers_list = None
            self.receivers_iter_counter = 0

            node = ReceiverNode(
                previous=self.receivers_tail,
                next=None,
                receiver=receiver,
                identifier=identifier,
            )
            self.receivers_by_id[identifier] = node

            if self.receivers_head is None:
                assert self.receivers_tail is None
                self.receivers_head = node
                self.receivers_tail = node
                return identifier

            assert self.receivers_tail is not None
            self.receivers_tail.next = node
            self.receivers_tail = node
            return identifier

    async def unregister_receiver(self, receiver_id: int) -> None:
        async with self._lock:
            node = self.receivers_by_id.pop(receiver_id)
            self.receivers_list = None
            self.receivers_iter_counter = 0

            # case 1: node is the only node
            if node.previous is None and node.next is None:
                assert self.receivers_head is node
                assert self.receivers_tail is node
                self.receivers_head = None
                self.receivers_tail = None
                return

            # case 2: node is the head, multiple nodes
            if node.previous is None:
                assert node.next is not None
                assert self.receivers_head is node
                self.receivers_head = node.next
                self.receivers_head.previous = None
                return

            # case 3: node is the tail, multiple nodes
            if node.next is None:
                assert node.previous is not None
                assert self.receivers_tail is node
                self.receivers_tail = node.previous
                self.receivers_tail.next = None
                return

            # case 4: node is in the middle, multiple nodes
            node.previous.next = node.next
            node.next.previous = node.previous
            return

    def iter_receivers(self) -> Iterable[BaseWSReceiver]:
        """Returns an iterator over the receivers; this will use a list
        if its available or if enough iterations have been started for it
        to be worth it, otherwise it will iterate over the linked-list
        """
        if self.receivers_list is not None:
            return self.receivers_list
        slow_iterable = (v.receiver for v in self.receivers_by_id.values())
        self.receivers_iter_counter += 1
        if self.receivers_iter_counter > len(self.receivers_by_id):
            self.receivers_list = list(slow_iterable)
            return self.receivers_list
        return slow_iterable

    async def increment_exact(self, topic: bytes, /) -> None:
        async with self._lock:
            old_count = self.exact_subscriptions.get(topic, 0)
            if old_count > 0:
                self.exact_subscriptions[topic] = old_count + 1
                return

            self.exact_subscriptions[topic] = 1
            if not self.entered:
                return

            try:
                result = await self.db.subscribe_exact(
                    url=self.receiver_url, recovery=self.recovery, exact=topic
                )
                if result != "success" and result != "conflict":
                    raise ValueError(f"failed to subscribe to {topic!r}: {result}")
            except BaseException:
                unsub_result = await self.db.unsubscribe_exact(
                    url=self.receiver_url, exact=topic
                )
                if unsub_result != "success" and unsub_result != "not_found":
                    raise ValueError(
                        f"failed to unsubscribe from {topic!r}: {unsub_result}"
                    )
                del self.exact_subscriptions[topic]
                raise

    async def decrement_exact(self, topic: bytes, /) -> None:
        async with self._lock:
            old_count = self.exact_subscriptions.get(topic, 0)
            if old_count == 0:
                return

            if old_count > 1:
                self.exact_subscriptions[topic] = old_count - 1
                return

            del self.exact_subscriptions[topic]
            if not self.entered:
                return

            result = await self.db.unsubscribe_exact(url=self.receiver_url, exact=topic)
            if result != "success" and result != "not_found":
                raise ValueError(f"failed to unsubscribe from {topic!r}: {result}")

    async def increment_glob(self, glob: str, /) -> None:
        async with self._lock:
            subscription = self.glob_subscriptions.get(glob)
            if subscription is not None:
                self.glob_subscriptions[glob] = dataclasses.replace(
                    subscription, count=subscription.count + 1
                )
                return

            pattern = re.compile(translate(glob))
            subscription = GlobSubscription(glob=glob, pattern=pattern, count=1)
            self.glob_subscriptions[glob] = subscription
            if not self.entered:
                return

            try:
                result = await self.db.subscribe_glob(
                    url=self.receiver_url, recovery=self.recovery, glob=glob
                )
                if result != "success" and result != "conflict":
                    raise ValueError(f"failed to subscribe to {glob!r}: {result}")
            except BaseException:
                unsub_result = await self.db.unsubscribe_glob(
                    url=self.receiver_url, glob=glob
                )
                if unsub_result != "success" and unsub_result != "not_found":
                    raise ValueError(
                        f"failed to unsubscribe from {glob!r}: {unsub_result}"
                    )
                del self.glob_subscriptions[glob]
                raise

    async def decrement_glob(self, glob: str, /) -> None:
        async with self._lock:
            subscription = self.glob_subscriptions.get(glob)
            if subscription is None:
                return

            if subscription.count > 1:
                self.glob_subscriptions[glob] = dataclasses.replace(
                    subscription, count=subscription.count - 1
                )
                return

            del self.glob_subscriptions[glob]
            if not self.entered:
                return

            result = await self.db.unsubscribe_glob(url=self.receiver_url, glob=glob)
            if result != "success" and result != "not_found":
                raise ValueError(f"failed to unsubscribe from {glob!r}: {result}")

    def is_relevant(self, topic: bytes) -> bool:
        if topic in self.exact_subscriptions:
            return True

        try:
            topic_str = topic.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            return False

        return any(
            glob.pattern.match(topic_str) for glob in self.glob_subscriptions.values()
        )

    async def on_large_exclusive_incoming(
        self,
        stream: SyncReadableBytesIO,
        /,
        *,
        topic: bytes,
        sha512: bytes,
        length: int,
    ) -> int:
        is_tellable: bool
        is_seekable: bool

        if isinstance(stream, (io.IOBase, SpooledTemporaryFile)):
            is_tellable = True
            is_seekable = True
        else:
            has_tell = hasattr(stream, "tell") and callable(
                cast(SyncIOBaseLikeIO, stream).tell
            )
            has_tellable = hasattr(stream, "tellable") and callable(
                cast(Any, stream).tellable
            )
            is_tellable = has_tell and (
                (not has_tellable) or cast(Any, stream).tellable()
            )

            has_seek = hasattr(stream, "seek") and callable(
                cast(SyncIOBaseLikeIO, stream).seek
            )
            has_seekable = hasattr(stream, "seekable") and callable(
                cast(Any, stream).seekable
            )
            is_seekable = has_seek and (
                (not has_seekable) or cast(Any, stream).seekable()
            )

        need_close: bool
        std_io: SyncIOBaseLikeIO

        if is_tellable and is_seekable:
            need_close = False
            std_io = cast(SyncIOBaseLikeIO, stream)
        else:
            need_close = True
            std_io = TemporaryFile("w+b")
            try:
                while True:
                    data = stream.read(io.DEFAULT_BUFFER_SIZE)
                    if not data:
                        break
                    std_io.write(data)
                std_io.seek(0, os.SEEK_SET)
            except BaseException:
                std_io.close()
                raise

        try:
            start_pos = std_io.tell()
            count = 0
            for receiver in self.iter_receivers():
                if not receiver.is_relevant(topic):
                    continue
                std_io.seek(start_pos, os.SEEK_SET)
                count += await receiver.on_large_exclusive_incoming(
                    stream, topic=topic, sha512=sha512, length=length
                )
            return count
        finally:
            if need_close:
                std_io.close()

    async def on_small_incoming(
        self,
        data: bytes,
        /,
        *,
        topic: bytes,
        sha512: bytes,
    ) -> int:
        count = 0
        for receiver in self.iter_receivers():
            if not receiver.is_relevant(topic):
                continue
            count += await receiver.on_small_incoming(data, topic=topic, sha512=sha512)
        return count

    async def on_missed(
        self,
        /,
        *,
        topic: bytes,
    ) -> int:
        count = 0
        for receiver in self.iter_receivers():
            if not receiver.is_relevant(topic):
                continue
            count += await receiver.on_missed(topic=topic)
        return count


if TYPE_CHECKING:
    _: Type[FanoutWSReceiver] = SimpleFanoutWSReceiver
