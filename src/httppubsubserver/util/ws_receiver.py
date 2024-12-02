from typing import Protocol
from httppubsubserver.util.sync_io import SyncReadableBytesIO


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
    ) -> None:
        """Handles the incoming message on the given topic. It can be assumed
        the stream can be consumed quickly and is not being consumed
        concurrently, but it must not be closed, as the caller may afterward
        seek back to the beginning and reuse the stream.

        The implementation should try to return quickly in all circumstances. For
        example, if the goal is to write the data to a websocket, there should be
        a timeout on a send after which point you copy whatever is remaining to
        a place you control then return back so the stream can be used by the next
        receiver (if any)
        """

    async def on_small_incoming(
        self,
        data: bytes,
        /,
        *,
        topic: bytes,
        sha512: bytes,
    ) -> None:
        """Handles the incoming message on the given topic. This is used if
        the data is small enough that spooling is not necessary which allows
        us to concurrently call this function safely (since all the arguments
        are immutable)
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
    """

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
