import asyncio
import re
from typing import TYPE_CHECKING, List, Optional, Set, Tuple, Type

from lonelypss.util.sync_io import SyncReadableBytesIO
from lonelypss.ws.state import (
    AsyncioWSReceiver,
    InternalLargeMessage,
    InternalMessage,
    InternalMessageType,
    InternalMissedMessage,
    InternalSmallMessage,
)


class SimpleReceiver:
    def __init__(self) -> None:
        self.exact_subscriptions: Set[bytes] = set()
        self.glob_subscriptions: List[Tuple[re.Pattern, str]] = []
        self.receiver_id: Optional[int] = None

        self.queue: asyncio.Queue[InternalMessage] = asyncio.Queue()

    def is_relevant(self, topic: bytes) -> bool:
        if topic in self.exact_subscriptions:
            return True

        try:
            topic_str = topic.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            return False

        return any(pattern.match(topic_str) for pattern, _ in self.glob_subscriptions)

    async def on_large_exclusive_incoming(
        self,
        stream: SyncReadableBytesIO,
        /,
        *,
        topic: bytes,
        sha512: bytes,
        length: int,
    ) -> int:
        finished = asyncio.Event()
        await self.queue.put(
            InternalLargeMessage(
                type=InternalMessageType.LARGE,
                stream=stream,
                length=length,
                finished=finished,
                topic=topic,
                sha512=sha512,
            )
        )
        await finished.wait()
        return 1

    async def on_small_incoming(
        self,
        data: bytes,
        /,
        *,
        topic: bytes,
        sha512: bytes,
    ) -> int:
        await self.queue.put(
            InternalSmallMessage(
                type=InternalMessageType.SMALL, topic=topic, data=data, sha512=sha512
            )
        )
        return 1

    async def on_missed(self, /, *, topic: bytes) -> int:
        await self.queue.put(
            InternalMissedMessage(type=InternalMessageType.MISSED, topic=topic)
        )
        return 1


if TYPE_CHECKING:
    _: Type[AsyncioWSReceiver] = SimpleReceiver
