import asyncio
import io
import tempfile
from types import TracebackType
from typing import TYPE_CHECKING, Optional, Type

from lonelypss.util.sync_io import SyncIOBaseLikeIO, SyncReadableBytesIO
from lonelypss.ws.handlers.open.send_receive_stream import send_receive_stream
from lonelypss.ws.handlers.open.senders.protocol import Sender
from lonelypss.ws.state import InternalLargeMessage, StateOpen


async def send_internal_large_message(
    state: StateOpen, message: InternalLargeMessage
) -> None:
    """Sends the message to the ASGI server for forwarding; if it takes too
    long to send it to the ASGI server, copies it to file so that the message
    can be used by the next receiver
    """
    try:
        if message.length == 0:
            message.finished.set()
            await send_receive_stream(
                state,
                io.BytesIO(b""),
                topic=message.topic,
                uncompressed_sha512=message.sha512,
                uncompressed_length=0,
                maybe_store_for_training=False,
                read_lock=None,
            )
            return

        if state.broadcaster_config.websocket_large_direct_send_timeout is None:
            await send_receive_stream(
                state,
                message.stream,
                topic=message.topic,
                uncompressed_sha512=message.sha512,
                uncompressed_length=message.length,
                maybe_store_for_training=True,
                read_lock=None,
            )
            message.finished.set()
            return

        read_lock = asyncio.Lock()
        with _SwappableSyncReadableBytesIO(message.stream, message.length) as stream:
            timeout = asyncio.create_task(
                asyncio.sleep(
                    state.broadcaster_config.websocket_large_direct_send_timeout
                )
            )
            send_task = asyncio.create_task(
                send_receive_stream(
                    state,
                    stream,
                    topic=message.topic,
                    uncompressed_sha512=message.sha512,
                    uncompressed_length=message.length,
                    maybe_store_for_training=True,
                    read_lock=read_lock,
                )
            )

            await asyncio.wait(
                [timeout, send_task], return_when=asyncio.FIRST_COMPLETED
            )

            timeout.cancel()
            if not send_task.done():
                async with read_lock:
                    await stream.swap(
                        spool_size=state.broadcaster_config.message_body_spool_size
                    )
                message.finished.set()
                await send_task
            else:
                message.finished.set()
                send_task.result()
    except BaseException:
        message.finished.set()
        raise


class _SwappableSyncReadableBytesIO:
    def __init__(
        self, original_stream: SyncReadableBytesIO, original_length: int
    ) -> None:
        self._original_stream: Optional[SyncReadableBytesIO] = original_stream
        self._original_remaining: Optional[int] = original_length
        self._swapped_stream: Optional[SyncIOBaseLikeIO] = None

    def __enter__(self) -> "_SwappableSyncReadableBytesIO":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.close()

    async def swap(self, spool_size: int) -> None:
        if self._original_stream is None:
            return

        assert self._original_remaining is not None

        target = tempfile.SpooledTemporaryFile(max_size=spool_size)
        remaining = self._original_remaining
        try:
            while True:
                chunk = self._original_stream.read(
                    min(remaining, io.DEFAULT_BUFFER_SIZE)
                )
                if not chunk:
                    break
                target.write(chunk)
                await asyncio.sleep(0)

            target.seek(0)
            self._swapped_stream = target
        except BaseException:
            target.close()
            raise

    def read(self, n: int) -> bytes:
        if self._swapped_stream is not None:
            result = self._swapped_stream.read(n)
            if not result:
                self._swapped_stream.close()
                self._swapped_stream = None
            return result

        if self._original_stream is None:
            return b""

        assert self._original_remaining is not None
        chunk = self._original_stream.read(min(n, self._original_remaining))
        self._original_remaining -= len(chunk)
        if self._original_remaining == 0:
            self._original_stream = None
            self._original_remaining = None
        return chunk

    def close(self) -> None:
        self._original_stream = None
        self._original_remaining = None
        if self._swapped_stream is not None:
            self._swapped_stream.close()
            self._swapped_stream = None


if TYPE_CHECKING:
    _: Sender[InternalLargeMessage] = send_internal_large_message
