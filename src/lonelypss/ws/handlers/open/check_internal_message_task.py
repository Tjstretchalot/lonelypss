import asyncio
import io
import tempfile
from typing import Union

from lonelypss.ws.handlers.open.check_result import CheckResult
from lonelypss.ws.handlers.open.senders.send_any import send_any
from lonelypss.ws.state import (
    InternalLargeMessage,
    InternalMessageType,
    InternalSmallMessage,
    StateOpen,
    WaitingInternalMessageType,
    WaitingInternalSpooledLargeMessage,
)


async def check_internal_message_task(state: StateOpen) -> CheckResult:
    """Makes progress using the result of the read task, if possible. Raises
    an exception to indicate that we should begin the cleanup and shutdown
    process
    """
    if not state.internal_message_task.done():
        return CheckResult.CONTINUE

    result = state.internal_message_task.result()
    state.internal_message_task = asyncio.create_task(state.my_receiver.queue.get())

    if state.send_task is None and not state.unsent_messages:
        state.send_task = asyncio.create_task(send_any(state, result))
        return CheckResult.RESTART

    if result.type != InternalMessageType.LARGE:
        state.unsent_messages.append(result)
        return CheckResult.RESTART

    spooled = _spool_large_message(state, result)
    state.unsent_messages.append(spooled)
    return CheckResult.RESTART


def _spool_large_message(
    state: StateOpen, message: InternalLargeMessage
) -> Union[InternalSmallMessage, WaitingInternalSpooledLargeMessage]:
    if message.length == 0:
        message.finished.set()
        return InternalSmallMessage(
            type=InternalMessageType.SMALL,
            topic=message.topic,
            data=b"",
            sha512=message.sha512,
        )

    if message.length < state.broadcaster_config.message_body_spool_size:
        try:
            message_body = message.stream.read(message.length)
        finally:
            message.finished.set()

        if len(message_body) != message.length:
            raise ValueError(
                f"expected {message.length} bytes, got {len(message_body)}"
            )

        return InternalSmallMessage(
            type=InternalMessageType.SMALL,
            topic=message.topic,
            data=message_body,
            sha512=message.sha512,
        )

    tmpfile = tempfile.TemporaryFile()
    try:
        remaining = message.length
        while remaining > 0:
            chunk = message.stream.read(min(io.DEFAULT_BUFFER_SIZE, remaining))
            if not chunk:
                raise ValueError(f"unexpected end of stream ({remaining} bytes left)")

            tmpfile.write(chunk)
            remaining -= len(chunk)

        if remaining < 0:
            raise ValueError(f"read too many bytes ({remaining} left)")

        tmpfile.seek(0)
        return WaitingInternalSpooledLargeMessage(
            type=WaitingInternalMessageType.SPOOLED_LARGE,
            stream=tmpfile,
            length=message.length,
            topic=message.topic,
            sha512=message.sha512,
        )
    except BaseException:
        tmpfile.close()
        raise
    finally:
        message.finished.set()
