from typing import Union

from lonelypss.ws.state import (
    InternalLargeMessage,
    InternalSmallMessage,
    SimplePendingSendPreFormatted,
    StateOpen,
    WaitingInternalSpooledLargeMessage,
)


async def send_any(
    state: StateOpen,
    sendable: Union[
        InternalSmallMessage,
        InternalLargeMessage,
        WaitingInternalSpooledLargeMessage,
        SimplePendingSendPreFormatted,
    ],
) -> None:
    """The target for `send_task` on StateOpen. This will write to the websocket and
    expect that nothing else is doing so.

    When sending an internal large message that is unspooled, it will handle
    setting the finished event as quickly as possible; to facilitate this, it
    will spool the message if it detects its taking too long to push the message
    to the ASGI server.
    """
    raise NotImplementedError
