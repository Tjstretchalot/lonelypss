from typing import TYPE_CHECKING, Any, Dict, Union, cast

from lonelypss.ws.handlers.open.senders.protocol import Sender
from lonelypss.ws.handlers.open.senders.send_disable_zstd_custom import (
    send_disable_zstd_custom,
)
from lonelypss.ws.handlers.open.senders.send_enable_zstd_custom import (
    send_enable_zstd_custom,
)
from lonelypss.ws.handlers.open.senders.send_enable_zstd_preset import (
    send_enable_zstd_preset,
)
from lonelypss.ws.handlers.open.senders.send_internal_large_message import (
    send_internal_large_message,
)
from lonelypss.ws.handlers.open.senders.send_internal_missed_message import (
    send_internal_missed_message,
)
from lonelypss.ws.handlers.open.senders.send_internal_small_message import (
    send_internal_small_message,
)
from lonelypss.ws.handlers.open.senders.send_simple_pending_send_pre_formatted import (
    send_simple_pending_send_pre_formatted,
)
from lonelypss.ws.handlers.open.senders.send_waiting_internal_spooled_large_message import (
    send_waiting_internal_spooled_large_message,
)
from lonelypss.ws.state import (
    InternalLargeMessage,
    InternalMessageType,
    InternalMissedMessage,
    InternalSmallMessage,
    SimplePendingSendDisableZstdCustom,
    SimplePendingSendEnableZstdCustom,
    SimplePendingSendEnableZstdPreset,
    SimplePendingSendPreFormatted,
    SimplePendingSendType,
    StateOpen,
    WaitingInternalMessageType,
    WaitingInternalSpooledLargeMessage,
)

Sendable = Union[
    InternalSmallMessage,
    InternalMissedMessage,
    InternalLargeMessage,
    WaitingInternalSpooledLargeMessage,
    SimplePendingSendPreFormatted,
    SimplePendingSendEnableZstdPreset,
    SimplePendingSendEnableZstdCustom,
    SimplePendingSendDisableZstdCustom,
]

SENDERS: Dict[Any, Any] = {
    InternalMessageType.SMALL: send_internal_small_message,
    InternalMessageType.MISSED: send_internal_missed_message,
    InternalMessageType.LARGE: send_internal_large_message,
    WaitingInternalMessageType.SPOOLED_LARGE: send_waiting_internal_spooled_large_message,
    SimplePendingSendType.PRE_FORMATTED: send_simple_pending_send_pre_formatted,
    SimplePendingSendType.ENABLE_ZSTD_PRESET: send_enable_zstd_preset,
    SimplePendingSendType.ENABLE_ZSTD_CUSTOM: send_enable_zstd_custom,
    SimplePendingSendType.DISABLE_ZSTD_CUSTOM: send_disable_zstd_custom,
}


async def send_any(
    state: StateOpen,
    message: Sendable,
) -> None:
    """The target for `send_task` on StateOpen. This will write to the websocket and
    expect that nothing else is doing so.

    When sending an internal large message that is unspooled, this will handle
    setting the finished event as quickly as possible; to facilitate this, it
    will spool the message if it detects its taking too long to push the message
    to the ASGI server.
    """
    await cast(Sender[Sendable], SENDERS[message.type])(state, message)


if TYPE_CHECKING:
    _: Sender[Sendable] = send_any
