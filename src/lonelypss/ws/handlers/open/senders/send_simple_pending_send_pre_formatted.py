from typing import TYPE_CHECKING

from lonelypss.ws.handlers.open.senders.protocol import Sender
from lonelypss.ws.state import SimplePendingSendPreFormatted, StateOpen


async def send_simple_pending_send_pre_formatted(
    state: StateOpen, message: SimplePendingSendPreFormatted
) -> None:
    """Sends the message to the ASGI server for forwarding"""
    await state.websocket.send_bytes(message.data)


if TYPE_CHECKING:
    _: Sender[SimplePendingSendPreFormatted] = send_simple_pending_send_pre_formatted
