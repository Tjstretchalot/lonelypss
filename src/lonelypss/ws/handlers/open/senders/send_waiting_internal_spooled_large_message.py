from typing import TYPE_CHECKING

from lonelypss.ws.handlers.open.send_receive_stream import send_receive_stream
from lonelypss.ws.handlers.open.senders.protocol import Sender
from lonelypss.ws.state import StateOpen, WaitingInternalSpooledLargeMessage


async def send_waiting_internal_spooled_large_message(
    state: StateOpen, message: WaitingInternalSpooledLargeMessage
) -> None:
    """Sends the message to the ASGI server for forwarding"""
    try:
        await send_receive_stream(
            state,
            message.stream,
            topic=message.topic,
            uncompressed_sha512=message.sha512,
            uncompressed_length=message.length,
            maybe_store_for_training=True,
            read_lock=None,
        )
    finally:
        message.stream.close()


if TYPE_CHECKING:
    _: Sender[WaitingInternalSpooledLargeMessage] = (
        send_waiting_internal_spooled_large_message
    )
