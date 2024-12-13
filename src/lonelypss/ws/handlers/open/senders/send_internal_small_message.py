import io
from typing import TYPE_CHECKING

from lonelypss.ws.handlers.open.collector_utils import (
    maybe_store_small_message_for_training,
)
from lonelypss.ws.handlers.open.send_receive_stream import (
    send_receive_stream,
)
from lonelypss.ws.handlers.open.senders.protocol import Sender
from lonelypss.ws.state import (
    InternalSmallMessage,
    StateOpen,
)


async def send_internal_small_message(
    state: StateOpen, message: InternalSmallMessage
) -> None:
    """Notifies the client about the small message that was sent to a topic
    they are subscribed to. Note that although the broadcaster kept the
    entire message in memory it may still be necessary to split the message
    when sending over the websocket
    """
    maybe_store_small_message_for_training(state, message.data)
    await send_receive_stream(
        state,
        io.BytesIO(message.data),
        topic=message.topic,
        uncompressed_sha512=message.sha512,
        uncompressed_length=len(message.data),
        maybe_store_for_training=False,
        read_lock=None,
    )


if TYPE_CHECKING:
    _: Sender[InternalSmallMessage] = send_internal_small_message
