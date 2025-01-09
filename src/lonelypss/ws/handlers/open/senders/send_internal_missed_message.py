import time
from typing import TYPE_CHECKING

from lonelypsp.stateful.constants import BroadcasterToSubscriberStatefulMessageType
from lonelypsp.stateful.messages.missed import B2S_Missed, serialize_b2s_missed

from lonelypss.ws.handlers.open.senders.protocol import Sender
from lonelypss.ws.handlers.open.websocket_url import (
    make_for_send_websocket_url_and_change_counter,
)
from lonelypss.ws.state import (
    InternalMissedMessage,
    StateOpen,
)


async def send_internal_missed_message(
    state: StateOpen, message: InternalMissedMessage
) -> None:
    """Notifies the subscriber that the broadcaster may have failed to
    send them a message on a topic they are subscribed to. The standard
    reason this is sent is if there are multiple broadcasters Alice and Bob,
    the subscriber is connected to Alice, and Bob received a message but
    could not tell Alice about it.
    """
    authorization = await state.broadcaster_config.authorize_missed(
        recovery=make_for_send_websocket_url_and_change_counter(state),
        topic=message.topic,
        now=time.time(),
    )
    await state.websocket.send_bytes(
        serialize_b2s_missed(
            B2S_Missed(
                type=BroadcasterToSubscriberStatefulMessageType.MISSED,
                authorization=authorization,
                topic=message.topic,
            ),
            minimal_headers=state.broadcaster_config.websocket_minimal_headers,
        )
    )


if TYPE_CHECKING:
    _: Sender[InternalMissedMessage] = send_internal_missed_message
