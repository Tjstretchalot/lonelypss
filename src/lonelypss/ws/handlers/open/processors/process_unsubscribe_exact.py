import time
from typing import TYPE_CHECKING

from lonelypsp.stateful.constants import BroadcasterToSubscriberStatefulMessageType
from lonelypsp.stateful.messages.confirm_unsubscribe import (
    B2S_ConfirmUnsubscribeExact,
    serialize_b2s_confirm_unsubscribe_exact,
)
from lonelypsp.stateful.messages.unsubscribe import S2B_UnsubscribeExact

from lonelypss.ws.handlers.open.errors import AuthRejectedException
from lonelypss.ws.handlers.open.processors.protocol import S2B_MessageProcessor
from lonelypss.ws.handlers.open.send_simple_asap import send_simple_asap
from lonelypss.ws.handlers.open.websocket_url import (
    make_for_receive_websocket_url_and_change_counter,
)
from lonelypss.ws.state import StateOpen


async def process_unsubscribe_exact(
    state: StateOpen, message: S2B_UnsubscribeExact
) -> None:
    """Processes a request by the subscriber to unsubscribe from a specific topic,
    no longer receiving notifications within this websocket
    """
    url = make_for_receive_websocket_url_and_change_counter(state)
    auth_at = time.time()
    auth_result = await state.broadcaster_config.is_subscribe_exact_allowed(
        url=url,
        recovery=None,
        exact=message.topic,
        now=auth_at,
        authorization=message.authorization,
    )
    if auth_result != "ok":
        raise AuthRejectedException(f"unsubscribe exact: {auth_result}")

    if message.topic not in state.my_receiver.exact_subscriptions:
        raise Exception("not subscribed to exact topic")

    state.my_receiver.exact_subscriptions.remove(message.topic)
    await state.internal_receiver.decrement_exact(message.topic)
    send_simple_asap(
        state,
        serialize_b2s_confirm_unsubscribe_exact(
            B2S_ConfirmUnsubscribeExact(
                type=BroadcasterToSubscriberStatefulMessageType.CONFIRM_UNSUBSCRIBE_EXACT,
                topic=message.topic,
            ),
            minimal_headers=state.broadcaster_config.websocket_minimal_headers,
        ),
    )


if TYPE_CHECKING:
    _: S2B_MessageProcessor[S2B_UnsubscribeExact] = process_unsubscribe_exact
