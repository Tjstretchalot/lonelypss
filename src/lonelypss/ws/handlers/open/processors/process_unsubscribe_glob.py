import time
from typing import TYPE_CHECKING

from lonelypsp.auth.config import AuthResult
from lonelypsp.stateful.constants import BroadcasterToSubscriberStatefulMessageType
from lonelypsp.stateful.messages.confirm_unsubscribe import (
    B2S_ConfirmUnsubscribeGlob,
    serialize_b2s_confirm_unsubscribe_glob,
)
from lonelypsp.stateful.messages.unsubscribe import S2B_UnsubscribeGlob

from lonelypss.ws.handlers.open.errors import AuthRejectedException
from lonelypss.ws.handlers.open.processors.protocol import S2B_MessageProcessor
from lonelypss.ws.handlers.open.send_simple_asap import send_simple_asap
from lonelypss.ws.handlers.open.websocket_url import (
    make_for_receive_websocket_url_and_change_counter,
)
from lonelypss.ws.state import StateOpen


async def process_unsubscribe_glob(
    state: StateOpen, message: S2B_UnsubscribeGlob
) -> None:
    """Processes a request by the subscriber to unsubscribe from utf-8 decodable
    topics which match the given glob pattern, given that they subscribed to this
    exact glob pattern before.
    """
    url = make_for_receive_websocket_url_and_change_counter(state)
    auth_at = time.time()
    auth_result = await state.broadcaster_config.is_subscribe_glob_allowed(
        tracing=message.tracing,
        url=url,
        recovery=None,
        glob=message.glob,
        now=auth_at,
        authorization=message.authorization,
    )
    if auth_result != AuthResult.OK:
        raise AuthRejectedException(f"subscribe exact: {auth_result}")

    for idx, (_, glob) in enumerate(state.my_receiver.glob_subscriptions):
        if glob == message.glob:
            break
    else:
        raise Exception("not subscribed to glob pattern")

    resp_tracing = b""  # TODO: tracing
    # TODO: not safe to use a different url here I believe, because we would need to
    # ensure nothing else gets queued to be sent between now and the send actually
    # being queued, which would require being in a send_task, not a process_task
    resp_authorization = (
        await state.broadcaster_config.authorize_confirm_subscribe_glob(
            tracing=resp_tracing,
            url=url,
            recovery=None,
            glob=message.glob,
            now=time.time(),
        )
    )

    state.my_receiver.glob_subscriptions.pop(idx)
    await state.internal_receiver.decrement_glob(message.glob)
    send_simple_asap(
        state,
        serialize_b2s_confirm_unsubscribe_glob(
            B2S_ConfirmUnsubscribeGlob(
                type=BroadcasterToSubscriberStatefulMessageType.CONFIRM_UNSUBSCRIBE_GLOB,
                glob=message.glob,
                authorization=resp_authorization,
                tracing=resp_tracing,
            ),
            minimal_headers=state.broadcaster_config.websocket_minimal_headers,
        ),
    )


if TYPE_CHECKING:
    _: S2B_MessageProcessor[S2B_UnsubscribeGlob] = process_unsubscribe_glob
