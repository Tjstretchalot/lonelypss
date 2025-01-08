import re
import time
from typing import TYPE_CHECKING

from lonelypsp.stateful.constants import BroadcasterToSubscriberStatefulMessageType
from lonelypsp.stateful.messages.confirm_subscribe import (
    B2S_ConfirmSubscribeGlob,
    serialize_b2s_confirm_subscribe_glob,
)
from lonelypsp.stateful.messages.subscribe import S2B_SubscribeGlob

from lonelypss.ws.handlers.open.errors import AuthRejectedException
from lonelypss.ws.handlers.open.processors.protocol import S2B_MessageProcessor
from lonelypss.ws.handlers.open.send_simple_asap import send_simple_asap
from lonelypss.ws.handlers.open.websocket_url import (
    make_for_receive_websocket_url_and_change_counter,
)
from lonelypss.ws.state import StateOpen

try:
    from glob import translate as _glob_translate  # type: ignore

    def translate(pat: str) -> str:
        return _glob_translate(pat, recursive=True, include_hidden=True)

except ImportError:
    from fnmatch import translate


async def process_subscribe_glob(state: StateOpen, message: S2B_SubscribeGlob) -> None:
    """Processes a request by the subscriber to subscribe to utf-8 decodable
    topics which match the given glob pattern, receiving notifications within
    this websocket
    """
    url = make_for_receive_websocket_url_and_change_counter(state)
    auth_at = time.time()
    auth_result = await state.broadcaster_config.is_subscribe_glob_allowed(
        url=url,
        recovery=None,
        glob=message.glob,
        now=auth_at,
        authorization=message.authorization,
    )
    if auth_result != "ok":
        raise AuthRejectedException(f"subscribe exact: {auth_result}")

    if any(message.glob == glob for _, glob in state.my_receiver.glob_subscriptions):
        raise Exception("already subscribed to glob pattern")

    glob_regex = re.compile(translate(message.glob))

    # note we confirm before registering to ensure they don't receive notifications
    # on the topic before its been confirmed
    send_simple_asap(
        state,
        serialize_b2s_confirm_subscribe_glob(
            B2S_ConfirmSubscribeGlob(
                type=BroadcasterToSubscriberStatefulMessageType.CONFIRM_SUBSCRIBE_GLOB,
                glob=message.glob,
            ),
            minimal_headers=state.broadcaster_config.websocket_minimal_headers,
        ),
    )
    state.my_receiver.glob_subscriptions.append((glob_regex, message.glob))
    await state.internal_receiver.increment_glob(message.glob)


if TYPE_CHECKING:
    _: S2B_MessageProcessor[S2B_SubscribeGlob] = process_subscribe_glob
