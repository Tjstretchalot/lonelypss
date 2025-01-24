import time
from typing import TYPE_CHECKING

from lonelypsp.stateful.constants import BroadcasterToSubscriberStatefulMessageType
from lonelypsp.stateful.messages.disable_zstd_custom import (
    B2S_DisableZstdCustom,
    serialize_b2s_disable_zstd_custom,
)

from lonelypss.ws.handlers.open.senders.protocol import Sender
from lonelypss.ws.handlers.open.websocket_url import (
    make_for_send_websocket_url_and_change_counter,
)
from lonelypss.ws.state import SimplePendingSendDisableZstdCustom, StateOpen


async def send_disable_zstd_custom(
    state: StateOpen, message: SimplePendingSendDisableZstdCustom
) -> None:
    """Sends the message to the ASGI server for forwarding"""
    # we need this in send lock because theres an event loop between url and sending
    # the message, and during that time it's important nothing else produces a send
    # url
    url = make_for_send_websocket_url_and_change_counter(state)
    tracing = b""  # TODO: tracing
    authorization = (
        await state.broadcaster_config.authorize_stateful_disable_zstd_custom(
            tracing=tracing,
            compressor_identifier=message.identifier,
            url=url,
            now=time.time(),
        )
    )
    data = serialize_b2s_disable_zstd_custom(
        B2S_DisableZstdCustom(
            type=BroadcasterToSubscriberStatefulMessageType.DISABLE_ZSTD_CUSTOM,
            identifier=message.identifier,
            authorization=authorization,
            tracing=tracing,
        ),
        minimal_headers=state.broadcaster_config.websocket_minimal_headers,
    )
    await state.websocket.send_bytes(data)


if TYPE_CHECKING:
    _: Sender[SimplePendingSendDisableZstdCustom] = send_disable_zstd_custom
