import time
from typing import TYPE_CHECKING

from lonelypsp.stateful.constants import BroadcasterToSubscriberStatefulMessageType
from lonelypsp.stateful.messages.enable_zstd_custom import (
    B2S_EnableZstdCustom,
    serialize_b2s_enable_zstd_custom,
)

from lonelypss.ws.handlers.open.senders.protocol import Sender
from lonelypss.ws.handlers.open.websocket_url import (
    make_for_send_websocket_url_and_change_counter,
)
from lonelypss.ws.state import SimplePendingSendEnableZstdCustom, StateOpen


async def send_enable_zstd_custom(
    state: StateOpen, message: SimplePendingSendEnableZstdCustom
) -> None:
    """Sends the message to the ASGI server for forwarding"""
    # we need this in send lock because theres an event loop between url and sending
    # the message, and during that time it's important nothing else produces a send
    # url
    url = make_for_send_websocket_url_and_change_counter(state)
    tracing = b""  # TODO: tracing
    authorization = (
        await state.broadcaster_config.authorize_stateful_enable_zstd_custom(
            tracing=tracing,
            url=url,
            compressor_identifier=message.identifier,
            compression_level=message.compression_level,
            min_size=message.min_size,
            max_size=message.max_size,
            sha512=message.sha512,
            now=time.time(),
        )
    )
    data = serialize_b2s_enable_zstd_custom(
        B2S_EnableZstdCustom(
            type=BroadcasterToSubscriberStatefulMessageType.ENABLE_ZSTD_CUSTOM,
            identifier=message.identifier,
            compression_level=message.compression_level,
            min_size=message.min_size,
            max_size=message.max_size,
            dictionary=message.dictionary,
            authorization=authorization,
            tracing=tracing,
            sha512=message.sha512,
        ),
        minimal_headers=state.broadcaster_config.websocket_minimal_headers,
    )
    await state.websocket.send_bytes(data)


if TYPE_CHECKING:
    _: Sender[SimplePendingSendEnableZstdCustom] = send_enable_zstd_custom
