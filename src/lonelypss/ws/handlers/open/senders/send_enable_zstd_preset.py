import time
from typing import TYPE_CHECKING

from lonelypsp.stateful.constants import BroadcasterToSubscriberStatefulMessageType
from lonelypsp.stateful.messages.enable_zstd_preset import (
    B2S_EnableZstdPreset,
    serialize_b2s_enable_zstd_preset,
)

from lonelypss.ws.handlers.open.senders.protocol import Sender
from lonelypss.ws.handlers.open.websocket_url import (
    make_for_send_websocket_url_and_change_counter,
)
from lonelypss.ws.state import SimplePendingSendEnableZstdPreset, StateOpen


async def send_enable_zstd_preset(
    state: StateOpen, message: SimplePendingSendEnableZstdPreset
) -> None:
    """Sends the message to the ASGI server for forwarding"""
    # we need this in send lock because theres an event loop between url and sending
    # the message, and during that time it's important nothing else produces a send
    # url
    url = make_for_send_websocket_url_and_change_counter(state)
    tracing = b""  # TODO: tracing
    authorization = (
        await state.broadcaster_config.authorize_stateful_enable_zstd_preset(
            tracing=tracing,
            url=url,
            compressor_identifier=message.identifier,
            compression_level=message.compression_level,
            min_size=message.min_size,
            max_size=message.max_size,
            now=time.time(),
        )
    )
    data = serialize_b2s_enable_zstd_preset(
        B2S_EnableZstdPreset(
            type=BroadcasterToSubscriberStatefulMessageType.ENABLE_ZSTD_PRESET,
            identifier=message.identifier,
            compression_level=message.compression_level,
            min_size=message.min_size,
            max_size=message.max_size,
            authorization=authorization,
            tracing=tracing,
        ),
        minimal_headers=state.broadcaster_config.websocket_minimal_headers,
    )
    await state.websocket.send_bytes(data)


if TYPE_CHECKING:
    _: Sender[SimplePendingSendEnableZstdPreset] = send_enable_zstd_preset
