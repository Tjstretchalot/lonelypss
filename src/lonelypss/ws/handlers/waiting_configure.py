import asyncio
import base64
import hashlib
import io
import secrets
import tempfile
import time
from collections import deque
from typing import TYPE_CHECKING, List, cast

import aiohttp
from lonelypsp.stateful.constants import (
    BroadcasterToSubscriberStatefulMessageType,
    SubscriberToBroadcasterStatefulMessageType,
)
from lonelypsp.stateful.messages.configure import S2B_ConfigureParser
from lonelypsp.stateful.messages.confirm_configure import (
    B2S_ConfirmConfigure,
    serialize_b2s_confirm_configure,
)
from lonelypsp.stateful.parser_helpers import parse_s2b_message_prefix

from lonelypss.util.websocket_message import WSMessageBytes
from lonelypss.ws.handlers.protocol import StateHandler
from lonelypss.ws.simple_receiver import SimpleReceiver
from lonelypss.ws.state import (
    Compressor,
    CompressorPreparing,
    CompressorReady,
    CompressorState,
    CompressorTrainingDataCollector,
    CompressorTrainingInfoBeforeLowWatermark,
    CompressorTrainingInfoType,
    ConnectionConfiguration,
    State,
    StateClosing,
    StateOpen,
    StateType,
    StateWaitingConfigure,
)
from lonelypss.ws.util import make_websocket_read_task


async def _make_standard_compressor(state: StateWaitingConfigure) -> CompressorReady:
    return CompressorReady(
        type=CompressorState.READY,
        identifier=1,
        level=3,
        min_size=state.broadcaster_config.compression_trained_max_size,
        max_size=None,
        data=None,
        compressors=list(),
        decompressors=list(),
    )


async def _make_preset_compressor(
    state: StateWaitingConfigure, compressor_id: int
) -> CompressorReady:
    compressor_info = await state.broadcaster_config.get_compression_dictionary_by_id(
        compressor_id
    )
    if compressor_info is None:
        raise ValueError(f"Unknown compressor ID {compressor_id}")
    zdict, level = compressor_info

    return CompressorReady(
        type=CompressorState.READY,
        identifier=compressor_id,
        level=level,
        min_size=state.broadcaster_config.compression_min_size,
        max_size=None,
        data=zdict,
        compressors=list(),
        decompressors=list(),
    )


async def handle_waiting_configure(state: State) -> State:
    """Waits for a configure message to be sent over the websocket; if the subscriber
    sends anything else, moves to closing.
    """

    assert state.type == StateType.WAITING_CONFIGURE
    ws_message = await state.read_task
    if ws_message["type"] == "websocket.disconnect":
        return StateClosing(type=StateType.CLOSING, websocket=state.websocket)
    if ws_message["type"] != "websocket.receive":
        return StateClosing(type=StateType.CLOSING, websocket=state.websocket)
    if "bytes" not in ws_message:
        return StateClosing(type=StateType.CLOSING, websocket=state.websocket)

    raw_message = cast(WSMessageBytes, ws_message)
    raw_message_reader = io.BytesIO(raw_message["bytes"])
    prefix = parse_s2b_message_prefix(raw_message_reader)
    if prefix.type != SubscriberToBroadcasterStatefulMessageType.CONFIGURE:
        return StateClosing(type=StateType.CLOSING, websocket=state.websocket)

    message = S2B_ConfigureParser.parse(prefix.flags, prefix.type, raw_message_reader)
    auth_result = await state.broadcaster_config.is_stateful_configure_allowed(
        message=message, now=time.time()
    )
    if auth_result != "ok":
        return StateClosing(type=StateType.CLOSING, websocket=state.websocket)

    receiver = SimpleReceiver()
    receiver_id = await state.internal_receiver.register_receiver(receiver)
    try:
        broadcaster_nonce = secrets.token_bytes(32)
        authorization = (
            await state.broadcaster_config.authorize_stateful_confirm_configure(
                broadcaster_nonce=broadcaster_nonce, now=time.time()
            )
        )
        connection_nonce = hashlib.sha256(
            message.subscriber_nonce + broadcaster_nonce
        ).digest()

        compressors: List[Compressor] = []
        if state.broadcaster_config.compression_allowed and message.enable_zstd:
            compressors.append(
                CompressorPreparing(
                    type=CompressorState.PREPARING,
                    identifier=1,
                    task=asyncio.create_task(_make_standard_compressor(state)),
                )
            )

            if message.initial_dict > 1:
                compressors.append(
                    CompressorPreparing(
                        type=CompressorState.PREPARING,
                        identifier=message.initial_dict,
                        task=asyncio.create_task(
                            _make_preset_compressor(state, message.initial_dict)
                        ),
                    )
                )

        return StateOpen(
            type=StateType.OPEN,
            websocket=state.websocket,
            broadcaster_config=state.broadcaster_config,
            connection_config=ConnectionConfiguration(
                enable_zstd=message.enable_zstd,
                enable_training=message.enable_zstd and message.enable_training,
            ),
            nonce_b64=base64.b64encode(connection_nonce).decode("ascii"),
            internal_receiver=state.internal_receiver,
            my_receiver=receiver,
            my_receiver_id=receiver_id,
            client_session=aiohttp.ClientSession(),
            compressors=compressors,
            compressor_training_info=(
                None
                if not state.broadcaster_config.allow_training
                or not message.enable_training
                else (
                    CompressorTrainingInfoBeforeLowWatermark(
                        type=CompressorTrainingInfoType.BEFORE_LOW_WATERMARK,
                        compressor_id=65536,
                        collector=CompressorTrainingDataCollector(
                            messages=0,
                            length=0,
                            tmpfile=tempfile.TemporaryFile(mode="w+b", buffering=-1),
                            pending=set(),
                        ),
                    )
                )
            ),
            broadcaster_counter=1,
            subscriber_counter=-1,
            read_task=make_websocket_read_task(state.websocket),
            internal_message_task=asyncio.create_task(receiver.queue.get()),
            notify_stream_state=None,
            send_task=asyncio.create_task(
                state.websocket.send_bytes(
                    serialize_b2s_confirm_configure(
                        B2S_ConfirmConfigure(
                            type=BroadcasterToSubscriberStatefulMessageType.CONFIRM_CONFIGURE,
                            broadcaster_nonce=broadcaster_nonce,
                            authorization=authorization,
                        ),
                        minimal_headers=state.broadcaster_config.websocket_minimal_headers,
                    )
                )
            ),
            process_task=None,
            unprocessed_messages=deque(
                maxlen=state.broadcaster_config.websocket_max_unprocessed_receives
            ),
            unsent_messages=deque(
                maxlen=state.broadcaster_config.websocket_max_pending_sends
            ),
            expecting_acks=asyncio.Queue(
                maxsize=state.broadcaster_config.websocket_send_max_unacknowledged or 0
            ),
            backgrounded=set(),
        )
    except BaseException:
        await state.internal_receiver.unregister_receiver(receiver_id)
        raise


if TYPE_CHECKING:
    _: StateHandler = handle_waiting_configure
