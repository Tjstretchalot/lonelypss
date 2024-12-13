import asyncio
import hashlib
import io
import tempfile
import time

from lonelypsp.stateful.constants import BroadcasterToSubscriberStatefulMessageType
from lonelypsp.stateful.messages.confirm_notify import (
    B2S_ConfirmNotify,
    serialize_b2s_confirm_notify,
)
from lonelypsp.stateful.messages.notify import S2B_Notify

from lonelypss.routes.notify import TrustedNotifyResultType, handle_trusted_notify
from lonelypss.ws.handlers.open.collector_utils import (
    maybe_store_small_message_for_training,
    maybe_write_large_message_for_training,
)
from lonelypss.ws.handlers.open.compressor_utils import reserve_decompressor
from lonelypss.ws.handlers.open.errors import AuthRejectedException
from lonelypss.ws.handlers.open.send_simple_asap import send_simple_asap
from lonelypss.ws.state import CompressorState, StateOpen


async def process_notify(state: StateOpen, message: S2B_Notify) -> None:
    """Processes a request by the subscriber to notify subscribers to a given
    topic with the given data
    """
    auth_at = time.time()
    auth_result = await state.broadcaster_config.is_notify_allowed(
        topic=message.topic,
        message_sha512=(
            message.verified_compressed_sha512
            if message.compressor_id is not None
            else message.verified_uncompressed_sha512
        ),
        now=auth_at,
        authorization=message.authorization,
    )

    if auth_result != "ok":
        raise AuthRejectedException(f"notify: {auth_result}")

    if message.compressor_id is None:
        maybe_store_small_message_for_training(state, message.uncompressed_message)
        notify_result = await handle_trusted_notify(
            message.topic,
            io.BytesIO(message.uncompressed_message),
            config=state.broadcaster_config,
            session=state.client_session,
            content_length=len(message.uncompressed_message),
            sha512=message.verified_uncompressed_sha512,
        )

        if notify_result.type != TrustedNotifyResultType.OK:
            raise ValueError(f"notify failed: {notify_result}")

        send_simple_asap(
            state,
            serialize_b2s_confirm_notify(
                B2S_ConfirmNotify(
                    type=BroadcasterToSubscriberStatefulMessageType.CONFIRM_NOTIFY,
                    identifier=message.identifier,
                    subscribers=notify_result.succeeded,
                ),
                minimal_headers=state.broadcaster_config.websocket_minimal_headers,
            ),
        )
        return

    for candidate_compressor in state.compressors:
        if candidate_compressor.identifier == message.compressor_id:
            compressor = candidate_compressor
            break
    else:
        raise ValueError(f"compressor not found: {message.compressor_id}")

    if compressor.type == CompressorState.PREPARING:
        compressor = await compressor.task

    # although the compressed data fits in memory, we may not want to have the
    # entire uncompressed data in memory. it would neat and efficient to simply
    # decompress in parts as we are notifying subscribers, but is a bit
    # challenging to implement

    with (
        tempfile.SpooledTemporaryFile(
            max_size=state.broadcaster_config.message_body_spool_size
        ) as decompressed_file,
    ):
        pos = 0
        hasher = hashlib.sha512()

        with (
            maybe_write_large_message_for_training(
                state, message.decompressed_length
            ) as training_writer,
            reserve_decompressor(state, compressor) as reserved_decompressor,
            reserved_decompressor.stream_reader(message.compressed_message) as streamer,
        ):
            while True:
                chunk = streamer.read(io.DEFAULT_BUFFER_SIZE)
                if not chunk:
                    break
                pos += len(chunk)
                if pos > message.decompressed_length:
                    raise ValueError(
                        "decompressed length exceeded during decompression"
                    )

                decompressed_file.write(chunk)
                training_writer.write_chunk(chunk)
                hasher.update(chunk)

                await asyncio.sleep(0)

        if pos != message.decompressed_length:
            raise ValueError("decompressed length not reached during decompression")

        decompressed_sha512 = hasher.digest()
        decompressed_file.seek(0)

        notify_result = await handle_trusted_notify(
            message.topic,
            decompressed_file,
            config=state.broadcaster_config,
            session=state.client_session,
            content_length=message.decompressed_length,
            sha512=decompressed_sha512,
        )
        if notify_result.type != TrustedNotifyResultType.OK:
            raise ValueError(f"notify failed: {notify_result}")
        send_simple_asap(
            state,
            serialize_b2s_confirm_notify(
                B2S_ConfirmNotify(
                    type=BroadcasterToSubscriberStatefulMessageType.CONFIRM_NOTIFY,
                    identifier=message.identifier,
                    subscribers=notify_result.succeeded,
                ),
                minimal_headers=state.broadcaster_config.websocket_minimal_headers,
            ),
        )
