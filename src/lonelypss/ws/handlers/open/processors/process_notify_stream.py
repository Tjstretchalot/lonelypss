import asyncio
import dataclasses
import hashlib
import io
import tempfile
import time
from typing import IO, cast

from lonelypsp.stateful.constants import BroadcasterToSubscriberStatefulMessageType
from lonelypsp.stateful.messages.confirm_notify import (
    B2S_ConfirmNotify,
    serialize_b2s_confirm_notify,
)
from lonelypsp.stateful.messages.continue_notify import (
    B2S_ContinueNotify,
    serialize_b2s_continue_notify,
)
from lonelypsp.stateful.messages.notify_stream import S2B_NotifyStream

from lonelypss.routes.notify import TrustedNotifyResultType, handle_trusted_notify
from lonelypss.ws.handlers.open.collector_utils import (
    maybe_write_large_message_for_training,
)
from lonelypss.ws.handlers.open.compressor_utils import reserve_decompressor
from lonelypss.ws.handlers.open.errors import AuthRejectedException
from lonelypss.ws.handlers.open.send_simple_asap import send_simple_asap
from lonelypss.ws.state import CompressorState, NotifyStreamState, StateOpen


async def process_notify_stream(state: StateOpen, message: S2B_NotifyStream) -> None:
    """Processes a request by the subscriber to notify subscribers to a given
    topic with the given data, where that data may be sent over multiple websocket
    messages
    """

    if message.part_id is None:
        if state.notify_stream_state is not None:
            raise Exception(
                "notify stream: already in progress despite first part received"
            )

        state.notify_stream_state = NotifyStreamState(
            identifier=message.identifier,
            first=dataclasses.replace(message, payload=b""),
            part_id=-1,
            body_hasher=hashlib.sha512(),
            body=tempfile.SpooledTemporaryFile(
                max_size=state.broadcaster_config.message_body_spool_size
            ),
        )

    if state.notify_stream_state is None:
        raise Exception("notify stream: no first part received")

    if message.identifier != state.notify_stream_state.identifier:
        raise Exception("notify stream: identifier mismatch")

    if message.part_id != state.notify_stream_state.part_id + 1:
        raise Exception("notify stream: part_id mismatch")

    first = state.notify_stream_state.first

    auth_at = time.time()
    auth_result = await state.broadcaster_config.is_notify_allowed(
        topic=first.topic,
        message_sha512=(
            first.unverified_compressed_sha512
            if first.compressor_id is not None
            else first.unverified_uncompressed_sha512
        ),
        now=auth_at,
        authorization=message.authorization,
    )

    if auth_result != "ok":
        raise AuthRejectedException(f"notify stream: {auth_result}")

    state.notify_stream_state.body_hasher.update(message.payload)
    state.notify_stream_state.body.write(message.payload)
    state.notify_stream_state.part_id = message.part_id

    read_so_far = state.notify_stream_state.body.tell()
    expected_length = (
        first.compressed_length
        if first.compressor_id is not None
        else first.uncompressed_length
    )

    if read_so_far > expected_length:
        raise Exception("notify stream: received too much data")

    if read_so_far < expected_length:
        send_simple_asap(
            state,
            serialize_b2s_continue_notify(
                B2S_ContinueNotify(
                    type=BroadcasterToSubscriberStatefulMessageType.CONTINUE_NOTIFY,
                    identifier=message.identifier,
                    part_id=message.part_id,
                ),
                minimal_headers=state.broadcaster_config.websocket_minimal_headers,
            ),
        )
        return

    actual_sha512 = state.notify_stream_state.body_hasher.digest()
    expected_sha512 = (
        first.unverified_compressed_sha512
        if first.compressor_id is not None
        else first.unverified_uncompressed_sha512
    )

    if actual_sha512 != expected_sha512:
        raise Exception("notify stream: integrity check failed (hash mismatch)")

    if expected_length == 0:
        # different strategy in this case to avoid read(0)
        if first.compressor_id is not None:
            raise Exception("notify stream: empty compressed message")

        notify_result = await handle_trusted_notify(
            first.topic,
            io.BytesIO(b""),
            config=state.broadcaster_config,
            session=state.client_session,
            content_length=0,
            sha512=actual_sha512,
        )
        if notify_result.type != TrustedNotifyResultType.OK:
            raise Exception(f"notify stream failed: {notify_result}")

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
        state.notify_stream_state.body.close()
        state.notify_stream_state = None
        return

    body = state.notify_stream_state.body
    if first.compressor_id is None:
        with maybe_write_large_message_for_training(
            state, first.uncompressed_length
        ) as training_writer:
            if training_writer.is_void:
                training_writer.skip_void()
            else:
                body.seek(0)

                while True:
                    chunk = body.read(
                        min(io.DEFAULT_BUFFER_SIZE, training_writer.remaining)
                    )
                    if not chunk:
                        break
                    training_writer.write_chunk(chunk)
                    if not training_writer.remaining:
                        break

                    await asyncio.sleep(0)

        body.seek(0)
        notify_result = await handle_trusted_notify(
            first.topic,
            body,
            config=state.broadcaster_config,
            session=state.client_session,
            content_length=first.uncompressed_length,
            sha512=actual_sha512,
        )

        if notify_result.type != TrustedNotifyResultType.OK:
            raise Exception(f"notify stream failed: {notify_result}")

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
        body.close()
        state.notify_stream_state = None
        return

    for candidate_compressor in state.compressors:
        if candidate_compressor.identifier == first.compressor_id:
            compressor = candidate_compressor
            break
    else:
        raise ValueError(f"compressor not found: {first.compressor_id}")

    if compressor.type == CompressorState.PREPARING:
        compressor = await compressor.task

    with tempfile.SpooledTemporaryFile(
        max_size=state.broadcaster_config.message_body_spool_size
    ) as decompressed_file:
        hasher = hashlib.sha512()
        pos = 0
        with (
            maybe_write_large_message_for_training(
                state, first.decompressed_length
            ) as training_writer,
            reserve_decompressor(state, compressor) as reserved_decompressor,
            reserved_decompressor.stream_reader(cast(IO[bytes], body)) as streamer,
        ):
            while True:
                chunk = streamer.read(io.DEFAULT_BUFFER_SIZE)
                if not chunk:
                    break

                pos += len(chunk)
                if pos > first.decompressed_length:
                    raise Exception("notify stream: received too much data")

                decompressed_file.write(chunk)
                training_writer.write_chunk(chunk)
                hasher.update(chunk)

                await asyncio.sleep(0)

        if pos != first.decompressed_length:
            raise Exception("notify stream: received too little data")

        decompressed_sha512 = hasher.digest()

        state.notify_stream_state.body.close()
        state.notify_stream_state = None

        decompressed_file.seek(0)
        notify_result = await handle_trusted_notify(
            first.topic,
            decompressed_file,
            config=state.broadcaster_config,
            session=state.client_session,
            content_length=first.decompressed_length,
            sha512=decompressed_sha512,
        )
        if notify_result.type != TrustedNotifyResultType.OK:
            raise Exception(f"notify stream failed: {notify_result}")

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
