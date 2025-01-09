import asyncio
import hashlib
import io
import secrets
import tempfile
import time
from typing import Optional

from lonelypsp.stateful.constants import (
    BroadcasterToSubscriberStatefulMessageType,
    SubscriberToBroadcasterStatefulMessageType,
)
from lonelypsp.stateful.messages.confirm_receive import S2B_ConfirmReceive
from lonelypsp.stateful.messages.continue_receive import S2B_ContinueReceive
from lonelypsp.stateful.messages.receive_stream import (
    B2S_ReceiveStreamContinuation,
    B2S_ReceiveStreamStartCompressed,
    B2S_ReceiveStreamStartUncompressed,
    serialize_b2s_receive_stream,
)

from lonelypss.util.sync_io import SyncReadableBytesIO, read_exact
from lonelypss.ws.handlers.open.collector_utils import (
    maybe_write_large_message_for_training,
)
from lonelypss.ws.handlers.open.compressor_utils import (
    choose_compressor_for_compression,
    reserve_compressor,
)
from lonelypss.ws.handlers.open.websocket_url import (
    make_for_send_websocket_url_and_change_counter,
)
from lonelypss.ws.state import (
    CompressorReady,
    StateOpen,
)


async def send_receive_stream(
    state: StateOpen,
    uncompressed_stream: SyncReadableBytesIO,
    /,
    *,
    topic: bytes,
    uncompressed_sha512: bytes,
    uncompressed_length: int,
    maybe_store_for_training: bool,
    read_lock: Optional[asyncio.Lock],
) -> None:
    """
    Sends a stream of data via potentially multiple RECEIVE_STREAM messages.
    This will determine if compression is appropriate, and if so, will compress
    the data before sending it.

    Args:
        state (StateOpen): the state
        uncompressed_stream (SyncReadableBytesIO): the readable stream of data to send
        topic (bytes): the topic for the message
        uncompressed_sha512 (bytes): the sha512 of the message
        uncompressed_length (int): the length of the stream; we assume the stream will not over-read
        maybe_store_for_training (bool): if true, if a message of this length would be
            useful for training a compression dictionary, it will be stored for that purpose,
            otherwise it will not be stored for that purpose
        read_lock (Optional[asyncio.Lock]): if not None, this lock will be acquired before
            reading from the stream and released right after reading the stream, which
            can be used to "promote" the read to async
    """
    compressor = choose_compressor_for_compression(state, uncompressed_length)

    if compressor is None:
        await send_uncompressed_receive_stream(
            state,
            uncompressed_stream,
            topic=topic,
            sha512=uncompressed_sha512,
            length=uncompressed_length,
            maybe_store_for_training=maybe_store_for_training,
            read_lock=read_lock,
        )
        return

    await send_compressed_receive_stream(
        state,
        uncompressed_stream,
        topic=topic,
        uncompressed_length=uncompressed_length,
        compressor=compressor,
        maybe_store_for_training=maybe_store_for_training,
        read_lock=read_lock,
    )


async def send_uncompressed_receive_stream(
    state: StateOpen,
    stream: SyncReadableBytesIO,
    /,
    *,
    topic: bytes,
    sha512: bytes,
    length: int,
    maybe_store_for_training: bool,
    read_lock: Optional[asyncio.Lock],
) -> None:
    """Sends an uncompressed stream of data via potentially multiple RECEIVE_STREAM
    messages

    Args:
        state (StateOpen): the state, for grabbing configuration options
        stream (SyncIOBaseLikeIO): the readable stream of data to send
        topic (bytes): the topic for the message
        sha512 (bytes): the sha512 of the message
        length (int): the length of the stream; we assume the stream will not over-read
        maybe_store_for_training (bool): if true, if a message of this length would be
            useful for training a compression dictionary, it will be stored for that purpose,
            otherwise it will not be stored for that purpose
        read_lock (Optional[asyncio.Lock]): if not None, this lock will be acquired before
            reading from the stream and released right after reading the stream, which
            can be used to "promote" the read to async
    """
    authorization = await state.broadcaster_config.authorize_receive(
        url=make_for_send_websocket_url_and_change_counter(state),
        topic=topic,
        message_sha512=sha512,
        now=time.time(),
    )
    identifier = secrets.token_bytes(4)

    headers = serialize_b2s_receive_stream(
        B2S_ReceiveStreamStartUncompressed(
            type=BroadcasterToSubscriberStatefulMessageType.RECEIVE_STREAM,
            authorization=authorization,
            identifier=identifier,
            part_id=None,
            topic=topic,
            compressor_id=None,
            uncompressed_length=length,
            unverified_uncompressed_sha512=sha512,
            payload=b"",
        ),
        minimal_headers=state.broadcaster_config.websocket_minimal_headers,
    )
    await send_receive_stream_given_first_headers(
        state,
        stream,
        length,
        identifier,
        topic,
        sha512,
        headers,
        maybe_store_for_training=maybe_store_for_training,
        read_lock=read_lock,
    )


async def send_compressed_receive_stream(
    state: StateOpen,
    uncompressed_stream: SyncReadableBytesIO,
    /,
    *,
    topic: bytes,
    uncompressed_length: int,
    compressor: CompressorReady,
    maybe_store_for_training: bool,
    read_lock: Optional[asyncio.Lock],
) -> None:
    """Sends a compressed stream of data via potentially multiple RECEIVE_STREAM
    messages

    Args:
        state (StateOpen): the state, for grabbing configuration options
        uncompressed_stream (SyncIOBaseLikeIO): the readable stream of data to send
        topic (bytes): the topic for the message
        uncompressed_length (int): the length of the uncompressed stream; we assume the stream
            will not over-read
        compressor (CompressorReady): the compressor to use for compressing the data
        maybe_store_for_training (bool): if true, if a message of this length would be
            useful for training a compression dictionary, it will be stored for that purpose,
            otherwise it will not be stored for that purpose.
        read_lock (Optional[asyncio.Lock]): if not None, this lock will be acquired before
            reading from the stream and released right after reading the stream, which
            can be used to "promote" the read to async

    NOTE:
        This correctly stores the uncompressed data for training purposes, not the
        compressed data, when appropriate
    """

    with tempfile.SpooledTemporaryFile(
        max_size=state.broadcaster_config.message_body_spool_size
    ) as to_send:
        hasher = hashlib.sha512()
        with (
            maybe_write_large_message_for_training(
                state, uncompressed_length, never_store=not maybe_store_for_training
            ) as training_writer,
            reserve_compressor(state, compressor) as reserved_compressor,
        ):
            chunker = reserved_compressor.chunker(
                size=uncompressed_length, chunk_size=io.DEFAULT_BUFFER_SIZE
            )

            while True:
                if read_lock is None:
                    uncompressed_chunk = uncompressed_stream.read(
                        io.DEFAULT_BUFFER_SIZE
                    )
                else:
                    async with read_lock:
                        uncompressed_chunk = uncompressed_stream.read(
                            io.DEFAULT_BUFFER_SIZE
                        )
                if not uncompressed_chunk:
                    break

                training_writer.write_chunk(uncompressed_chunk)
                for chunk in chunker.compress(uncompressed_chunk):
                    to_send.write(chunk)
                    hasher.update(chunk)
                    await asyncio.sleep(0)

            for chunk in chunker.finish():
                to_send.write(chunk)
                hasher.update(chunk)
                await asyncio.sleep(0)

        compressed_length = to_send.tell()
        compressed_sha512 = hasher.digest()
        to_send.seek(0)

        identifier = secrets.token_bytes(4)
        headers = serialize_b2s_receive_stream(
            B2S_ReceiveStreamStartCompressed(
                type=BroadcasterToSubscriberStatefulMessageType.RECEIVE_STREAM,
                authorization=await state.broadcaster_config.authorize_receive(
                    url=make_for_send_websocket_url_and_change_counter(state),
                    topic=topic,
                    message_sha512=compressed_sha512,
                    now=time.time(),
                ),
                identifier=identifier,
                part_id=None,
                topic=topic,
                compressor_id=compressor.identifier,
                compressed_length=compressed_length,
                decompressed_length=uncompressed_length,
                unverified_compressed_sha512=compressed_sha512,
                payload=b"",
            ),
            minimal_headers=state.broadcaster_config.websocket_minimal_headers,
        )
        await send_receive_stream_given_first_headers(
            state,
            to_send,
            compressed_length,
            identifier,
            topic,
            compressed_sha512,
            headers,
            maybe_store_for_training=False,
            read_lock=None,
        )


async def send_receive_stream_given_first_headers(
    state: StateOpen,
    stream: SyncReadableBytesIO,
    length: int,
    identifier: bytes,
    topic: bytes,
    sha512: bytes,
    first_headers: bytes,
    maybe_store_for_training: bool,
    read_lock: Optional[asyncio.Lock],
) -> None:
    """Sends the given stream of data to the subscriber via potentially multiple
    RECEIVE_STREAM messages, using the given headers for the first message.

    Args:
        state (StateOpen): the state, for grabbing configuration options
        stream (SyncReadableBytesIO): the readable stream of data to send
        length (int): the length of the stream; we assume the stream will not over-read
        identifier (bytes): the identifier for the message
        topic (bytes): the topic for the message
        sha512 (bytes): the sha512 of the message
        first_headers (bytes): the headers for the first message
        maybe_store_for_training (bool): if true, if a message of this length would be
            useful for training a compression dictionary, it will be stored for that purpose,
            otherwise it will not be stored for that purpose.
        read_lock (Optional[asyncio.Lock]): if not None, this lock will be acquired before
            reading from the stream and released right after reading the stream, which
            can be used to "promote" the read to async

    NOTE:
        This only handles the data from the stream as-is, meaning that if
        `maybe_store_for_training` is true, then the stream should be for
        uncompressed data.
    """
    headers = first_headers
    msg_size = state.broadcaster_config.outgoing_max_ws_message_size or (2**64 - 1)

    part_id = 0
    pos = 0
    with maybe_write_large_message_for_training(
        state, length, never_store=not maybe_store_for_training
    ) as training_writer:
        while True:
            target_read_amount = min(length - pos, max(512, msg_size - len(headers)))

            if target_read_amount == 0:
                payload = b""
            elif read_lock is None:
                payload = read_exact(stream, target_read_amount)
            else:
                async with read_lock:
                    payload = read_exact(stream, target_read_amount)
            training_writer.write_chunk(payload)
            pos += len(payload)
            is_done = pos >= length
            await state.expecting_acks.put(
                S2B_ConfirmReceive(
                    type=SubscriberToBroadcasterStatefulMessageType.CONFIRM_RECEIVE,
                    identifier=identifier,
                )
                if is_done
                else S2B_ContinueReceive(
                    type=SubscriberToBroadcasterStatefulMessageType.CONTINUE_RECEIVE,
                    identifier=identifier,
                    part_id=part_id,
                )
            )
            await state.websocket.send_bytes(headers + payload)

            if is_done:
                return

            part_id += 1
            authorization = await state.broadcaster_config.authorize_receive(
                url=make_for_send_websocket_url_and_change_counter(state),
                topic=topic,
                message_sha512=sha512,
                now=time.time(),
            )
            headers = serialize_b2s_receive_stream(
                B2S_ReceiveStreamContinuation(
                    type=BroadcasterToSubscriberStatefulMessageType.RECEIVE_STREAM,
                    authorization=authorization,
                    identifier=identifier,
                    part_id=part_id,
                    payload=b"",
                ),
                minimal_headers=state.broadcaster_config.websocket_minimal_headers,
            )
