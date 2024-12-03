import base64
import hashlib
import io
import re
import secrets
import tempfile
import time
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Protocol,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)
from fastapi import APIRouter, WebSocket
from dataclasses import dataclass
from collections import deque
from enum import IntFlag, IntEnum, Enum, auto
from httppubsubserver.config.config import Config
from httppubsubserver.middleware.config import get_config_from_request
from httppubsubserver.middleware.ws_receiver import get_ws_receiver_from_request
from httppubsubserver.util.websocket_message import (
    WSMessage,
    WSMessageBytes,
)
import asyncio

from httppubsubserver.util.ws_receiver import BaseWSReceiver, FanoutWSReceiver
from httppubsubserver.util.sync_io import SyncReadableBytesIO, SyncIOBaseLikeIO

try:
    import zstandard
except ImportError:
    ...


router = APIRouter()


class _ParsedWSMessageFlags(IntFlag):
    MINIMAL_HEADERS = 1 << 0


class _ParsedWSMessageType(IntEnum):
    CONFIGURE = 0
    SUBSCRIBE = 1
    UNSUBSCRIBE = 2
    NOTIFY = 3
    NOTIFY_STREAM = 4


class _OutgoingParsedWSMessageType(IntEnum):
    CONFIRM_CONFIGURE = 0
    CONFIRM_SUBSCRIBE_EXACT = 1
    CONFIRM_SUBSCRIBE_GLOB = 2
    CONFIRM_UNSUBSCRIBE_EXACT = 3
    CONFIRM_UNSUBSCRIBE_GLOB = 4
    CONFIRM_NOTIFY = 5
    CONTINUE_NOTIFY = 6
    RECEIVE_STREAM = 7
    ENABLE_ZSTD_PRESET = 100
    ENABLE_ZSTD_CUSTOM = 101


@dataclass
class _ParsedWSMessage:
    flags: _ParsedWSMessageFlags
    type: _ParsedWSMessageType
    headers: Dict[str, bytes]
    body: bytes


def _exact_read(stream: SyncReadableBytesIO, n: int) -> bytes:
    data = stream.read(n)
    if len(data) != n:
        raise ValueError("Stream ended unexpectedly")
    return data


_STANDARD_MINIMAL_HEADERS_BY_TYPE: Dict[_ParsedWSMessageType, List[str]] = {
    _ParsedWSMessageType.CONFIGURE: [],
    _ParsedWSMessageType.SUBSCRIBE: ["authorization"],
    _ParsedWSMessageType.UNSUBSCRIBE: ["authorization"],
    _ParsedWSMessageType.NOTIFY: ["authorization", "x-identifier"],
}


def _parse_websocket_message(body: bytes) -> _ParsedWSMessage:
    stream = io.BytesIO(body)
    flags = _ParsedWSMessageFlags(int.from_bytes(_exact_read(stream, 2), "big"))
    message_type = _ParsedWSMessageType(int.from_bytes(_exact_read(stream, 2), "big"))

    headers: Dict[str, bytes] = {}
    if flags & _ParsedWSMessageFlags.MINIMAL_HEADERS:
        if message_type in _STANDARD_MINIMAL_HEADERS_BY_TYPE:
            minimal_headers = _STANDARD_MINIMAL_HEADERS_BY_TYPE[message_type]
        if message_type == _ParsedWSMessageType.NOTIFY_STREAM:
            length = int.from_bytes(_exact_read(stream, 2), "big")
            headers["authorization"] = _exact_read(stream, length)

            length = int.from_bytes(_exact_read(stream, 2), "big")
            if length > 8:
                raise ValueError("part id max 8 bytes")
            part_id_bytes = _exact_read(stream, length)
            headers["x-part-id"] = part_id_bytes

            part_id = int.from_bytes(part_id_bytes, "big")
            if part_id == 0:
                minimal_headers = [
                    "x-topic",
                    "x-compressor",
                    "x-compressed-length",
                    "x-deflated-length",
                    "x-repr-digest",
                    "x-identifier",
                ]
            else:
                minimal_headers = ["x-identifier"]

        for header in minimal_headers:
            length = int.from_bytes(_exact_read(stream, 2), "big")
            headers[header] = _exact_read(stream, length)
    else:
        num_headers = int.from_bytes(_exact_read(stream, 2), "big")
        for _ in range(num_headers):
            name_length = int.from_bytes(_exact_read(stream, 2), "big")
            name_enc = _exact_read(stream, name_length)
            name = name_enc.decode("ascii").lower()
            value_length = int.from_bytes(_exact_read(stream, 2), "big")
            value = _exact_read(stream, value_length)
            headers[name] = value

    return _ParsedWSMessage(flags, message_type, headers, stream.read())


def _make_websocket_message(
    flags: _ParsedWSMessageFlags,
    message_type: _OutgoingParsedWSMessageType,
    headers: List[Tuple[str, bytes]],
    body: bytes,
) -> bytes:
    stream = io.BytesIO()
    stream.write(flags.to_bytes(2, "big"))
    stream.write(message_type.to_bytes(2, "big"))
    if flags & _ParsedWSMessageFlags.MINIMAL_HEADERS:
        for _, value in headers:
            stream.write(len(value).to_bytes(2, "big"))
            stream.write(value)
    else:
        stream.write(len(headers).to_bytes(2, "big"))
        for name, value in headers:
            enc_name = name.encode("ascii")
            stream.write(len(enc_name).to_bytes(2, "big"))
            stream.write(enc_name)
            stream.write(len(value).to_bytes(2, "big"))
            stream.write(value)
    stream.write(body)
    return stream.getvalue()


def _make_websocket_read_task(websocket: WebSocket) -> asyncio.Task[WSMessage]:
    return cast(asyncio.Task[WSMessage], asyncio.create_task(websocket.receive()))


class _ConfigurationFlags(IntFlag):
    ZSTD_ENABLED = 1 << 0
    ZSTD_TRAINING_ALLOWED = 1 << 1


@dataclass
class _Configuration:
    flags: _ConfigurationFlags
    dictionary_id: int
    nonce_b64: str
    """The agreed upon nonce for this connection, which mixes input from the broadcaster and subscriber"""


class _StateType(Enum):
    ACCEPTING = auto()
    OPEN = auto()
    CLOSING = auto()
    CLOSED = auto()


@dataclass
class _StateAccepting:
    type: Literal[_StateType.ACCEPTING]
    websocket: WebSocket
    config: Config
    receiver: FanoutWSReceiver


class _MessageType(Enum):
    SMALL = auto()
    LARGE = auto()
    FORMATTED = auto()


@dataclass
class _LargeMessage:
    type: Literal[_MessageType.LARGE]
    stream: SyncReadableBytesIO
    topic: bytes
    sha512: bytes
    length: int
    finished: asyncio.Event


@dataclass
class _LargeSpooledMessage:
    type: Literal[_MessageType.LARGE]
    stream: SyncIOBaseLikeIO
    topic: bytes
    sha512: bytes
    length: int
    finished: asyncio.Event


@dataclass
class _SmallMessage:
    type: Literal[_MessageType.SMALL]
    data: bytes
    topic: bytes
    sha512: bytes


@dataclass
class _FormattedMessage:
    type: Literal[_MessageType.FORMATTED]
    websocket_data: bytes


class _MyReceiver:
    def __init__(self) -> None:
        self.exact_subscriptions: Set[bytes] = set()
        self.glob_subscriptions: List[Tuple[re.Pattern, str]] = []
        self.receiver_id: Optional[int] = None

        self.queue: asyncio.Queue[Union[_LargeMessage, _SmallMessage]] = asyncio.Queue()

    def is_relevant(self, topic: bytes) -> bool:
        return topic in self.exact_subscriptions or any(
            pattern.match(topic) for pattern, _ in self.glob_subscriptions
        )

    async def on_large_exclusive_incoming(
        self,
        stream: SyncReadableBytesIO,
        /,
        *,
        topic: bytes,
        sha512: bytes,
        length: int,
    ) -> None:
        finished = asyncio.Event()
        await self.queue.put(
            _LargeMessage(_MessageType.LARGE, stream, topic, sha512, length, finished)
        )
        await finished.wait()

    async def on_small_incoming(
        self,
        data: bytes,
        /,
        *,
        topic: bytes,
        sha512: bytes,
    ) -> None:
        await self.queue.put(_SmallMessage(_MessageType.SMALL, data, topic, sha512))


if TYPE_CHECKING:
    _: Type[BaseWSReceiver] = _MyReceiver


@dataclass
class _CompressorTrainingDataCollector:
    started_at: float
    messages: int
    length: int
    """The length of the actual sample data; the file will be longer as we
    will include length prefixes before each sample data
    """
    tmpfile: SyncIOBaseLikeIO


class _CompressorTrainingInfoType(Enum):
    BEFORE_LOW_WATERMARK = auto()
    """We are waiting for data to build a new dictionary using the low watermark settings"""
    BEFORE_HIGH_WATERMARK = auto()
    """We are waiting for data to build a new dictionary using the high watermark settings"""
    WAITING_TO_REFRESH = auto()
    """We built a dictionary recently; once some time passes, we'll build another one"""


@dataclass
class _CompressorTrainingInfoBeforeLowWatermark:
    type: Literal[_CompressorTrainingInfoType.BEFORE_LOW_WATERMARK]
    collector: _CompressorTrainingDataCollector
    dirty: bool


@dataclass
class _CompressorTrainingInfoBeforeHighWatermark:
    type: Literal[_CompressorTrainingInfoType.BEFORE_HIGH_WATERMARK]
    collector: _CompressorTrainingDataCollector
    dirty: bool


@dataclass
class _CompressorTrainingInfoWaitingToRefresh:
    type: Literal[_CompressorTrainingInfoType.WAITING_TO_REFRESH]
    last_refreshed_at: float
    dirty: Literal[False]


_CompressorTrainingInfo = Union[
    _CompressorTrainingInfoBeforeLowWatermark,
    _CompressorTrainingInfoBeforeHighWatermark,
    _CompressorTrainingInfoWaitingToRefresh,
]


class _CompressorState(Enum):
    PREPARING = auto()
    READY = auto()


@dataclass
class _CompressorReady:
    type: Literal[_CompressorState.READY]
    dictionary_id: int
    level: int
    data: "zstandard.ZstdCompressionDict"


@dataclass
class _CompressorPreparing:
    type: Literal[_CompressorState.PREPARING]
    dictionary_id: int
    task: asyncio.Task[_CompressorReady]


_Compressor = Union[_CompressorReady, _CompressorPreparing]


@dataclass
class _StateOpen:
    type: Literal[_StateType.OPEN]
    websocket: WebSocket
    config: Config
    receiver: FanoutWSReceiver

    configuration: Optional[_Configuration]
    my_receiver: _MyReceiver

    read_task: asyncio.Task[WSMessage]
    send_task: Optional[asyncio.Task[None]]
    message_task: asyncio.Task[Union[_SmallMessage, _LargeMessage]]
    pending_sends: deque[Union[_SmallMessage, _LargeSpooledMessage, _FormattedMessage]]
    """If we can't push a message to the send_task immediately, we move it here.
    When we move large messages to this queue, we spool them to file
    """

    active_compressor: Optional[_Compressor]
    last_compressor: Optional[_Compressor]
    training_data: Optional[_CompressorTrainingInfo]
    backgrounded: Set[asyncio.Task[None]]

    broadcaster_counter: int
    """For authorization headers made by the broadcaster; increments after we use it"""
    subscriber_counter: int
    """What we expect for authorization headers made by the subscriber; decrements after we see it"""
    custom_compression_dict_counter: int
    """The id we should use for the next generated compression dictionary"""


@dataclass
class _StateClosing:
    type: Literal[_StateType.CLOSING]
    websocket: WebSocket
    exception: Optional[BaseException] = None


@dataclass
class _StateClosed:
    type: Literal[_StateType.CLOSED]


_State = Union[
    _StateAccepting,
    _StateOpen,
    _StateClosing,
    _StateClosed,
]


class _StateHandler(Protocol):
    async def __call__(self, state: _State) -> _State: ...


async def _handle_accepting(state: _State) -> _State:
    assert state.type == _StateType.ACCEPTING
    try:
        await asyncio.wait_for(
            state.websocket.accept(), timeout=state.config.websocket_accept_timeout
        )
    except asyncio.TimeoutError:
        return _StateClosing(type=_StateType.CLOSING, websocket=state.websocket)

    my_receiver = _MyReceiver()
    return _StateOpen(
        type=_StateType.OPEN,
        websocket=state.websocket,
        config=state.config,
        receiver=state.receiver,
        configuration=None,
        my_receiver=my_receiver,
        read_task=_make_websocket_read_task(state.websocket),
        send_task=None,
        message_task=asyncio.create_task(my_receiver.queue.get()),
        pending_sends=deque(maxlen=state.config.websocket_max_pending_sends),
        active_compressor=None,
        last_compressor=None,
        training_data=(
            None
            if not state.config.allow_training
            else _CompressorTrainingInfoBeforeLowWatermark(
                type=_CompressorTrainingInfoType.BEFORE_LOW_WATERMARK,
                collector=_CompressorTrainingDataCollector(
                    started_at=time.time(),
                    messages=0,
                    length=0,
                    tmpfile=tempfile.TemporaryFile("w+b", buffering=-1),
                ),
                dirty=False,
            )
        ),
        backgrounded=set(),
        broadcaster_counter=1,
        subscriber_counter=-1,
        custom_compression_dict_counter=65536,
    )


def _smallest_unsigned_size(n: int) -> int:
    assert n >= 0
    return (n.bit_length() - 1) // 8 + 1


def _make_for_send_websocket_url_and_change_counter(state: _StateOpen) -> str:
    assert state.configuration is not None
    ctr = state.broadcaster_counter
    state.broadcaster_counter += 1
    return f"websocket:{state.configuration.nonce_b64}:{ctr:x}"


def _make_for_receive_websocket_url_and_change_counter(state: _StateOpen) -> str:
    assert state.configuration is not None
    ctr = state.subscriber_counter
    state.subscriber_counter -= 1
    return f"websocket:{state.configuration.nonce_b64}:{ctr:x}"


def _handle_if_should_start_retraining(state: _StateOpen) -> None:
    if state.training_data is None:
        return

    if state.training_data.type != _CompressorTrainingInfoType.WAITING_TO_REFRESH:
        return

    next_refresh = (
        state.training_data.last_refreshed_at
        + state.config.compression_retrain_interval_seconds
    )
    now = time.time()
    if now < next_refresh:
        return
    state.training_data = _CompressorTrainingInfoBeforeHighWatermark(
        type=_CompressorTrainingInfoType.BEFORE_HIGH_WATERMARK,
        collector=_CompressorTrainingDataCollector(
            started_at=now,
            messages=0,
            length=0,
            tmpfile=tempfile.TemporaryFile("w+b", buffering=-1),
        ),
        dirty=False,
    )


def _store_small_for_compression_training(state: _StateOpen, data: bytes) -> None:
    if state.training_data is None:
        return

    length = len(data)
    if state.config.compression_min_size > length:
        # this data is too small for compression to be useful
        return

    if state.config.compression_trained_max_size <= length:
        # this data is too large to benefit from precomputing the compression dictionary
        return

    _handle_if_should_start_retraining(state)
    if state.training_data.type == _CompressorTrainingInfoType.WAITING_TO_REFRESH:
        return

    state.training_data.collector.tmpfile.write(length.to_bytes(4, "big"))
    state.training_data.collector.tmpfile.write(data)
    state.training_data.collector.messages += 1
    state.training_data.collector.length += length
    state.training_data.dirty = True


def _should_store_large_message_for_training(
    state: _StateOpen, msg: Union[_LargeMessage, _LargeSpooledMessage]
) -> bool:
    """It is possible to configure us so that some messages are compressed with
    a precomputed dictionary but spooled to file... this is a pretty strange
    setup, but it may be helpful for benchmarking
    """
    if state.training_data is None:
        return False

    if state.config.compression_trained_max_size <= msg.length:
        # this data is too large to benefit from precomputing the compression dictionary
        # (this is what we expect, since we spooled to file)
        return False

    if state.config.compression_min_size > msg.length:
        # this data is too small for compression to be useful (this is absurd given we spooled)
        return False

    _handle_if_should_start_retraining(state)
    if state.training_data.type == _CompressorTrainingInfoType.WAITING_TO_REFRESH:
        return False

    return True


async def _check_training_data(state: _StateOpen) -> _State:
    if state.training_data is None:
        return state

    if state.training_data.type == _CompressorTrainingInfoType.BEFORE_LOW_WATERMARK:
        if (
            state.training_data.collector.length
            >= state.config.compression_training_high_watermark
        ):
            # skip low watermark
            state.training_data = _CompressorTrainingInfoBeforeHighWatermark(
                type=_CompressorTrainingInfoType.BEFORE_HIGH_WATERMARK,
                collector=state.training_data.collector,
                dirty=True,
            )
            return state

        if (
            state.training_data.collector.length
            >= state.config.compression_training_low_watermark
        ):
            samples: List[bytes] = []
            state.training_data.collector.tmpfile.seek(0)
            while True:
                length_bytes = state.training_data.collector.tmpfile.read(4)
                if not length_bytes:
                    break
                length = int.from_bytes(length_bytes, "big")
                samples.append(state.training_data.collector.tmpfile.read(length))

            dictionary_id = state.custom_compression_dict_counter
            state.custom_compression_dict_counter += 1

            async def _make_compressor() -> _CompressorReady:
                zdict, level = await state.config.train_compression_dict_low_watermark(
                    samples
                )
                return _CompressorReady(
                    type=_CompressorState.READY,
                    dictionary_id=dictionary_id,
                    level=level,
                    data=zdict,
                )

            state.last_compressor = state.active_compressor
            state.active_compressor = _CompressorPreparing(
                type=_CompressorState.PREPARING,
                dictionary_id=dictionary_id,
                task=asyncio.create_task(_make_compressor()),
            )
            state.training_data = _CompressorTrainingInfoBeforeHighWatermark(
                type=_CompressorTrainingInfoType.BEFORE_HIGH_WATERMARK,
                collector=state.training_data.collector,
                dirty=False,
            )
            return state

    if state.training_data.type == _CompressorTrainingInfoType.BEFORE_HIGH_WATERMARK:
        if (
            state.training_data.collector.length
            >= state.config.compression_training_high_watermark
        ):
            samples = []
            state.training_data.collector.tmpfile.seek(0)
            while True:
                length_bytes = state.training_data.collector.tmpfile.read(4)
                if not length_bytes:
                    break
                length = int.from_bytes(length_bytes, "big")
                samples.append(state.training_data.collector.tmpfile.read(length))

            dictionary_id = state.custom_compression_dict_counter
            state.custom_compression_dict_counter += 1

            async def _make_compressor() -> _CompressorReady:
                zdict, level = await state.config.train_compression_dict_high_watermark(
                    samples
                )
                return _CompressorReady(
                    type=_CompressorState.READY,
                    dictionary_id=dictionary_id,
                    level=level,
                    data=zdict,
                )

            state.last_compressor = state.active_compressor
            state.active_compressor = _CompressorPreparing(
                type=_CompressorState.PREPARING,
                dictionary_id=dictionary_id,
                task=asyncio.create_task(_make_compressor()),
            )
            state.training_data.collector.tmpfile.close()
            state.training_data = _CompressorTrainingInfoWaitingToRefresh(
                type=_CompressorTrainingInfoType.WAITING_TO_REFRESH,
                last_refreshed_at=time.time(),
                dirty=False,
            )
            return state

    return state


async def _make_receive_stream_message_prefix(
    state: _StateOpen,
    topic: bytes,
    sha512: bytes,
    msg_identifier: bytes,
    part_id: int,
    dictionary_id: int,
    compressed_length: int,
    deflated_length: int,
) -> bytes:
    authorization = await state.config.setup_authorization(
        url=_make_for_send_websocket_url_and_change_counter(state),
        topic=topic,
        message_sha512=sha512,
        now=time.time(),
    )
    return _make_websocket_message(
        _ParsedWSMessageFlags.MINIMAL_HEADERS,
        _OutgoingParsedWSMessageType.RECEIVE_STREAM,
        [
            *(
                [("authorization", authorization.encode("utf-8"))]
                if authorization is not None
                else []
            ),
            ("x-identifier", msg_identifier),
            (
                "x-part-id",
                part_id.to_bytes(_smallest_unsigned_size(part_id), "big"),
            ),
            *(
                []
                if part_id != 0
                else [
                    ("x-topic", topic),
                    (
                        "x-compressor",
                        dictionary_id.to_bytes(
                            _smallest_unsigned_size(dictionary_id),
                            "big",
                        ),
                    ),
                    (
                        "x-compressed-length",
                        compressed_length.to_bytes(
                            _smallest_unsigned_size(compressed_length),
                            "big",
                        ),
                    ),
                    (
                        "x-deflated-length",
                        deflated_length.to_bytes(
                            _smallest_unsigned_size(deflated_length),
                            "big",
                        ),
                    ),
                    ("x-repr-digest", sha512),
                ]
            ),
        ],
        b"",
    )


async def _send_large_compressed_message_optimistically(
    state: _StateOpen,
    msg: Union[_LargeMessage, _LargeSpooledMessage],
    *,
    dictionary: "Optional[zstandard.ZstdCompressionDict]",
    dictionary_id: int,
    level: int = 3,
) -> None:
    """The implementation of _send_large_message_optimistically when we are compressing
    the payload. Since we have to do a pass through the data anyway to compress, we copy
    the compressed data over to a tempfile before sending, which lets us release the original
    handle without waiting for socket io (while still being much more efficient than if the
    copy had been done by the caller without compression)
    """
    capturing = _should_store_large_message_for_training(state, msg)

    handle_capture: Callable[[bytes], int] = lambda _: 0

    if capturing:

        assert state.training_data is not None
        assert (
            state.training_data.type != _CompressorTrainingInfoType.WAITING_TO_REFRESH
        )

        capturing_tmpfile = state.training_data.collector.tmpfile

        state.training_data.collector.length += msg.length
        state.training_data.collector.messages += 1

        capturing_tmpfile.write(msg.length.to_bytes(4, "big"))
        handle_capture = capturing_tmpfile.write

    with tempfile.TemporaryFile("w+b", buffering=0) as compressed_file:
        zcompress = zstandard.ZstdCompressor(
            level=level,
            dict_data=dictionary,
            write_checksum=False,
            write_content_size=False,
            write_dict_id=False,
        )

        chunker = zcompress.chunker(size=msg.length, chunk_size=io.DEFAULT_BUFFER_SIZE)
        while True:
            chunk = msg.stream.read(io.DEFAULT_BUFFER_SIZE)
            if not chunk:
                break

            handle_capture(chunk)
            for compressed_chunk in chunker.compress(chunk):
                compressed_file.write(compressed_chunk)

            await asyncio.sleep(0)

        msg.finished.set()

        for compressed_chunk in chunker.finish():
            compressed_file.write(compressed_chunk)

        await asyncio.sleep(0)

        compressed_length = compressed_file.tell()
        compressed_file.seek(0)
        msg_identifier = secrets.token_bytes(4)
        part_id = 0

        while True:
            headers = await _make_receive_stream_message_prefix(
                state,
                msg.topic,
                msg.sha512,
                msg_identifier,
                part_id,
                dictionary_id,
                compressed_length,
                msg.length,
            )

            remaining_space = (
                max(
                    512,
                    state.config.outgoing_max_ws_message_size - len(headers),
                )
                if state.config.outgoing_max_ws_message_size is not None
                else compressed_length
            )
            part = compressed_file.read(remaining_space)
            if not part:
                break
            await state.websocket.send_bytes(headers + part)
            part_id += 1


async def _send_large_message_optimistically(
    state: _StateOpen, msg: _LargeMessage
) -> None:
    """A target for a task in send_task that must urgently call msg.finished.set()"""
    if (
        state.config.compression_allowed
        and msg.length >= state.config.compression_trained_max_size
    ):
        return await _send_large_compressed_message_optimistically(
            state, msg, dictionary=None, dictionary_id=1
        )

    if (
        state.active_compressor is not None
        and state.active_compressor.type == _CompressorState.READY
        and msg.length >= state.config.compression_min_size
    ):
        return await _send_large_compressed_message_optimistically(
            state,
            msg,
            dictionary=state.active_compressor.data,
            dictionary_id=state.active_compressor.dictionary_id,
            level=state.active_compressor.level,
        )

    capturing = _should_store_large_message_for_training(state, msg)

    handle_capture: Callable[[bytes], int] = lambda _: 0

    if capturing:

        assert state.training_data is not None
        assert (
            state.training_data.type != _CompressorTrainingInfoType.WAITING_TO_REFRESH
        )

        capturing_tmpfile = state.training_data.collector.tmpfile

        state.training_data.collector.length += msg.length
        state.training_data.collector.messages += 1

        capturing_tmpfile.write(msg.length.to_bytes(4, "big"))
        handle_capture = capturing_tmpfile.write

    spool_timeout = (
        asyncio.create_task(
            asyncio.sleep(state.config.websocket_large_direct_send_timeout)
        )
        if state.config.websocket_large_direct_send_timeout is not None
        else asyncio.Future()
    )

    sender: Optional[asyncio.Task[None]] = None
    msg_identifier = secrets.token_bytes(4)
    part_id = 0
    max_ws_msg_size = (
        2**64 - 1
        if state.config.outgoing_max_ws_message_size is None
        else state.config.outgoing_max_ws_message_size
    )
    while True:
        headers = await _make_receive_stream_message_prefix(
            state,
            msg.topic,
            msg.sha512,
            msg_identifier,
            part_id,
            0,
            msg.length,
            msg.length,
        )

        remaining_ws_msg_space = max(512, max_ws_msg_size - len(headers))

        chunk = msg.stream.read(remaining_ws_msg_space)
        if not chunk:
            msg.finished.set()
            spool_timeout.cancel()
            return

        handle_capture(chunk)
        sender = asyncio.create_task(state.websocket.send_bytes(headers + chunk))
        part_id += 1
        await asyncio.wait([sender, spool_timeout], return_when=asyncio.FIRST_COMPLETED)
        if sender.done():
            sender.result()
            sender = None
            continue

        break

    # timeout reached while sender is not done, respool the remainder so we can release the message
    assert sender is not None, "impossible"

    with tempfile.TemporaryFile("w+b", buffering=-1) as target:
        while True:
            chunk = msg.stream.read(io.DEFAULT_BUFFER_SIZE)
            if not chunk:
                break

            handle_capture(chunk)
            target.write(chunk)
            await asyncio.sleep(0)

        msg.finished.set()
        target.seek(0)

        await sender

        while True:
            headers = await _make_receive_stream_message_prefix(
                state,
                msg.topic,
                msg.sha512,
                msg_identifier,
                part_id,
                0,
                msg.length,
                msg.length,
            )

            remaining_ws_msg_space = max(512, max_ws_msg_size - len(headers))
            part = target.read(remaining_ws_msg_space)
            if not part:
                break

            part_id += 1
            await state.websocket.send_bytes(headers + part)


async def _send_large_message_from_spooled(
    state: _StateOpen, msg: _LargeSpooledMessage
) -> None:
    """A target for a task in send_task where we have as long as we need to call msg.finished.set()"""
    if (
        state.config.compression_allowed
        and msg.length >= state.config.compression_trained_max_size
    ):
        return await _send_large_compressed_message_optimistically(
            state, msg, dictionary=None, dictionary_id=1
        )

    if (
        state.active_compressor is not None
        and state.active_compressor.type == _CompressorState.READY
        and msg.length >= state.config.compression_min_size
    ):
        return await _send_large_compressed_message_optimistically(
            state,
            msg,
            dictionary=state.active_compressor.data,
            dictionary_id=state.active_compressor.dictionary_id,
            level=state.active_compressor.level,
        )

    capturing = _should_store_large_message_for_training(state, msg)

    handle_capture: Callable[[bytes], int] = lambda _: 0

    if capturing:

        assert state.training_data is not None
        assert (
            state.training_data.type != _CompressorTrainingInfoType.WAITING_TO_REFRESH
        )

        capturing_tmpfile = state.training_data.collector.tmpfile

        state.training_data.collector.length += msg.length
        state.training_data.collector.messages += 1

        capturing_tmpfile.write(msg.length.to_bytes(4, "big"))
        handle_capture = capturing_tmpfile.write

    msg_identifier = secrets.token_bytes(4)
    part_id = 0
    max_ws_msg_size = (
        2**64 - 1
        if state.config.outgoing_max_ws_message_size is None
        else state.config.outgoing_max_ws_message_size
    )
    while True:
        headers = await _make_receive_stream_message_prefix(
            state,
            msg.topic,
            msg.sha512,
            msg_identifier,
            part_id,
            0,
            msg.length,
            msg.length,
        )

        remaining_ws_msg_space = max(512, max_ws_msg_size - len(headers))

        chunk = msg.stream.read(remaining_ws_msg_space)
        if not chunk:
            msg.finished.set()
            return

        handle_capture(chunk)
        await state.websocket.send_bytes(headers + chunk)
        part_id += 1


def _spool_large_message_immediately(
    msg: _LargeMessage,
) -> Tuple[_LargeSpooledMessage, asyncio.Task[None]]:
    target = tempfile.TemporaryFile("w+b", buffering=-1)
    try:
        while True:
            chunk = msg.stream.read(io.DEFAULT_BUFFER_SIZE)
            if not chunk:
                break

            target.write(chunk)
        target.seek(0)

        finished = asyncio.Event()

        async def _background() -> None:
            try:
                await finished.wait()
            finally:
                target.close()

        return (
            _LargeSpooledMessage(
                type=_MessageType.LARGE,
                stream=target,
                topic=msg.topic,
                sha512=msg.sha512,
                length=msg.length,
                finished=finished,
            ),
            asyncio.create_task(_background()),
        )
    except BaseException:
        target.close()
        raise


async def _send_small_message(state: _StateOpen, msg: _SmallMessage) -> None:
    """Target for send_task with a small message"""
    _store_small_for_compression_training(state, msg.data)

    remaining = msg.data
    msg_identifier = secrets.token_bytes(4)
    part_id = 0

    compressor_id: int = 0
    deflated_length = len(remaining)

    if state.config.compression_allowed:
        if (
            len(remaining) >= state.config.compression_min_size
            and len(remaining) < state.config.compression_trained_max_size
            and state.active_compressor is not None
            and state.active_compressor.type == _CompressorState.READY
        ):
            compressor_id = state.active_compressor.dictionary_id
            remaining = zstandard.ZstdCompressor(
                level=state.active_compressor.level,
                dict_data=state.active_compressor.data,
                write_checksum=False,
                write_content_size=False,
                write_dict_id=False,
            ).compress(remaining)
        elif len(remaining) >= state.config.compression_trained_max_size:
            compressor_id = 1
            remaining = zstandard.ZstdCompressor(
                level=3,
                write_checksum=False,
                write_content_size=False,
                write_dict_id=False,
            ).compress(remaining)

    compressed_length = len(remaining)

    while remaining:
        headers = await _make_receive_stream_message_prefix(
            state,
            msg.topic,
            msg.sha512,
            msg_identifier,
            part_id,
            compressor_id,
            compressed_length,
            deflated_length,
        )

        remaining_space = (
            len(remaining)
            if state.config.outgoing_max_ws_message_size is None
            else max(
                512,
                state.config.outgoing_max_ws_message_size - len(headers),
            )
        )
        part, remaining = (
            remaining[:remaining_space],
            remaining[remaining_space:],
        )
        await state.websocket.send_bytes(headers + part)
        part_id += 1


async def _handle_open(state: _State) -> _State:
    assert state.type == _StateType.OPEN
    try:
        if state.send_task is None and state.pending_sends:
            next_send = state.pending_sends.popleft()

            if next_send.type == _MessageType.SMALL:
                state.send_task = asyncio.create_task(
                    _send_small_message(state, next_send)
                )
            elif next_send.type == _MessageType.LARGE:
                state.send_task = asyncio.create_task(
                    _send_large_message_from_spooled(state, next_send)
                )
            else:
                assert next_send.type == _MessageType.FORMATTED
                state.send_task = asyncio.create_task(
                    state.websocket.send_bytes(next_send.websocket_data)
                )

            return state

        if state.send_task is not None and state.send_task.done():
            state.send_task.result()
            state.send_task = None
            return state

        if (
            state.active_compressor is not None
            and state.active_compressor.type == _CompressorState.PREPARING
            and state.active_compressor.task.done()
        ):
            state.active_compressor = state.active_compressor.task.result()
            return state

        if state.message_task.done():
            msg = state.message_task.result()
            state.message_task = asyncio.create_task(state.my_receiver.queue.get())

            if state.send_task is None:
                if msg.type == _MessageType.SMALL:
                    state.send_task = asyncio.create_task(
                        _send_small_message(state, msg)
                    )
                else:
                    state.send_task = asyncio.create_task(
                        _send_large_message_optimistically(state, msg)
                    )
                return state

            if msg.type == _MessageType.SMALL:
                state.pending_sends.append(msg)
                return state

            spooled, bknd = _spool_large_message_immediately(msg)
            msg.finished.set()
            state.pending_sends.append(spooled)
            state.backgrounded.add(bknd)
            return state

        if (
            state.last_compressor is not None
            and state.last_compressor.type == _CompressorState.PREPARING
            and state.last_compressor.task.done()
        ):
            state.last_compressor = state.last_compressor.task.result()
            return state

        if state.read_task.done():
            raw_message = state.read_task.result()
            if raw_message["type"] == "websocket.disconnect":
                return await _cleanup_open(state, None)

            if "bytes" not in raw_message:
                return await _cleanup_open(
                    state, ValueError("only bytes or close messages expected")
                )

            raw_message = cast(WSMessageBytes, raw_message)
            parsed_message = _parse_websocket_message(raw_message["bytes"])

            if parsed_message.type == _ParsedWSMessageType.CONFIGURE:
                if state.configuration is not None:
                    return await _cleanup_open(
                        state, ValueError("configuration already set")
                    )
                rdr = io.BytesIO(parsed_message.body)
                subscriber_nonce = _exact_read(rdr, 32)
                flags = _ConfigurationFlags(int.from_bytes(_exact_read(rdr, 4), "big"))
                dictionary_id = int.from_bytes(_exact_read(rdr, 2), "big")

                broadcaster_nonce = secrets.token_bytes(32)
                connection_nonce = hashlib.sha256(
                    subscriber_nonce + broadcaster_nonce
                ).digest()
                state.configuration = _Configuration(
                    flags=flags,
                    dictionary_id=dictionary_id,
                    nonce_b64=base64.urlsafe_b64encode(connection_nonce).decode(
                        "ascii"
                    ),
                )

                if (
                    flags & _ConfigurationFlags.ZSTD_TRAINING_ALLOWED
                ) == 0 and state.training_data is not None:
                    if (
                        state.training_data.type
                        == _CompressorTrainingInfoType.BEFORE_LOW_WATERMARK
                    ):
                        state.training_data.collector.tmpfile.close()
                    if (
                        state.training_data.type
                        == _CompressorTrainingInfoType.BEFORE_HIGH_WATERMARK
                    ):
                        state.training_data.collector.tmpfile.close()
                    state.training_data = None

                if (
                    dictionary_id != 0  # "no compression"
                    and dictionary_id != 1  # reserved for not using a dictionary
                    and state.config.compression_allowed
                    and (
                        state.active_compressor is None
                        or dictionary_id != state.active_compressor.dictionary_id
                    )
                ):
                    requested = await state.config.get_compression_dictionary_by_id(
                        dictionary_id
                    )
                    if requested is None:
                        return await _cleanup_open(
                            state, ValueError("dictionary not found")
                        )

                    zdict, level = requested

                    state.last_compressor = state.active_compressor
                    state.active_compressor = _CompressorReady(
                        type=_CompressorState.READY,
                        dictionary_id=dictionary_id,
                        level=level,
                        data=zdict,
                    )

                    state.pending_sends.append(
                        _FormattedMessage(
                            type=_MessageType.FORMATTED,
                            websocket_data=_make_websocket_message(
                                parsed_message.flags,
                                _OutgoingParsedWSMessageType.ENABLE_ZSTD_PRESET,
                                [
                                    ("x-identifier", dictionary_id.to_bytes(2, "big")),
                                    ("x-compression-level", level.to_bytes(2, "big")),
                                ],
                                b"",
                            ),
                        )
                    )

                state.read_task = _make_websocket_read_task(state.websocket)

                state.pending_sends.append(
                    _FormattedMessage(
                        type=_MessageType.FORMATTED,
                        websocket_data=_make_websocket_message(
                            parsed_message.flags,
                            _OutgoingParsedWSMessageType.CONFIRM_CONFIGURE,
                            [("x-broadcaster-nonce", broadcaster_nonce)],
                            b"",
                        ),
                    )
                )
                return state
            else:
                raise NotImplementedError

        if state.training_data is not None and state.training_data.dirty:
            return await _check_training_data(state)

        if state.backgrounded:
            found_done = False
            for task in state.backgrounded:
                if task.done():
                    found_done = True
                    break
            if found_done:
                done, state.backgrounded = await asyncio.wait(
                    state.backgrounded, return_when=asyncio.FIRST_COMPLETED
                )
                for task in done:
                    task.result()

        await asyncio.wait(
            [
                state.read_task,
                state.message_task,
                *([state.send_task] if state.send_task is not None else []),
                *(
                    [state.active_compressor.task]
                    if state.active_compressor is not None
                    and state.active_compressor.type == _CompressorState.PREPARING
                    else []
                ),
                *(
                    [state.last_compressor.task]
                    if state.last_compressor is not None
                    and state.last_compressor.type == _CompressorState.PREPARING
                    else []
                ),
                *state.backgrounded,
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )
        return state
    except BaseException as e:
        return await _cleanup_open(state, e)


async def _cleanup_open(
    state: _StateOpen, exception: Optional[BaseException]
) -> _State:
    state.read_task.cancel()
    state.message_task.cancel()
    if state.send_task is not None:
        state.send_task.cancel()
    if (
        state.active_compressor is not None
        and state.active_compressor.type == _CompressorState.PREPARING
    ):
        state.active_compressor.task.cancel()
    if (
        state.last_compressor is not None
        and state.last_compressor.type == _CompressorState.PREPARING
    ):
        state.last_compressor.task.cancel()

    for task in state.backgrounded:
        task.cancel()

    if state.training_data is not None:
        if state.training_data.type == _CompressorTrainingInfoType.BEFORE_LOW_WATERMARK:
            state.training_data.collector.tmpfile.close()
        elif (
            state.training_data.type
            == _CompressorTrainingInfoType.BEFORE_HIGH_WATERMARK
        ):
            state.training_data.collector.tmpfile.close()

    if state.my_receiver.receiver_id is not None:
        await state.receiver.unregister_receiver(state.my_receiver.receiver_id)

    for exact in state.my_receiver.exact_subscriptions:
        await state.receiver.decrement_exact(exact)

    for _, glob in state.my_receiver.glob_subscriptions:
        await state.receiver.decrement_glob(glob)

    return _StateClosing(
        type=_StateType.CLOSING, websocket=state.websocket, exception=exception
    )


async def _handle_closing(state: _State) -> _State:
    assert state.type == _StateType.CLOSING
    await state.websocket.close()
    if state.exception is not None:
        raise state.exception
    return _StateClosed(type=_StateType.CLOSED)


_HANDLERS: Dict[_StateType, _StateHandler] = {
    _StateType.ACCEPTING: _handle_accepting,
    _StateType.OPEN: _handle_open,
    _StateType.CLOSING: _handle_closing,
}


async def _handle_until_closed(state: _State) -> None:
    while state.type != _StateType.CLOSED:
        handler = _HANDLERS[state.type]
        try:
            state = await handler(state)
        except BaseException as e:
            if state.type != _StateType.CLOSING and state.type != _StateType.CLOSED:
                state = _StateClosing(
                    type=_StateType.CLOSING, websocket=state.websocket, exception=e
                )
            else:
                raise e


@router.websocket("/v1/websocket")
async def websocket_endpoint(websocket: WebSocket) -> None:
    """Allows sending and receiving notifications over a websocket connection,
    as opposed to the typical way this library is used (HTTP requests). This is
    helpful for the following scenarios:

    - You need to send a large number of notifications, OR
    - You need to receive a large number of notifications, OR
    - You need to receive notifications for a short period of time before unsubscribing, OR
    - You need to receive some notifications, but you cannot accept incoming HTTP requests

    For maximum compatibility with websocket clients, we only communicate
    over the websocket itself (not the http-level header fields).

    ## COMPRESSION

    For notifications (both posted and received) over websockets, this supports
    using zstandard compression. It will either use an embedded dictionary, a
    precomputed dictionary, or a trained dictionary. Under the typical settings, this:

    - Only considers messages that are between 128 and 16384 bytes for training
    - Will train once after 100kb of data is ready, and once more after 10mb of data is ready,
      then will sample 10mb every 24 hours
    - Will only used the trained dictionary on messages that would be used for training

    ## MESSAGES

    messages always begin as follows

    - 2 bytes (F): flags (interpret as big-endian):
        - least significant bit (1): 0 if headers are expanded, 1 if headers are minimal
    - 2 bytes (T): type of message; see below, depends on if it's sent by a subscriber
      or the broadcaster big-endian encoded, unsigned

    EXPANDED HEADERS:
        - 2 bytes (N): number of headers, big-endian encoded, unsigned
        - REPEAT N:
            - 2 bytes (M): length of header name, big-endian encoded, unsigned
            - M bytes: header name, ascii-encoded
            - 2 bytes (L): length of header value, big-endian encoded, unsigned
            - L bytes: header value

    MINIMAL HEADERS:
    the order of the headers are fixed based on the type, in the order documented.
    Given N headers:
    - Repeat N:
        - 2 bytes (L): length of header value, big-endian encoded, unsigned
        - L bytes: header value

    ## Messages Sent to the Broadcaster

    0: Configure:
        configures the broadcasters behavior; may be set at most once and must be
        sent and confirmed before doing anything else if the url is relevant for
        the authorization header

        headers:
            - none

        body:
            - 32 bytes (N): subscriber chosen random bytes to contribute to generating the nonce. The connection
                nonce is SHA256(subscriber_nonce CONCAT broadcaster_nonce).
            - 4 bytes (C): configuration flags, big-endian encoded, unsigned. From least to most significant bit:
                - 1: whether zstandard compression is enabled for notify and receive (set: enable, unset: disable).
                - 2: whether training a compression dictionary on the sent and received message bodies may be useful
                    (set: should train, unset: must not train). generally should be set if >50mb of data is expected to be
                    sent and received in <16kb chunks.
            - 2 bytes (D): either 0x00 or the id of the preset compression dictionary to start with
    1: Subscribe:
        subscribe to an exact topic or glob pattern

        headers:
            - authorization (url: websocket:<nonce>:<ctr>, see below)
        body:
            - exact body of /v1/subscribe
    2: Unsubscribe:
        unsubscribe from an exact topic or glob pattern

        headers:
            - authorization (url: websocket:<nonce>:<ctr>, see below)
        body:
            - exact body of /v1/unsubscribe
    3: Notify:
        send a notification within a single websocket message (typically, max 16MB). this
        can be suitable for arbitrary websocket sizes depending on the configuration of the
        broadcaster (e.g., uvicorn and all intermediaries might limit max ws message sizes)

        headers:
            - authorization (url: websocket:<nonce>:<ctr>, see below)
            - x-identifier identifies the notification so we can confirm it
        body:
            - exact body of /v1/notify
    4: Notify Stream:
        send a notification over multiple websocket messages. this is more likely to work on
        typical setups when the notification payload exceeds 16MB.

        headers:
            - authorization (url: websocket:<nonce>:<ctr>, see below)
            - x-part-id starts at 0 and increments by 1 for each part. interpreted unsigned, big-endian, max 8 bytes
            - x-topic iff x-part-id is 0, the topic of the notification
            - x-compressor iff x-part-id is 0, either 0 for no compression, 1
              for zstandard compression without a custom dictionary, and
              otherwise the id of the compressor from one of the
              "Enable X compression" broadcaster->subscriber messages
            - x-compressed-length iff x-part-id is 0, the total length of the compressed body, big-endian, unsigned, max 8 bytes
            - x-deflated-length iff x-part-id is 0, the total length of the deflated body, big-endian, unsigned, max 8 bytes
            - x-repr-digest iff x-part-id is 0, the sha-512 hash of the deflated content once all parts are concatenated, 64 bytes
            - x-identifier identifies the notify whose compressed body is being appended. arbitrary blob, max 64 bytes

        body:
            - blob of data to append to the compressed notification body

    ## Messages Sent to the Subscriber

    0: Configure Confirmation:
        confirms we received the configuration options from the subscriber

        headers:
            - `x-broadcaster-nonce`: (32 bytes)
                the broadcasters contribution for random bytes to the nonce.
                the connection nonce is SHA256(subscriber_nonce CONCAT broadcaster_nonce),
                which is used in the url for generating the authorization header
                when the broadcaster sends a notification to the receiver over
                this websocket and when the subscriber subscribers to a topic over
                this websocket.

                the url is of the form `websocket:<nonce>:<ctr>`, where the ctr is
                a signed 8-byte integer that starts at 1 (or -1) and that depends on if it
                was sent by the broadcaster or subscriber. Both the subscriber and
                broadcaster keep track of both counters; the subscribers counter
                is always negative and decremented by 1 after each subscribe or unsubscribe
                request, the broadcasters counter is always positive and incremented by 1 after
                each notification sent. The nonce is base64url encoded, the ctr is
                hex encoded without a leading 0x and unpadded, e.g.,
                `websocket:abc123:10ffffffffffffff` or `websocket:abc123:-1a`. note that
                the counter changes every time an authorization header is provided,
                even within a single "operation", so e.g. a Notify Stream message broken
                into 6 parts will change the counter 6 times.

    1: Subscribe Exact Confirmation:
        confirms that the subscriber will receive notifications for the given topic

        headers:
            - x-topic: the topic that the subscriber is now subscribed to

        body: none
    2. Subscribe Glob Confirmation:
        confirms that the subscriber will receive notifications for the given glob pattern

        headers:
            - x-glob: the pattern that the subscriber is now subscribed to

        body: none
    3: Unsubscribe Exact Confirmation:
        confirms that the subscriber will no longer receive notifications for the given topic

        headers:
            - x-topic: the topic that the subscriber is now unsubscribed from

        body: none
    4: Unsubscribe Glob Confirmation:
        confirms that the subscriber will no longer receive notifications for the given glob pattern

        headers:
            - x-glob: the pattern that the subscriber is now unsubscribed from

        body: none
    5: Notify Confirmation:
        confirms that we sent a notification to subscribers; this is also sent
        for streamed notifications after the last part was received by the broadcaster

        headers:
            - x-identifier: the identifier of the notification that was sent
            - x-subscribers: the number of subscribers that received the notification

        body: none
    6: Notify Continue:
        confirms that we received a part of a streamed notification but need more. You
        do not need to wait for this before continuing, and should never retry WS messages
        as the underlying protocol already handles retries. to abort a send, close the WS
        and reconnect

        headers:
            - x-identifier: the identifier of the notification we need more parts for
            - x-part-id: the part id that we received up to, big-endian, unsigned

        body: none
    7: Receive Stream
        tells the subscriber about a notification on a topic they are subscribed to, possibly
        over multiple messages

        headers:
            - authorization (url: websocket:<nonce>:<ctr>, see above)
            - x-identifier identifies the notify whose compressed body is being appended. arbitrary blob, max 64 bytes
            - x-part-id starts at 0 and increments by 1 for each part. interpreted unsigned, big-endian, max 8 bytes
            - x-topic iff x-part-id is 0, the topic of the notification
            - x-compressor iff x-part-id is 0, either 0 for no compression, 1 for no custom dictionary zstd, and
              otherwise the id of the compressor from one of
              the "Enable X compression" broadcaster->subscriber messages
            - x-compressed-length iff x-part-id is 0, the total length of the compressed body, big-endian, unsigned, max 8 bytes
            - x-deflated-length iff x-part-id is 0, the total length of the deflated body, big-endian, unsigned, max 8 bytes
            - x-repr-digest iff x-part-id is 0, the sha-512 hash of the deflated content once all parts are concatenated, 64 bytes

        body:
            - blob of data to append to the compressed notification body
    8-99: Reserved
    100: Enable zstandard compression with preset dictionary
        configures the subscriber to expect and use a dictionary that it already has available.
        this may use precomputed dictionaries that were specified during the broadcaster's
        configuration with the assumption the subscriber has them

        headers:
            x-identifier: which compressor is enabled, unsigned, big-endian, max 2 bytes, min 1
            x-compression-level: what compression level we think is best when using
                this dictionary. signed, big-endian, max 2 bytes, max 22. the subscriber
                is free to choose a different compression level

        body: none
    101: Enable zstandard compression with a custom dictionary
        configures the subscriber to use a dictionary we just trained

        headers:
            x-identifier: the id we are assigning to this dictionary, unsigned, big-endian, max 8 bytes,
                min 65536. if not unique, overwrite the previous dictionary
            x-compression-level: what compression level we think is best when using
                this dictionary. signed, big-endian, max 2 bytes, max 22. the subscriber
                is free to choose a different compression level

        body: the dictionary, max 15MB, typically ~16kb. may be length 0 to
            indicate we no longer want to use this dictionary
    """
    config = get_config_from_request(websocket)
    receiver = get_ws_receiver_from_request(websocket)
    await _handle_until_closed(
        _StateAccepting(
            type=_StateType.ACCEPTING,
            websocket=websocket,
            config=config,
            receiver=receiver,
        )
    )
