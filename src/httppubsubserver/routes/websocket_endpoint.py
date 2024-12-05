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
    Coroutine,
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
    IO,
)
import aiohttp
from fastapi import APIRouter, WebSocket
from dataclasses import dataclass
from collections import deque
from enum import IntFlag, IntEnum, Enum, auto
from httppubsubserver.config.config import Config
from httppubsubserver.middleware.config import get_config_from_request
from httppubsubserver.middleware.ws_receiver import get_ws_receiver_from_request
from httppubsubserver.routes.notify import (
    TrustedNotifyResultType,
    handle_trusted_notify,
)
from httppubsubserver.routes.subscribe import SubscribeType, parse_subscribe_payload
from httppubsubserver.util.websocket_message import (
    WSMessage,
    WSMessageBytes,
)
import asyncio

from httppubsubserver.util.ws_receiver import BaseWSReceiver, FanoutWSReceiver
from httppubsubserver.util.sync_io import (
    SyncReadableBytesIO,
    SyncIOBaseLikeIO,
    read_exact,
)

try:
    import zstandard
except ImportError:
    ...


try:
    from glob import translate as _glob_translate  # type: ignore

    def translate(pat: str) -> str:
        return _glob_translate(pat, recursive=True, include_hidden=True)

except ImportError:
    from fnmatch import translate


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


_STANDARD_MINIMAL_HEADERS_BY_TYPE: Dict[_ParsedWSMessageType, List[str]] = {
    _ParsedWSMessageType.CONFIGURE: [],
    _ParsedWSMessageType.SUBSCRIBE: ["authorization"],
    _ParsedWSMessageType.UNSUBSCRIBE: ["authorization"],
    _ParsedWSMessageType.NOTIFY: ["authorization", "x-identifier", "x-compressor"],
}


def _parse_websocket_message(body: bytes) -> _ParsedWSMessage:
    stream = io.BytesIO(body)
    flags = _ParsedWSMessageFlags(int.from_bytes(read_exact(stream, 2), "big"))
    message_type = _ParsedWSMessageType(int.from_bytes(read_exact(stream, 2), "big"))

    headers: Dict[str, bytes] = {}
    if flags & _ParsedWSMessageFlags.MINIMAL_HEADERS:
        if message_type in _STANDARD_MINIMAL_HEADERS_BY_TYPE:
            minimal_headers = _STANDARD_MINIMAL_HEADERS_BY_TYPE[message_type]
        if message_type == _ParsedWSMessageType.NOTIFY_STREAM:
            length = int.from_bytes(read_exact(stream, 2), "big")
            headers["authorization"] = read_exact(stream, length)

            length = int.from_bytes(read_exact(stream, 2), "big")
            if length > 64:
                raise ValueError("message id max 64 bytes")
            headers["x-identifier"] = read_exact(stream, length)

            length = int.from_bytes(read_exact(stream, 2), "big")
            if length > 8:
                raise ValueError("part id max 8 bytes")
            part_id_bytes = read_exact(stream, length)
            headers["x-part-id"] = part_id_bytes

            part_id = int.from_bytes(part_id_bytes, "big")
            if part_id == 0:
                minimal_headers = [
                    "x-topic",
                    "x-compressor",
                    "x-compressed-length",
                    "x-deflated-length",
                    "x-compressed-sha512",
                ]
            else:
                minimal_headers = ["x-identifier"]

        for header in minimal_headers:
            length = int.from_bytes(read_exact(stream, 2), "big")
            headers[header] = read_exact(stream, length)
    else:
        num_headers = int.from_bytes(read_exact(stream, 2), "big")
        for _ in range(num_headers):
            name_length = int.from_bytes(read_exact(stream, 2), "big")
            name_enc = read_exact(stream, name_length)
            name = name_enc.decode("ascii").lower()
            value_length = int.from_bytes(read_exact(stream, 2), "big")
            value = read_exact(stream, value_length)
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
    data: "Optional[zstandard.ZstdCompressionDict]"
    compressor: "zstandard.ZstdCompressor"
    decompressor: "zstandard.ZstdDecompressor"


@dataclass
class _CompressorPreparing:
    type: Literal[_CompressorState.PREPARING]
    dictionary_id: int
    task: asyncio.Task[_CompressorReady]


_Compressor = Union[_CompressorReady, _CompressorPreparing]


@dataclass
class _ReceivingNotify:
    """A notification we are in the process of receiving"""

    identifier: bytes
    """The message id for this notification"""

    last_part_id: int
    topic: bytes
    compressor_id: int
    compressed_length: int
    deflated_length: int
    compressed_sha512: bytes

    body_hasher: "hashlib._Hash"
    body: SyncIOBaseLikeIO


@dataclass
class _StateOpen:
    type: Literal[_StateType.OPEN]
    websocket: WebSocket
    broadcaster_config: Config
    receiver: FanoutWSReceiver
    client_session: aiohttp.ClientSession
    standard_compressor: Optional[_CompressorReady]
    """The compressor when not using a custom dictionary"""

    socket_level_config: Optional[_Configuration]
    my_receiver: _MyReceiver

    read_task: asyncio.Task[WSMessage]
    send_task: Optional[asyncio.Task[None]]
    process_task: Optional[asyncio.Task[None]]
    """If we are currently processing an incoming WSMessage, the corresponding task,
    otherwise, None
    """
    message_task: asyncio.Task[Union[_SmallMessage, _LargeMessage]]
    pending_sends: deque[Union[_SmallMessage, _LargeSpooledMessage, _FormattedMessage]]
    """If we can't push a message to the send_task immediately, we move it here.
    When we move large messages to this queue, we spool them to file
    """
    unprocessed_receives: deque[_ParsedWSMessage]
    """If we receive a message but can't set process_task immediately, we move it here."""
    incoming_notification: Optional[_ReceivingNotify]
    """If the subscriber is streaming us a notification, the current object
    tracking the state of that stream, otherwise None to indicate no notification
    is in the process of being received
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
        broadcaster_config=state.config,
        receiver=state.receiver,
        client_session=aiohttp.ClientSession(),
        standard_compressor=None,
        socket_level_config=None,
        my_receiver=my_receiver,
        read_task=_make_websocket_read_task(state.websocket),
        send_task=None,
        process_task=None,
        message_task=asyncio.create_task(my_receiver.queue.get()),
        pending_sends=deque(maxlen=state.config.websocket_max_pending_sends),
        unprocessed_receives=deque(
            maxlen=state.config.websocket_max_unprocessed_receives
        ),
        incoming_notification=None,
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
    assert state.socket_level_config is not None
    ctr = state.broadcaster_counter
    state.broadcaster_counter += 1
    return f"websocket:{state.socket_level_config.nonce_b64}:{ctr:x}"


def _make_for_receive_websocket_url_and_change_counter(state: _StateOpen) -> str:
    assert state.socket_level_config is not None
    ctr = state.subscriber_counter
    state.subscriber_counter -= 1
    return f"websocket:{state.socket_level_config.nonce_b64}:{ctr:x}"


def _handle_if_should_start_retraining(state: _StateOpen) -> None:
    if state.training_data is None:
        return

    if state.training_data.type != _CompressorTrainingInfoType.WAITING_TO_REFRESH:
        return

    next_refresh = (
        state.training_data.last_refreshed_at
        + state.broadcaster_config.compression_retrain_interval_seconds
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
    if state.broadcaster_config.compression_min_size > length:
        # this data is too small for compression to be useful
        return

    if state.broadcaster_config.compression_trained_max_size <= length:
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

    if state.broadcaster_config.compression_trained_max_size <= msg.length:
        # this data is too large to benefit from precomputing the compression dictionary
        # (this is what we expect, since we spooled to file)
        return False

    if state.broadcaster_config.compression_min_size > msg.length:
        # this data is too small for compression to be useful (this is absurd given we spooled)
        return False

    _handle_if_should_start_retraining(state)
    if state.training_data.type == _CompressorTrainingInfoType.WAITING_TO_REFRESH:
        return False

    return True


async def _error_guard(task: asyncio.Task[_CompressorReady]) -> None:
    try:
        await task
    except BaseException:
        ...


def _rotate_compressor(state: _StateOpen, new_compressor: _Compressor) -> None:
    if (
        state.last_compressor is not None
        and state.last_compressor.type == _CompressorState.PREPARING
    ):
        state.last_compressor.task.cancel()

        state.backgrounded.add(
            asyncio.create_task(_error_guard(state.last_compressor.task))
        )

    state.last_compressor = state.active_compressor
    state.active_compressor = new_compressor


async def _check_training_data(state: _StateOpen) -> _State:
    if state.training_data is None:
        return state

    if state.training_data.type == _CompressorTrainingInfoType.BEFORE_LOW_WATERMARK:
        if (
            state.training_data.collector.length
            >= state.broadcaster_config.compression_training_high_watermark
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
            < state.broadcaster_config.compression_training_low_watermark
        ):
            state.training_data.dirty = False
            return state

        samples: List[bytes] = []
        state.training_data.collector.tmpfile.seek(0)
        while True:
            length_bytes = state.training_data.collector.tmpfile.read(4)
            if not length_bytes:
                break
            assert len(length_bytes) == 4
            length = int.from_bytes(length_bytes, "big")
            samples.append(read_exact(state.training_data.collector.tmpfile, length))

        dictionary_id = state.custom_compression_dict_counter
        state.custom_compression_dict_counter += 1

        async def _make_compressor() -> _CompressorReady:
            zdict, level = (
                await state.broadcaster_config.train_compression_dict_low_watermark(
                    samples
                )
            )
            return _CompressorReady(
                type=_CompressorState.READY,
                dictionary_id=dictionary_id,
                level=level,
                data=zdict,
                compressor=zstandard.ZstdCompressor(
                    level=level,
                    dict_data=zdict,
                    write_checksum=False,
                    write_content_size=False,
                    write_dict_id=False,
                ),
                decompressor=zstandard.ZstdDecompressor(
                    dict_data=zdict,
                    max_window_size=state.broadcaster_config.decompression_max_window_size,
                ),
            )

        _rotate_compressor(
            state,
            _CompressorPreparing(
                type=_CompressorState.PREPARING,
                dictionary_id=dictionary_id,
                task=asyncio.create_task(_make_compressor()),
            ),
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
            < state.broadcaster_config.compression_training_high_watermark
        ):
            state.training_data.dirty = False
            return state

        samples = []
        state.training_data.collector.tmpfile.seek(0)
        while True:
            length_bytes = state.training_data.collector.tmpfile.read(4)
            if not length_bytes:
                break
            assert len(length_bytes) == 4
            length = int.from_bytes(length_bytes, "big")
            samples.append(read_exact(state.training_data.collector.tmpfile, length))

        dictionary_id = state.custom_compression_dict_counter
        state.custom_compression_dict_counter += 1

        async def _make_compressor() -> _CompressorReady:
            zdict, level = (
                await state.broadcaster_config.train_compression_dict_high_watermark(
                    samples
                )
            )
            return _CompressorReady(
                type=_CompressorState.READY,
                dictionary_id=dictionary_id,
                level=level,
                data=zdict,
                compressor=zstandard.ZstdCompressor(
                    level=level,
                    dict_data=zdict,
                    write_checksum=False,
                    write_content_size=False,
                    write_dict_id=False,
                ),
                decompressor=zstandard.ZstdDecompressor(
                    dict_data=zdict,
                    max_window_size=state.broadcaster_config.decompression_max_window_size,
                ),
            )

        _rotate_compressor(
            state,
            _CompressorPreparing(
                type=_CompressorState.PREPARING,
                dictionary_id=dictionary_id,
                task=asyncio.create_task(_make_compressor()),
            ),
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
    compressed_sha512: bytes,
    msg_identifier: bytes,
    part_id: int,
    dictionary_id: int,
    compressed_length: int,
    deflated_length: int,
) -> bytes:
    authorization = await state.broadcaster_config.setup_authorization(
        url=_make_for_send_websocket_url_and_change_counter(state),
        topic=topic,
        message_sha512=compressed_sha512,
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
                    ("x-compressed-sha512", compressed_sha512),
                ]
            ),
        ],
        b"",
    )


async def _send_large_compressed_message_optimistically(
    state: _StateOpen,
    msg: Union[_LargeMessage, _LargeSpooledMessage],
    *,
    compressor: "zstandard.ZstdCompressor",
    compressor_id: int,
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
        chunker = compressor.chunker(size=msg.length, chunk_size=io.DEFAULT_BUFFER_SIZE)
        hasher = hashlib.sha512()
        while True:
            chunk = msg.stream.read(io.DEFAULT_BUFFER_SIZE)
            if not chunk:
                break

            handle_capture(chunk)
            for compressed_chunk in chunker.compress(chunk):
                compressed_file.write(compressed_chunk)
                hasher.update(compressed_chunk)

            await asyncio.sleep(0)

        msg.finished.set()

        for compressed_chunk in chunker.finish():
            compressed_file.write(compressed_chunk)
            hasher.update(compressed_chunk)

        compressed_sha512 = hasher.digest()
        await asyncio.sleep(0)

        compressed_length = compressed_file.tell()
        compressed_file.seek(0)
        msg_identifier = secrets.token_bytes(4)
        part_id = 0

        while True:
            headers = await _make_receive_stream_message_prefix(
                state,
                msg.topic,
                compressed_sha512,
                msg_identifier,
                part_id,
                compressor_id,
                compressed_length,
                msg.length,
            )

            remaining_space = (
                max(
                    512,
                    state.broadcaster_config.outgoing_max_ws_message_size
                    - len(headers),
                )
                if state.broadcaster_config.outgoing_max_ws_message_size is not None
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
    assert state.socket_level_config is not None, "not configured"

    if (
        state.broadcaster_config.compression_allowed
        and (state.socket_level_config.flags & _ConfigurationFlags.ZSTD_ENABLED) != 0
        and msg.length >= state.broadcaster_config.compression_trained_max_size
        and state.standard_compressor is not None
    ):
        return await _send_large_compressed_message_optimistically(
            state, msg, compressor=state.standard_compressor.compressor, compressor_id=1
        )

    if (
        state.active_compressor is not None
        and state.active_compressor.type == _CompressorState.READY
        and msg.length >= state.broadcaster_config.compression_min_size
        and msg.length < state.broadcaster_config.compression_trained_max_size
    ):
        return await _send_large_compressed_message_optimistically(
            state,
            msg,
            compressor=state.active_compressor.compressor,
            compressor_id=state.active_compressor.dictionary_id,
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
            asyncio.sleep(state.broadcaster_config.websocket_large_direct_send_timeout)
        )
        if state.broadcaster_config.websocket_large_direct_send_timeout is not None
        else asyncio.Future()
    )

    sender: Optional[asyncio.Task[None]] = None
    msg_identifier = secrets.token_bytes(4)
    part_id = 0
    max_ws_msg_size = (
        2**64 - 1
        if state.broadcaster_config.outgoing_max_ws_message_size is None
        else state.broadcaster_config.outgoing_max_ws_message_size
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
        state.standard_compressor is not None
        and msg.length >= state.broadcaster_config.compression_trained_max_size
    ):
        return await _send_large_compressed_message_optimistically(
            state, msg, compressor=state.standard_compressor.compressor, compressor_id=1
        )

    if (
        state.active_compressor is not None
        and state.active_compressor.type == _CompressorState.READY
        and msg.length >= state.broadcaster_config.compression_min_size
    ):
        return await _send_large_compressed_message_optimistically(
            state,
            msg,
            compressor=state.active_compressor.compressor,
            compressor_id=state.active_compressor.dictionary_id,
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
        if state.broadcaster_config.outgoing_max_ws_message_size is None
        else state.broadcaster_config.outgoing_max_ws_message_size
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
    assert state.socket_level_config is not None, "not configured"
    _store_small_for_compression_training(state, msg.data)

    remaining = msg.data
    msg_identifier = secrets.token_bytes(4)
    part_id = 0

    compressor_id: int = 0
    deflated_length = len(remaining)
    compressed_sha512 = msg.sha512

    if (
        len(remaining) >= state.broadcaster_config.compression_min_size
        and len(remaining) < state.broadcaster_config.compression_trained_max_size
        and state.active_compressor is not None
        and state.active_compressor.type == _CompressorState.READY
    ):
        compressor_id = state.active_compressor.dictionary_id
        remaining = state.active_compressor.compressor.compress(remaining)
        compressed_sha512 = hashlib.sha512(remaining).digest()
    elif (
        len(remaining) >= state.broadcaster_config.compression_trained_max_size
        and state.standard_compressor is not None
    ):
        compressor_id = 1
        remaining = state.standard_compressor.compressor.compress(remaining)
        compressed_sha512 = hashlib.sha512(remaining).digest()

    compressed_length = len(remaining)

    while remaining:
        headers = await _make_receive_stream_message_prefix(
            state,
            msg.topic,
            compressed_sha512,
            msg_identifier,
            part_id,
            compressor_id,
            compressed_length,
            deflated_length,
        )

        remaining_space = (
            len(remaining)
            if state.broadcaster_config.outgoing_max_ws_message_size is None
            else max(
                512,
                state.broadcaster_config.outgoing_max_ws_message_size - len(headers),
            )
        )
        part, remaining = (
            remaining[:remaining_space],
            remaining[remaining_space:],
        )
        await state.websocket.send_bytes(headers + part)
        part_id += 1


async def _process_configure(state: _StateOpen, message: _ParsedWSMessage) -> None:
    assert message.type == _ParsedWSMessageType.CONFIGURE
    if state.socket_level_config is not None:
        raise ValueError("configuration already set")
    rdr = io.BytesIO(message.body)
    subscriber_nonce = read_exact(rdr, 32)
    flags = _ConfigurationFlags(int.from_bytes(read_exact(rdr, 4), "big"))
    dictionary_id = int.from_bytes(read_exact(rdr, 2), "big")

    broadcaster_nonce = secrets.token_bytes(32)
    connection_nonce = hashlib.sha256(subscriber_nonce + broadcaster_nonce).digest()
    state.socket_level_config = _Configuration(
        flags=flags,
        dictionary_id=dictionary_id,
        nonce_b64=base64.urlsafe_b64encode(connection_nonce).decode("ascii"),
    )

    if (
        ((flags & _ConfigurationFlags.ZSTD_ENABLED) == 0)
        or ((flags & _ConfigurationFlags.ZSTD_TRAINING_ALLOWED) == 0)
    ) and state.training_data is not None:
        if state.training_data.type == _CompressorTrainingInfoType.BEFORE_LOW_WATERMARK:
            state.training_data.collector.tmpfile.close()
        if (
            state.training_data.type
            == _CompressorTrainingInfoType.BEFORE_HIGH_WATERMARK
        ):
            state.training_data.collector.tmpfile.close()
        state.training_data = None

    if (flags & _ConfigurationFlags.ZSTD_ENABLED) != 0:
        state.standard_compressor = _CompressorReady(
            type=_CompressorState.READY,
            dictionary_id=1,
            level=3,
            data=None,
            compressor=zstandard.ZstdCompressor(
                level=3,
                write_checksum=False,
                write_content_size=False,
                write_dict_id=False,
            ),
            decompressor=zstandard.ZstdDecompressor(
                max_window_size=state.broadcaster_config.decompression_max_window_size
            ),
        )

    if (
        dictionary_id != 0  # "no compression"
        and dictionary_id != 1  # reserved for not using a dictionary
        and (flags & _ConfigurationFlags.ZSTD_ENABLED) != 0
        and state.broadcaster_config.compression_allowed
        and (
            state.active_compressor is None
            or dictionary_id != state.active_compressor.dictionary_id
        )
    ):

        async def _make_compressor() -> _CompressorReady:
            requested = await state.broadcaster_config.get_compression_dictionary_by_id(
                dictionary_id
            )
            if requested is None:
                raise ValueError("dictionary not found")

            zdict, level = requested

            return _CompressorReady(
                type=_CompressorState.READY,
                dictionary_id=dictionary_id,
                level=level,
                data=zdict,
                compressor=zstandard.ZstdCompressor(
                    level=level,
                    dict_data=zdict,
                    write_checksum=False,
                    write_content_size=False,
                    write_dict_id=False,
                ),
                decompressor=zstandard.ZstdDecompressor(
                    dict_data=zdict,
                    max_window_size=state.broadcaster_config.decompression_max_window_size,
                ),
            )

        _rotate_compressor(
            state,
            _CompressorPreparing(
                type=_CompressorState.PREPARING,
                dictionary_id=dictionary_id,
                task=asyncio.create_task(_make_compressor()),
            ),
        )

    state.read_task = _make_websocket_read_task(state.websocket)

    state.pending_sends.append(
        _FormattedMessage(
            type=_MessageType.FORMATTED,
            websocket_data=_make_websocket_message(
                message.flags,
                _OutgoingParsedWSMessageType.CONFIRM_CONFIGURE,
                [("x-broadcaster-nonce", broadcaster_nonce)],
                b"",
            ),
        )
    )


async def _acknowledge_compressor_ready(
    state: _StateOpen, compressor: _CompressorReady
) -> _State:
    if compressor.dictionary_id <= 1:
        return state

    is_preset = compressor.dictionary_id < 65536

    headers = _make_websocket_message(
        _ParsedWSMessageFlags.MINIMAL_HEADERS,
        (
            _OutgoingParsedWSMessageType.ENABLE_ZSTD_PRESET
            if is_preset
            else _OutgoingParsedWSMessageType.ENABLE_ZSTD_CUSTOM
        ),
        [
            (
                "x-identifier",
                compressor.dictionary_id.to_bytes(
                    _smallest_unsigned_size(compressor.dictionary_id),
                    "big",
                ),
            ),
            (
                "x-compression-level",
                compressor.level.to_bytes(
                    _smallest_unsigned_size(compressor.level),
                    "big",
                ),
            ),
        ],
        b"",
    )

    if not is_preset:
        assert compressor.data is not None, "custom compressor without custom dict?"
        dict_data_bytes = compressor.data.as_bytes()
        if (
            state.broadcaster_config.outgoing_max_ws_message_size is not None
            and len(headers) + len(dict_data_bytes)
            > state.broadcaster_config.outgoing_max_ws_message_size
        ):
            raise ValueError(
                f"cannot transfer {len(dict_data_bytes)} byte dictionary with "
                f"{state.broadcaster_config.outgoing_max_ws_message_size} byte max "
                f"outgoing websocket message size ({len(headers)} bytes needed for headers)"
            )

        message = headers + dict_data_bytes
    else:
        message = headers

    if state.send_task is None:
        state.send_task = asyncio.create_task(state.websocket.send_bytes(message))
    else:
        state.pending_sends.append(
            _FormattedMessage(type=_MessageType.FORMATTED, websocket_data=message)
        )
    return state


async def _process_subscribe_or_unsubscribe(
    state: _StateOpen, message: _ParsedWSMessage
) -> None:
    assert (
        message.type == _ParsedWSMessageType.SUBSCRIBE
        or message.type == _ParsedWSMessageType.UNSUBSCRIBE
    )
    payload = parse_subscribe_payload(io.BytesIO(message.body))

    authorization_bytes = message.headers.get("authorization")
    authorization = (
        None if authorization_bytes is None else authorization_bytes.decode("utf-8")
    )
    url = _make_for_receive_websocket_url_and_change_counter(state)
    if payload.url != url:
        raise ValueError("unexpected url target for subscribe")

    auth_at = time.time()
    if payload.type == SubscribeType.EXACT:
        auth_result = await state.broadcaster_config.is_subscribe_exact_allowed(
            url=url,
            exact=payload.topic,
            now=auth_at,
            authorization=authorization,
        )
    else:
        auth_result = await state.broadcaster_config.is_subscribe_glob_allowed(
            url=url,
            glob=payload.glob,
            now=auth_at,
            authorization=authorization,
        )

    if auth_result != "ok":
        raise Exception(auth_result)

    if payload.type == SubscribeType.EXACT:
        if message.type == _ParsedWSMessageType.SUBSCRIBE:
            if payload.topic in state.my_receiver.exact_subscriptions:
                raise Exception(f"already subscribed to {payload.topic!r}")

            state.my_receiver.exact_subscriptions.add(payload.topic)
            await state.receiver.increment_exact(payload.topic)
            response = _make_websocket_message(
                flags=message.flags,
                message_type=_OutgoingParsedWSMessageType.CONFIRM_SUBSCRIBE_EXACT,
                headers=[("x-topic", payload.topic)],
                body=b"",
            )
        else:
            try:
                state.my_receiver.exact_subscriptions.remove(payload.topic)
            except KeyError:
                raise Exception(f"not subscribed to {payload.topic!r}")

            await state.receiver.decrement_exact(payload.topic)
            response = _make_websocket_message(
                flags=message.flags,
                message_type=_OutgoingParsedWSMessageType.CONFIRM_UNSUBSCRIBE_EXACT,
                headers=[("x-topic", payload.topic)],
                body=b"",
            )
    else:
        if message.type == _ParsedWSMessageType.SUBSCRIBE:
            if any(
                payload.glob == glob for _, glob in state.my_receiver.glob_subscriptions
            ):
                raise Exception(f"already subscribed to {payload.glob}")

            glob_regex = re.compile(translate(payload.glob))
            state.my_receiver.glob_subscriptions.append((glob_regex, payload.glob))
            await state.receiver.increment_glob(payload.glob)
            response = _make_websocket_message(
                flags=message.flags,
                message_type=_OutgoingParsedWSMessageType.CONFIRM_SUBSCRIBE_GLOB,
                headers=[("x-glob", payload.glob.encode("utf-8"))],
                body=b"",
            )
        else:
            subscription_idx: Optional[int] = None
            for idx, (_, glob) in enumerate(state.my_receiver.glob_subscriptions):
                if glob == payload.glob:
                    subscription_idx = idx
                    break

            if subscription_idx is None:
                raise Exception(f"not subscribed to {payload.glob}")

            state.my_receiver.glob_subscriptions.pop(subscription_idx)
            await state.receiver.decrement_glob(payload.glob)
            response = _make_websocket_message(
                flags=message.flags,
                message_type=_OutgoingParsedWSMessageType.CONFIRM_UNSUBSCRIBE_GLOB,
                headers=[("x-glob", payload.glob.encode("utf-8"))],
                body=b"",
            )

    if state.send_task is None:
        state.send_task = asyncio.create_task(state.websocket.send_bytes(response))
    else:
        state.pending_sends.append(
            _FormattedMessage(type=_MessageType.FORMATTED, websocket_data=response)
        )


async def _process_notify(state: _StateOpen, message: _ParsedWSMessage) -> None:
    assert message.type == _ParsedWSMessageType.NOTIFY
    if state.socket_level_config is None:
        raise Exception("notify before configure")

    compressor_id_bytes = message.headers.get("x-compressor", b"\x00")
    if len(compressor_id_bytes) > 8:
        raise ValueError("x-compressor max 8 bytes")

    message_id = message.headers["x-identifier"]
    if len(message_id) > 64:
        raise ValueError("x-identifier max 64 bytes")

    compressor_id = int.from_bytes(compressor_id_bytes, "big")

    if compressor_id != 0 and (
        not state.broadcaster_config.compression_allowed
        or (state.socket_level_config.flags & _ConfigurationFlags.ZSTD_ENABLED) == 0
    ):
        raise ValueError("compression used but compression is forbidden")

    body_io = io.BytesIO(message.body)
    topic_length = int.from_bytes(read_exact(body_io, 2), "big")
    topic = read_exact(body_io, topic_length)
    compressed_sha512 = read_exact(body_io, 64)

    authorization_bytes = message.headers.get("authorization")
    authorization = (
        None if authorization_bytes is None else authorization_bytes.decode("utf-8")
    )
    auth_result = await state.broadcaster_config.is_notify_allowed(
        topic=topic,
        message_sha512=compressed_sha512,
        now=time.time(),
        authorization=authorization,
    )
    if auth_result != "ok":
        raise Exception(auth_result)

    compressed_length = int.from_bytes(read_exact(body_io, 8), "big")
    compressed_data = read_exact(body_io, compressed_length)

    real_compressed_sha512 = hashlib.sha512(compressed_data).digest()
    if real_compressed_sha512 != compressed_sha512:
        raise Exception("integrity check failed")

    decompressor: Optional[_Compressor]
    if compressor_id == 0:
        decompressor = None
    elif (
        state.standard_compressor is not None
        and compressor_id == state.standard_compressor.dictionary_id
    ):
        decompressor = state.standard_compressor
    elif (
        state.active_compressor is not None
        and compressor_id == state.active_compressor.dictionary_id
    ):
        decompressor = state.active_compressor
    elif (
        state.last_compressor is not None
        and compressor_id == state.last_compressor.dictionary_id
    ):
        decompressor = state.last_compressor
    else:
        raise ValueError("unrecognized compressor id")

    decompressed_data: Optional[SyncIOBaseLikeIO] = None
    if decompressor is None:
        decompressed_data = io.BytesIO(compressed_data)
        decompressed_sha512 = hashlib.sha512(compressed_data).digest()
        decompressed_length = len(compressed_data)
    else:
        if decompressor.type == _CompressorState.PREPARING:
            decompressor = await decompressor.task

        unseekable_decompressed_data = decompressor.decompressor.stream_reader(
            compressed_data
        )
        decompressed_hasher = hashlib.sha512()
        try:
            decompressed_data = tempfile.SpooledTemporaryFile(
                max_size=state.broadcaster_config.message_body_spool_size
            )
            while True:
                chunk = unseekable_decompressed_data.read(io.DEFAULT_BUFFER_SIZE)
                if not chunk:
                    break
                decompressed_data.write(chunk)
                decompressed_hasher.update(chunk)
                await asyncio.sleep(0)
            decompressed_sha512 = decompressed_hasher.digest()
            decompressed_length = decompressed_data.tell()
            decompressed_data.seek(0)
        except BaseException:
            if decompressed_data is not None:
                decompressed_data.close()
            raise
        finally:
            unseekable_decompressed_data.close()

    try:
        notify_result = await handle_trusted_notify(
            topic,
            decompressed_data,
            config=state.broadcaster_config,
            session=state.client_session,
            content_length=decompressed_length,
            sha512=decompressed_sha512,
        )
    finally:
        decompressed_data.close()

    if notify_result.type == TrustedNotifyResultType.UNAVAILABLE:
        raise Exception("failed to attempt all subscribers")

    to_send = _make_websocket_message(
        message.flags,
        _OutgoingParsedWSMessageType.CONFIRM_NOTIFY,
        [
            ("x-identifier", message_id),
            (
                "x-subscribers",
                notify_result.succeeded.to_bytes(
                    _smallest_unsigned_size(notify_result.succeeded), "big"
                ),
            ),
        ],
        b"",
    )

    if state.send_task is None:
        state.send_task = asyncio.create_task(state.websocket.send_bytes(to_send))
    else:
        state.pending_sends.append(
            _FormattedMessage(type=_MessageType.FORMATTED, websocket_data=to_send)
        )


async def _process_notify_stream(state: _StateOpen, message: _ParsedWSMessage) -> None:
    assert message.type == _ParsedWSMessageType.NOTIFY_STREAM

    message_id = message.headers["x-identifier"]
    if (
        state.incoming_notification is not None
        and state.incoming_notification.identifier != message_id
    ):
        raise ValueError("did not finish last message")

    part_id_bytes = message.headers["x-part-id"]
    if len(part_id_bytes) > 8:
        raise ValueError("x-part-id max 8 bytes")

    part_id = int.from_bytes(part_id_bytes, "big")

    notif = state.incoming_notification
    if part_id == 0:
        if notif is not None:
            raise ValueError("did not complete previous message")
        topic = message.headers["x-topic"]
        compressor_id_bytes = message.headers["x-compressor"]
        if len(compressor_id_bytes) > 8:
            raise ValueError("compressor id max 8 bytes")
        compressor_id = int.from_bytes(compressor_id_bytes, "big")
        compressed_length_bytes = message.headers["x-compressed-length"]
        if len(compressed_length_bytes) > 8:
            raise ValueError("compressed length max 8 bytes")
        compressed_length = int.from_bytes(compressed_length_bytes, "big")
        deflated_length_bytes = message.headers["x-deflated-length"]
        if len(deflated_length_bytes) > 8:
            raise ValueError("deflated length max 8 bytes")
        deflated_length = int.from_bytes(deflated_length_bytes, "big")
        compressed_sha512 = message.headers["x-compressed-sha512"]
        if len(compressed_sha512) != 64:
            raise ValueError("sha512 must be exactly 64 bytes")

        notif = _ReceivingNotify(
            identifier=message_id,
            last_part_id=-1,
            topic=topic,
            compressor_id=compressor_id,
            compressed_length=compressed_length,
            deflated_length=deflated_length,
            compressed_sha512=compressed_sha512,
            body_hasher=hashlib.sha512(),
            body=tempfile.SpooledTemporaryFile(
                max_size=state.broadcaster_config.message_body_spool_size
            ),
        )
        state.incoming_notification = notif
    else:
        if notif is None or notif.last_part_id + 1 != part_id:
            raise ValueError("received part out of order")

    # verify we will still be able to decompress this message
    if notif.compressor_id != 0:
        if not (
            (
                state.standard_compressor is not None
                and state.standard_compressor.dictionary_id == notif.compressor_id
            )
            or (
                state.active_compressor is not None
                and state.active_compressor.dictionary_id == notif.compressor_id
            )
            or (
                state.last_compressor is not None
                and state.last_compressor.dictionary_id == notif.compressor_id
            )
        ):
            raise ValueError("unknown compressor id")

    notif.body_hasher.update(message.body)
    notif.body.write(message.body)
    compressed_bytes_so_far = notif.body.tell()
    if compressed_bytes_so_far > notif.compressed_length:
        raise ValueError("compressed message exceeds indicated length")

    if compressed_bytes_so_far < notif.compressed_length:
        ack_message = _make_websocket_message(
            message.flags,
            _OutgoingParsedWSMessageType.CONTINUE_NOTIFY,
            [("x-identifier", notif.identifier), ("x-part-id", part_id_bytes)],
            b"",
        )
        if state.send_task is None:
            state.send_task = asyncio.create_task(
                state.websocket.send_bytes(ack_message)
            )
        else:
            state.pending_sends.append(
                _FormattedMessage(
                    type=_MessageType.FORMATTED, websocket_data=ack_message
                )
            )
        return

    actual_compressed_sha512 = notif.body_hasher.digest()
    if actual_compressed_sha512 != notif.compressed_sha512:
        raise ValueError("integrity mismatch")

    compressor: Optional[_Compressor] = None
    if (
        state.standard_compressor is not None
        and notif.compressor_id == state.standard_compressor.dictionary_id
    ):
        compressor = state.standard_compressor
    elif (
        state.active_compressor is not None
        and notif.compressor_id == state.active_compressor.dictionary_id
    ):
        compressor = state.active_compressor
    elif (
        state.last_compressor is not None
        and notif.compressor_id == state.last_compressor.dictionary_id
    ):
        compressor = state.last_compressor
    elif notif.compressor_id != 0:
        raise ValueError("unknown compressor id")

    if compressor is None:
        notif.body.seek(0)
        result = await handle_trusted_notify(
            notif.topic,
            notif.body,
            config=state.broadcaster_config,
            session=state.client_session,
            content_length=notif.compressed_length,
            sha512=notif.compressed_sha512,
        )
        if result.type == TrustedNotifyResultType.UNAVAILABLE:
            raise ValueError("could not attempt all subscribers")

        ack_message = _make_websocket_message(
            flags=message.flags,
            message_type=_OutgoingParsedWSMessageType.CONFIRM_NOTIFY,
            headers=[
                ("x-identifier", notif.identifier),
                (
                    "x-subscribers",
                    result.succeeded.to_bytes(
                        _smallest_unsigned_size(result.succeeded), "big"
                    ),
                ),
            ],
            body=b"",
        )
        if state.send_task is None:
            state.send_task = asyncio.create_task(
                state.websocket.send_bytes(ack_message)
            )
        else:
            state.pending_sends.append(
                _FormattedMessage(
                    type=_MessageType.FORMATTED, websocket_data=ack_message
                )
            )
        notif.body.close()
        state.incoming_notification = None
        return

    if compressor.type == _CompressorState.PREPARING:
        compressor = await compressor.task

    notif.body.seek(0)
    with tempfile.SpooledTemporaryFile(
        max_size=state.broadcaster_config.message_body_spool_size
    ) as target:
        decompressed_hasher = hashlib.sha512()

        with compressor.decompressor.stream_reader(
            cast(IO[bytes], notif.body), read_size=io.DEFAULT_BUFFER_SIZE
        ) as decompressor:
            while True:
                chunk = decompressor.read(io.DEFAULT_BUFFER_SIZE)
                if not chunk:
                    break

                decompressed_hasher.update(chunk)
                target.write(chunk)
                if target.tell() > notif.deflated_length:
                    raise ValueError("decompresses to larger than indicated")

                await asyncio.sleep(0)

        notif.body.close()

        if target.tell() != notif.deflated_length:
            raise ValueError("decompresses to less than indicated")

        decompressed_sha512 = decompressed_hasher.digest()

        target.seek(0)

        result = await handle_trusted_notify(
            notif.topic,
            target,
            config=state.broadcaster_config,
            session=state.client_session,
            content_length=notif.deflated_length,
            sha512=decompressed_sha512,
        )

        if result.type == TrustedNotifyResultType.UNAVAILABLE:
            raise ValueError("failed to attempt all subscribers")

        ack_message = _make_websocket_message(
            flags=message.flags,
            message_type=_OutgoingParsedWSMessageType.CONFIRM_NOTIFY,
            headers=[
                ("x-identifier", notif.identifier),
                (
                    "x-subscribers",
                    result.succeeded.to_bytes(
                        _smallest_unsigned_size(result.succeeded), "big"
                    ),
                ),
            ],
            body=b"",
        )
        if state.send_task is None:
            state.send_task = asyncio.create_task(
                state.websocket.send_bytes(ack_message)
            )
        else:
            state.pending_sends.append(
                _FormattedMessage(
                    type=_MessageType.FORMATTED, websocket_data=ack_message
                )
            )
        state.incoming_notification = None


_PROCESSOR_BY_TYPE: Dict[
    _ParsedWSMessageType,
    Callable[[_StateOpen, _ParsedWSMessage], Coroutine[None, None, None]],
] = {
    _ParsedWSMessageType.CONFIGURE: _process_configure,
    _ParsedWSMessageType.SUBSCRIBE: _process_subscribe_or_unsubscribe,
    _ParsedWSMessageType.UNSUBSCRIBE: _process_subscribe_or_unsubscribe,
    _ParsedWSMessageType.NOTIFY: _process_notify,
    _ParsedWSMessageType.NOTIFY_STREAM: _process_notify_stream,
}


def _process_message_asap(state: _StateOpen, message: _ParsedWSMessage) -> None:
    if state.process_task is not None:
        state.unprocessed_receives.append(message)
        return

    state.process_task = asyncio.create_task(
        _PROCESSOR_BY_TYPE[message.type](state, message)
    )


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

        if state.process_task is None and state.unprocessed_receives:
            _process_message_asap(state, state.unprocessed_receives.popleft())
            return state

        if state.send_task is not None and state.send_task.done():
            state.send_task.result()
            state.send_task = None
            return state

        if state.process_task is not None and state.process_task.done():
            state.process_task.result()
            state.process_task = None
            return state

        if (
            state.active_compressor is not None
            and state.active_compressor.type == _CompressorState.PREPARING
            and state.active_compressor.task.done()
        ):
            state.active_compressor = state.active_compressor.task.result()
            return await _acknowledge_compressor_ready(state, state.active_compressor)

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
            # we purposely don't acknowledge this compressor to avoid confusing the order
            # on the client
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
            _process_message_asap(state, parsed_message)
            return state

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
    if state.process_task is not None:
        state.process_task.cancel()
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
    if state.incoming_notification is not None:
        state.incoming_notification.body.close()

    for task in state.backgrounded:
        task.cancel()

    await state.client_session.close()

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

    - Only considers messages that are between 32 and 16384 bytes for training
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
            - x-compressor is a big-endian unsigned integer; 0 for no compression,
              1 for compressed with no custom dictionary, otherwise one of the last
              two dictionary ids from enable zstandard compress dictionary messages.
              when compressing, the sha512 and length must be for the compressed content
        body:
            - exact body of /v1/notify
    4: Notify Stream:
        send a notification over multiple websocket messages. this is more likely to work on
        typical setups when the notification payload exceeds 16MB.

        headers:
            - authorization (url: websocket:<nonce>:<ctr>, see below)
            - x-identifier identifies the notify whose compressed body is being appended. arbitrary blob, max 64 bytes
            - x-part-id starts at 0 and increments by 1 for each part. interpreted unsigned, big-endian, max 8 bytes
            - x-topic iff x-part-id is 0, the topic of the notification
            - x-compressor iff x-part-id is 0, either 0 for no compression, 1
              for zstandard compression without a custom dictionary, and
              otherwise the id of the compressor from one of the
              "Enable X compression" broadcaster->subscriber messages
            - x-compressed-length iff x-part-id is 0, the total length of the compressed body, big-endian, unsigned, max 8 bytes
            - x-deflated-length iff x-part-id is 0, the total length of the deflated body, big-endian, unsigned, max 8 bytes
            - x-compressed-sha512 iff x-part-id is 0, the sha-512 hash of the compressed content once all parts are concatenated, 64 bytes

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
            - x-compressed-sha512 iff x-part-id is 0, the sha-512 hash of the compressed content once all parts are concatenated, 64 bytes

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
