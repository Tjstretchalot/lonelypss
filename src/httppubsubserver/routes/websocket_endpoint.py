import io
import re
from typing import (
    TYPE_CHECKING,
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
from httppubsubserver.util.sync_io import SyncReadableBytesIO

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


class _StateType(Enum):
    ACCEPTING = auto()
    IDLE = auto()
    CLOSING = auto()
    CLOSED = auto()


@dataclass
class _StateAccepting:
    type: Literal[_StateType.ACCEPTING]
    websocket: WebSocket
    config: Config
    receiver: FanoutWSReceiver


@dataclass
class _LargeMessage:
    stream: SyncReadableBytesIO
    topic: bytes
    sha512: bytes
    length: int
    finished: asyncio.Event


@dataclass
class _SmallMessage:
    data: bytes
    topic: bytes
    sha512: bytes


class _MyReceiver:
    def __init__(self) -> None:
        self.exact_subscriptions: Set[bytes] = set()
        self.glob_subscriptions: List[Tuple[re.Pattern, str]] = []
        self.receiver_id: Optional[int] = None

        self.large_queue: asyncio.Queue[_LargeMessage] = asyncio.Queue()
        self.small_queue: asyncio.Queue[_SmallMessage] = asyncio.Queue()

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
        await self.large_queue.put(
            _LargeMessage(stream, topic, sha512, length, finished)
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
        await self.small_queue.put(_SmallMessage(data, topic, sha512))


if TYPE_CHECKING:
    _: Type[BaseWSReceiver] = _MyReceiver


@dataclass
class _CompressorTrainingDataCollector:
    started_at: float
    messages: int
    length: int
    tmpfile: io.RawIOBase


@dataclass
class _Compressor:
    dictionary_id: int
    level: int
    data: "zstandard.ZstdCompressionDict"


@dataclass
class _StateIdle:
    type: Literal[_StateType.IDLE]
    websocket: WebSocket
    config: Config
    receiver: FanoutWSReceiver

    configuration: Optional[_Configuration]
    my_receiver: _MyReceiver

    read_task: asyncio.Task[WSMessage]
    send_task: Optional[asyncio.Task[None]]
    small_message_task: asyncio.Task[_SmallMessage]
    large_message_task: asyncio.Task[_LargeMessage]

    pending_sends: deque[bytes]

    active_compressor: Optional[_Compressor]
    last_compressor: Optional[_Compressor]


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
    _StateIdle,
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
    return _StateIdle(
        type=_StateType.IDLE,
        websocket=state.websocket,
        config=state.config,
        receiver=state.receiver,
        configuration=None,
        my_receiver=my_receiver,
        read_task=_make_websocket_read_task(state.websocket),
        send_task=None,
        small_message_task=asyncio.create_task(my_receiver.small_queue.get()),
        large_message_task=asyncio.create_task(my_receiver.large_queue.get()),
        pending_sends=deque(),
        active_compressor=None,
        last_compressor=None,
    )


async def _handle_idle(state: _State) -> _State:
    assert state.type == _StateType.IDLE
    try:
        if state.send_task is None and state.pending_sends:
            next_send = state.pending_sends.popleft()
            state.send_task = asyncio.create_task(state.websocket.send_bytes(next_send))
            return state

        await asyncio.wait(
            [
                state.read_task,
                state.small_message_task,
                state.large_message_task,
                *([state.send_task] if state.send_task is not None else []),
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )

        if state.send_task is not None and state.send_task.done():
            state.send_task.result()
            state.send_task = None
            return state

        if state.read_task.done():
            raw_message = await state.read_task
            if raw_message["type"] == "websocket.disconnect":
                return await _cleanup_idle(state, None)

            if "bytes" not in raw_message:
                return await _cleanup_idle(
                    state, ValueError("only bytes or close messages expected")
                )

            raw_message = cast(WSMessageBytes, raw_message)
            parsed_message = _parse_websocket_message(raw_message["bytes"])

            if parsed_message.type == _ParsedWSMessageType.CONFIGURE:
                if state.configuration is not None:
                    return await _cleanup_idle(
                        state, ValueError("configuration already set")
                    )
                rdr = io.BytesIO(parsed_message.body)
                flags = _ConfigurationFlags(int.from_bytes(_exact_read(rdr, 4), "big"))
                dictionary_id = int.from_bytes(_exact_read(rdr, 2), "big")
                state.configuration = _Configuration(flags, dictionary_id)
                state.read_task = _make_websocket_read_task(state.websocket)
                state.pending_sends.append(
                    _make_websocket_message(
                        parsed_message.flags,
                        _OutgoingParsedWSMessageType.CONFIRM_CONFIGURE,
                        [],
                        b"",
                    )
                )
                return state

        raise NotImplementedError()
    except BaseException as e:
        return await _cleanup_idle(state, e)


async def _cleanup_idle(
    state: _StateIdle, exception: Optional[BaseException]
) -> _State:
    state.read_task.cancel()
    state.small_message_task.cancel()
    state.large_message_task.cancel()

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
    _StateType.IDLE: _handle_idle,
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
        configures the broadcasters behavior; may be set at most once

        headers:
            - none

        body:
            - 4 bytes (C): configuration flags, big-endian encoded, unsigned. From least to most significant bit:
                - 1: whether zstandard compression is enabled for notify and receive (set: enable, unset: disable).
                - 2: whether training a compression dictionary on the sent and received message bodies may be useful
                    (set: should train, unset: must not train). generally should be set if >50mb of data is expected to be
                    sent and received in <16kb chunks.
            - 2 bytes (D): either 0x00 or the id of the preset compression dictionary to start with
    1: Subscribe:
        subscribe to an exact topic or glob pattern

        headers:
            - authorization
        body:
            - exact body of /v1/subscribe
    2: Unsubscribe:
        unsubscribe from an exact topic or glob pattern

        headers:
            - authorization
        body:
            - exact body of /v1/unsubscribe
    3: Notify:
        send a notification within a single websocket message (typically, max 16MB). this
        can be suitable for arbitrary websocket sizes depending on the configuration of the
        broadcaster (e.g., uvicorn and all intermediaries might limit max ws message sizes)

        headers:
            - authorization
            - x-identifier identifies the notification so we can confirm it
        body:
            - exact body of /v1/notify
    4: Notify Stream:
        send a notification over multiple websocket messages. this is more likely to work on
        typical setups when the notification payload exceeds 16MB.

        headers:
            - authorization
            - x-part-id starts at 0 and increments by 1 for each part. interpreted unsigned, big-endian, max 8 bytes
            - x-topic iff x-part-id is 0, the topic of the notification
            - x-compressor iff x-part-id is 0, either 0 for no compression and otherwise the id of the compressor from one of
              the "Enable X compression" broadcaster->subscriber messages
            - x-compressed-length iff x-part-id is 0, the total length of the compressed body, big-endian, unsigned, max 8 bytes
            - x-deflated-length iff x-part-id is 0, the total length of the deflated body, big-endian, unsigned, max 8 bytes
            - x-repr-digest iff x-part-id is 0, the sha-512 hash of the deflated content once all parts are concatenated, 64 bytes
            - x-identifier identifies the notify whose compressed body is being appended. arbitrary blob, max 64 bytes

        body:
            - blob of data to append to the compressed notification body

    ## Messages Sent to the Subscriber

    0: Configure Confirmation:
        confirms we received the configuration options from the client
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
            - x-identifier identifies the notify whose compressed body is being appended. arbitrary blob, max 64 bytes
            - x-part-id starts at 0 and increments by 1 for each part. interpreted unsigned, big-endian, max 8 bytes
            - authorization: iff x-part-id is 0, proof the broadcaster is authorized to notify the subscriber
            - x-topic iff x-part-id is 0, the topic of the notification
            - x-compressor iff x-part-id is 0, either 0 for no compression and otherwise the id of the compressor from one of
              the "Enable X compression" broadcaster->subscriber messages
            - x-compressed-length iff x-part-id is 0, the total length of the compressed body, big-endian, unsigned, max 8 bytes
            - x-deflated-length iff x-part-id is 0, the total length of the deflated body, big-endian, unsigned, max 8 bytes
            - x-repr-digest iff x-part-id is 0, the sha-512 hash of the deflated content once all parts are concatenated, 64 bytes

        body:
            - blob of data to append to the compressed notification body
    8-99: Reserved
    100: Enable zstandard compression with preset dictionary
        configures the client to expect and use a dictionary that it already has available.
        this may use precomputed dictionaries that were specified during the broadcaster's
        configuration with the assumption the client has them

        headers:
            x-identifier: which compressor is enabled, unsigned, big-endian, max 2 bytes, min 1
            x-compression-level: what compression level we think is best when using
                this dictionary. signed, big-endian, max 2 bytes, max 22. the client
                is free to choose a different compression level

        body: none
    101: Enable zstandard compression with a custom dictionary
        configures the client to use a dictionary we just trained

        headers:
            x-identifier: the id we are assigning to this dictionary, unsigned, big-endian, max 8 bytes,
                min 65536. if not unique, overwrite the previous dictionary
            x-compression-level: what compression level we think is best when using
                this dictionary. signed, big-endian, max 2 bytes, max 22. the client
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
