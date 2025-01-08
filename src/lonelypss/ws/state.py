import asyncio
import hashlib
import re
from collections import deque
from dataclasses import dataclass
from enum import Enum, auto
from typing import List, Literal, Optional, Protocol, Set, Tuple, Union

import aiohttp
from fastapi import WebSocket
from lonelypsp.stateful.message import S2B_Message
from lonelypsp.stateful.messages.confirm_receive import S2B_ConfirmReceive
from lonelypsp.stateful.messages.continue_receive import S2B_ContinueReceive
from lonelypsp.stateful.messages.notify_stream import (
    S2B_NotifyStreamStartCompressed,
    S2B_NotifyStreamStartUncompressed,
)

from lonelypss.config.config import Config
from lonelypss.util.sync_io import SyncIOBaseLikeIO, SyncReadableBytesIO
from lonelypss.util.websocket_message import WSMessage
from lonelypss.util.ws_receiver import BaseWSReceiver, FanoutWSReceiver

try:
    import zstandard
except ImportError:
    ...


@dataclass
class ConnectionConfiguration:
    """Describes what settings were negotiated at the connection level.
    These are requested by the CONFIGURE packet by the subscriber, then
    we reduce it to what we are willing to do and do _not_ tell the
    subscriber what we chose (for convenience).
    """

    enable_zstd: bool
    """True if the subscriber can use zstandard-compressed data and might send
    zstandard-compressed data, false if the broadcaster should not send
    zstandard-compressed data and should reject zstandard-compressed data.
    """

    enable_training: bool
    """True if the subscriber can receive custom compression dictionaries, so
    it may make sense for the broadcaster to train one and send it. False if the
    subscriber will not accept custom dictionaries and so the broadcaster
    shouldn't waste time training one.
    """


class StateType(Enum):
    """Discriminator value for the state the websocket is in."""

    ACCEPTING = auto()
    """The websocket was just initialized but we are waiting to finish the
    websocket-level handshake (websocket.accept())
    """

    WAITING_CONFIGURE = auto()
    """The websocket-level handshake is finished and we are waiting for the
    subscriber to send a CONFIGURE message
    """

    OPEN = auto()
    """The main state of the websocket, where we are sending and receiving
    messages.
    """

    CLOSING = auto()
    """We are performing final cleanup tasks before closing the websocket."""

    CLOSED = auto()
    """The websocket is closed and no longer usable."""


@dataclass
class StateAccepting:
    """The variables available when in the ACCEPTING state"""

    type: Literal[StateType.ACCEPTING]
    """discriminator value"""

    websocket: WebSocket
    """the underlying socket that we will communciate over"""

    broadcaster_config: Config
    """the configuration of the broadcaster, which will be used to e.g.
    check authorization headers, load preset compression dictionaries, etc
    """

    internal_receiver: FanoutWSReceiver
    """how the broadcaster can receive messages sent from subscribers to
    other broadcasters
    """


class InternalMessageType(Enum):
    """The types of messages that we can receive from the internal receiver.
    These are pushed immediately into a queue so they can be processed by the
    standard coroutine as soon as possible while otherwise being unaltered.
    """

    SMALL = auto()
    """A messages small enough that we can hold it in memory"""

    LARGE = auto()
    """A message which may be large enough that we may not want to hold it
    in memory.
    """

    MISSED = auto()
    """A message which indicates this broadcaster may have missed a message
    from another broadcaster on a relevant topic
    """


@dataclass
class InternalSmallMessage:
    """A message from the receiver that is small enough to hold in memory"""

    type: Literal[InternalMessageType.SMALL]
    """discriminator value"""

    topic: bytes
    """the topic the message was sent to"""

    data: bytes
    """the uncompressed message data"""

    sha512: bytes
    """a trusted sha512 hash of the data"""


@dataclass
class InternalLargeMessage:
    """A message from the receiver that may be too large to hold in memory"""

    type: Literal[InternalMessageType.LARGE]
    """discriminator value"""

    stream: SyncReadableBytesIO
    """a readable stream of the message. this stream is not tellable, seekable, or
    closeable; use the finished event to indicate that you will no longer be
    accessing the stream.

    the read(n) function may read past the end of the message if n is too large;
    you must instead be careful to read only the number of bytes you expect.
    """

    length: int
    """the length of the message body"""

    finished: asyncio.Event
    """must be set as soon as the stream will no longer be accessed for this
    websocket connection; allows the handle to be used by other connections
    """

    topic: bytes
    """the topic the message was sent to"""

    sha512: bytes
    """a trusted sha512 hash of the message body"""


@dataclass
class InternalMissedMessage:
    """A message from the receiver that indicates this broadcaster may have missed
    a message from another broadcaster on a relevant topic
    """

    type: Literal[InternalMessageType.MISSED]
    """discriminator value"""

    topic: bytes
    """the topic the message was sent to"""


InternalMessage = Union[
    InternalSmallMessage, InternalLargeMessage, InternalMissedMessage
]


class WaitingInternalMessageType(Enum):
    """Describes the types of messages that we have pulled off the internal
    receiver queue but were not able to process as the websocket was busy.

    This is similar to the internal messages, but we augmented them by assigning
    them an identifier, and switched the data structure they are stored in since
    we no longer have to share it with other asyncio tasks.
    """

    SPOOLED_LARGE = auto()
    """A message that may be large enough that we may not want to hold it in
    memory. Unlike with the original internal message, when we realized we
    weren't able to process it immediately we copied the data to another location
    and released the original stream, so this can be closed with a typical close()
    call.
    """


@dataclass
class WaitingInternalSpooledLargeMessage:
    """A message from the receiver that may be too large to hold in memory, after
    we've copied it to a new location and released the original stream.
    """

    type: Literal[WaitingInternalMessageType.SPOOLED_LARGE]
    """discriminator value"""

    stream: SyncIOBaseLikeIO
    """a readable, seekable, tellable, and closeable stream to the copied data. closing
    this stream deletes our copy of the data

    0 is the start of the message and does not contain any additional data besides the
    message (ie., cannot read past)
    """

    length: int
    """the length of the message body in bytes"""

    topic: bytes
    """the topic the message was sent to"""

    sha512: bytes
    """a trusted sha512 hash of the message body"""


WaitingInternalMessage = Union[
    InternalSmallMessage, InternalMissedMessage, WaitingInternalSpooledLargeMessage
]


class SimplePendingSendType(Enum):
    """Describes tasks besides WaitingInternalMessage that will eventually need
    exclusive access to sending data through the websocket
    """

    PRE_FORMATTED = auto()
    """A message that is ready to be sent via send_bytes"""


@dataclass
class SimplePendingSendPreFormatted:
    """A message that is ready to be sent via send_bytes on the websocket once it's
    not busy
    """

    type: Literal[SimplePendingSendType.PRE_FORMATTED]
    """discriminator value"""

    data: bytes
    """the data to send"""


class AsyncioWSReceiver(BaseWSReceiver, Protocol):
    """The receiver type that we use for the websocket where all the function
    implementations just forward to exposed data structures in a relatively
    obvious way. This is used because the caller essentially wants to manipulate
    the functions behaviors directly and synchronously.
    """

    @property
    def exact_subscriptions(self) -> Set[bytes]:
        """The exact topics that the receiver returns True to from is_relevant,
        that the caller can mutate
        """

    @property
    def glob_subscriptions(self) -> List[Tuple[re.Pattern, str]]:
        """The glob patterns that the receiver returns True to from is_relevant,
        that the caller can mutate. The first element in each tuple is the regex
        that matches the pattern, and the second element is the original pattern
        """

    @property
    def queue(self) -> asyncio.Queue[InternalMessage]:
        """The queue that this receiver pushes messages to. For large messages,
        this involves generating a finished event that is wait()'d before returning
        """


@dataclass
class CompressorTrainingDataCollector:
    """Combines the related state for collecting training data to train a custom
    compression dictionary for this connection
    """

    messages: int
    """the number of messages that have been collected so far"""

    length: int
    """how much training data has been collected; the file length will be longer
    as it includes the length of each sample before the sample itself (4 bytes per sample)
    """

    tmpfile: SyncIOBaseLikeIO
    """a readable, writable, seekable, tellable, closable file-like object where we are storing 
    the data to train the dictionary. closing this file will delete the data

    see collector_utils for a more convenient interface

    typically, if writing a small message, the flow is:
    - seek to the end (`seek(0, os.SEEK_END)`)
    - write the length prefix (`length.to_bytes(4, "big")`)
    - write the data
    - yield to the event loop

    for writing a large message, the flow is:
    - store a reference to the collector as the states collector may change
    - create an asyncio.Event, add it to the 'pending' set
    - seek to the end (`seek(0, os.SEEK_END)`)
    - write the length prefix (`length.to_bytes(4, "big")`)
    - remember the current position (`pos = tell()`)
    - write zeros for the data
    - yield to the event loop
    - when you receive more data:
      - seek to the position (`seek(pos, os.SEEK_SET)`)
      - write the data
      - remember the new position (`pos += len(data)`)
      - repeat until all data is written
    - mark the event finished
    - remove the event from the pending set
    """

    pending: Set[asyncio.Event]
    """while this set is non-empty the data in the tmpfile may be in a partial
    state; when this is empty, the data in the tmpfile is complete and can be
    used to train a dictionary
    """


class CompressorTrainingInfoType(Enum):
    """If training is allowed, the potential states we are in with regard to training a
    compression dictionary
    """

    BEFORE_LOW_WATERMARK = auto()
    """We are waiting for data to build a new dictionary using the low watermark settings"""
    BEFORE_HIGH_WATERMARK = auto()
    """We are waiting for data to build a new dictionary using the high watermark settings"""
    WAITING_TO_REFRESH = auto()
    """We built a dictionary recently; once some time passes, we'll build another one"""


@dataclass
class CompressorTrainingInfoBeforeLowWatermark:
    """We are waiting for data to build a new dictionary using the low watermark settings"""

    type: Literal[CompressorTrainingInfoType.BEFORE_LOW_WATERMARK]
    """discriminator value"""

    collector: CompressorTrainingDataCollector
    """The collector where the data is stored"""

    compressor_id: int
    """The compressor id we will use for the next dictionary once we have
    produced it
    """


@dataclass
class CompressorTrainingInfoBeforeHighWatermark:
    """We are collecting data to build a new dictionary using the high watermark settings"""

    type: Literal[CompressorTrainingInfoType.BEFORE_HIGH_WATERMARK]
    """discriminator value"""

    collector: CompressorTrainingDataCollector
    """The collector where the data is stored"""

    compressor_id: int
    """The compressor id we will use for the next dictionary once we have
    produced it
    """


@dataclass
class CompressorTrainingInfoWaitingToRefresh:
    """We built a dictionary recently; once some time passes, we'll build another one"""

    type: Literal[CompressorTrainingInfoType.WAITING_TO_REFRESH]
    """discriminator value"""

    last_built: float
    """the time the last high watermark dictionary was built in seconds from the epoch
    (as if by `time.time()`)
    """

    compressor_id: int
    """The compressor id we will use for the next dictionary once we have
    produced it
    """


CompressorTrainingInfo = Union[
    CompressorTrainingInfoBeforeLowWatermark,
    CompressorTrainingInfoBeforeHighWatermark,
    CompressorTrainingInfoWaitingToRefresh,
]


class CompressorState(Enum):
    """Describes the state that a zstandard compressor is in"""

    PREPARING = auto()
    """We are in the process of preparing the compressor for use"""

    READY = auto()
    """The compressor is ready to use right now; the individual zstandard compressors/
    decompressors are not async or thread safe, but the pool of them is async safe
    """


@dataclass
class CompressorReady:
    """A compressor which is ready to use"""

    type: Literal[CompressorState.READY]
    """discriminator value"""

    identifier: int
    """the integer identifier for this compressor; positive integers only."""

    level: int
    """the compression level the compressor is set to"""

    min_size: int
    """the minimum size, in bytes, inclusive, that a message can be for the broadcaster
    to choose this compressor for the message. 0 means no minimum size. Note that the
    subscriber may use this compressor for messages smaller than this size and the
    broadcaster will still decompress it.
    """

    max_size: Optional[int]
    """the maximum size, if any, in bytes, exclusive, that a message can be for the broadcaster
    to choose this compressor for the message. None means no maximum size. Note that the
    subscriber may use this compressor for messages larger than this size and the
    broadcaster will still decompress it.
    """

    data: "Optional[zstandard.ZstdCompressionDict]"
    """if there is a custom compression dictionary, that dictionary, otherwise None"""

    compressors: "List[zstandard.ZstdCompressor]"
    """the zstandard compressor objects that are not in use. pulled LIFO as it is
    preferable to reuse the same object as much as possible.
    
    WARN: individual compressors are not asyncio safe
    """

    decompressors: "List[zstandard.ZstdDecompressor]"
    """the zstandard decompressors that are not in use. pulled LIFO as it is
    preferable to reuse the same object as much as possible.
    
    WARN: decompressors are not asyncio-safe
    """


@dataclass
class CompressorPreparing:
    """A compressor that isn't ready to use yet"""

    type: Literal[CompressorState.PREPARING]
    """discriminator value"""

    identifier: int
    """the integer identifier for this compressor; positive integers only.

    1 is reserved for the compressor with no special compression dictionary,
    i.e., data is compressed by building a compression dictionary for each
    message. this works best on moderate sized messages (e.g., at least 16kb)
    as otherwise the overhead of the dictionary size may outweigh the benefits
    of using it

    2-65535 are reserved for preset compression dictionaries, where the compression
    dictionary is distributed out of band

    65536 and above are custom dictionaries built for the connection
    """

    task: asyncio.Task[CompressorReady]
    """The task that is working on preparing the compressor for use"""


Compressor = Union[CompressorReady, CompressorPreparing]


@dataclass
class NotifyStreamState:
    """Keeps track of the combined state of the last related NOTIFY_STREAM messages
    from the subscriber that have been processed.
    """

    identifier: bytes
    """the message identifier that we are receiving, chosen arbitrarily by the
    subscriber
    """

    first: Union[S2B_NotifyStreamStartUncompressed, S2B_NotifyStreamStartCompressed]
    """The first stream message with this id, with the payload stripped out"""

    part_id: int
    """The last part id that we received"""

    body_hasher: "hashlib._Hash"
    """the hash object that is producing the sha512 hash of the body as it comes in"""

    body: SyncIOBaseLikeIO
    """a writable, seekable, tellable, closeable file-like object where we are storing
    the body of the message as it comes in. closing this file will delete the data
    """


@dataclass
class StateWaitingConfigure:
    """The variables available in the WAITING_CONFIGURE state, before the subscriber
    has sent the CONFIGURE message
    """

    type: Literal[StateType.WAITING_CONFIGURE]
    """discriminator value"""

    websocket: WebSocket
    """the underlying socket that we will communciate over. We exclusively read
    bytes via the read_task and do not send bytes in this state
    """

    broadcaster_config: Config
    """configures how the broadcaster acts and what functionality it supports"""

    internal_receiver: FanoutWSReceiver
    """how the broadcaster can receive messages sent from subscribers to
    other broadcasters
    """

    read_task: asyncio.Task[WSMessage]
    """The task that is reading the next message from the websocket, which
    ought to be the configure message
    """


@dataclass
class StateOpen:
    """The variables available when in the OPEN state, which comprises the majority
    of the time the websocket connection is active
    """

    type: Literal[StateType.OPEN]
    """discriminator value"""

    websocket: WebSocket
    """the underlying socket that we will communciate over.
    
    we exclusively send bytes to this websocket in the send_task, and we 
    exclusively read bytes from this websocket in the read_task
    """

    broadcaster_config: Config
    """configures how the broadcaster acts and what functionality it supports"""

    connection_config: ConnectionConfiguration
    """the negotiated configuration taking into account the clients CONFIGURE message"""

    nonce_b64: str
    """The base64 encoded connection nonce, which is the format we need it in
    for producing URLs
    """

    internal_receiver: FanoutWSReceiver
    """allows the broadcaster to receive notifications sent to other broadcasters"""

    my_receiver: AsyncioWSReceiver
    """the receiver we registered with the fanout receiver"""

    my_receiver_id: int
    """the id we received when registering `my_receiver` with `internal_receiver`"""

    client_session: aiohttp.ClientSession
    """the aiohttp ClientSession for notifying other subscribers when this subscriber
    notifies the broadcaster via NOTIFY or NOTIFY_STREAM
    """

    compressors: List[Compressor]
    """The compressors that the broadcaster may try to use or will accept,
    in the order they were initialized.
    
    When rotating in a new compressor, generally the oldest (closest to the
    front) is removed, excluding the first entry (identifier 1).

    May be empty is compresion is disabled in the broadcaster config or the
    connection config
    """

    compressor_training_info: Optional[CompressorTrainingInfo]
    """If we may potentially collect data for training a new compression dictionary,
    based on the broadcaster config and the connection config, contains the state
    related to that
    """

    broadcaster_counter: int
    """Whenever the broadcaster produces authorization headers a URI identifying
    the subscriber is required. For the HTTP api, this is the actual URL that
    the broadcaster is reaching out to. For the websocket api, instead, this is
    a unique value that is comprised of the connection nonce and this counter,
    where this counter is incremented after each use.

    Starts at 1
    """

    subscriber_counter: int
    """Whenever the subscriber produces authorization headers a URI identifying
    the subscriber is required. For the HTTP api, this is the actual URL the
    broadcaster can reach the subscriber. For the websocket api, instead, this is
    a unique value that is comprised of the connection nonce and this counter,
    where this counter is decremented after each use.

    Starts at -1
    """

    read_task: asyncio.Task[WSMessage]
    """The task that is currently responsible for getting the next message on the
    websocket from the ASGI server
    """

    internal_message_task: asyncio.Task[InternalMessage]
    """The task that is currently responsible for getting the next message from 
    my_receiver.queue
    """

    notify_stream_state: Optional[NotifyStreamState]
    """If there is a NOTIFY_STREAM sequence that has not completed yet, the state
    of that sequence, otherwise None
    """

    send_task: Optional[asyncio.Task[None]]
    """If a task is currently responsible for sending data over the websocket, that
    task, otherwise None to indicate nothing is sending data over the websocket
    """

    process_task: Optional[asyncio.Task[None]]
    """If a task is currently processing one of the incoming messages, the task that
    is doing that, otherwise None
    """

    unprocessed_messages: deque[S2B_Message]
    """If the broadcaster has received messages from the subscriber that it has not
    yet processed, the messages that have not been processed in the order they were 
    received.

    basic idea: websocket incoming -> read_task -> unprocessed_messages -> process_task
    """

    unsent_messages: deque[Union[WaitingInternalMessage, SimplePendingSendPreFormatted]]
    """If the broadcaster knows it needs to send messages to the subscriber that it
    has not yet sent because the websocket was busy (send_task was set), the messages
    that need to be sent in the order they should be sent.
    
    Note that for internal messages, we don't format them until we have exclusive access
    to sending messages over the websocket for efficiencies sake.

    - scenario 1: process_task -> unsent_messages -> send_task (e.g., confirm notify)
    - scenario 2: my_receiver.queue -> message_task -> unsent_messages -> send_task (e.g., receive)
    """

    expecting_acks: asyncio.Queue[Union[S2B_ContinueReceive, S2B_ConfirmReceive]]
    """When the broadcaster sends a RECEIVE_STREAM message to the subscriber the
    subscriber must acknowledge the message. If there are enough messages that have
    not been acknowledged, the broadcaster begins to wait for acknowledgements before
    sending more messages, which can cause unsent_messages to grow to its limit which
    will then have the websocket disconnected by the broadcaster

    The left of this queue is the next ack we expect to receive; if we receive any
    other ack there was a problem
    """

    backgrounded: Set[asyncio.Task[None]]
    """A set of tasks that need some time to complete, and should be checked to make
    sure they did not error (otherwise, the broadcaster should disconnect the subscriber),
    but do not have any other particular influence
    """


@dataclass
class StateClosing:
    """The variables available when in the CLOSING state, where we are closing
    the websocket
    """

    type: Literal[StateType.CLOSING]
    """discriminator value"""

    websocket: WebSocket
    """the websocket we are waiting to close"""

    exception: Optional[BaseException] = None
    """the exception we intend to raise once cleanup is complete, if any"""


@dataclass
class StateClosed:
    """The variables available when in the CLOSED state, where the websocket is
    closed and no longer usable
    """

    type: Literal[StateType.CLOSED]
    """discriminator value"""


State = Union[
    StateAccepting, StateWaitingConfigure, StateOpen, StateClosing, StateClosed
]
