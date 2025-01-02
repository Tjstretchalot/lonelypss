import asyncio
import os
import tempfile
import time
from types import TracebackType
from typing import TYPE_CHECKING, List, Optional, Protocol, Tuple, Type

from lonelypsp.stateful.constants import BroadcasterToSubscriberStatefulMessageType
from lonelypsp.stateful.messages.disable_zstd_custom import (
    B2S_DisableZstdCustom,
    serialize_b2s_disable_zstd_custom,
)

from lonelypss.util.sync_io import read_exact
from lonelypss.ws.handlers.open.send_simple_asap import send_simple_asap
from lonelypss.ws.state import (
    Compressor,
    CompressorPreparing,
    CompressorReady,
    CompressorState,
    CompressorTrainingDataCollector,
    CompressorTrainingInfoBeforeHighWatermark,
    CompressorTrainingInfoType,
    CompressorTrainingInfoWaitingToRefresh,
    StateOpen,
)

try:
    import zstandard
except ImportError:
    ...


class _ConfigCompressorGenerator(Protocol):
    """
    type that describes
    state.broadcaster_config.train_compression_dict_low_watermark and
    state.broadcaster_config.train_compression_dict_high_watermark
    """

    async def __call__(
        self, /, samples: List[bytes]
    ) -> "Tuple[zstandard.ZstdCompressionDict, int]": ...


async def make_ready_compressor_from_collector_and_generator(
    state: StateOpen,
    collector: CompressorTrainingDataCollector,
    generator: _ConfigCompressorGenerator,
    identifier: int,
    close_fd: bool,
) -> CompressorReady:
    """Generates a new ready compressor using the data in the given collector
    and the given compression dictionary generator from samples. This will
    load samples from the collector, allowing for new data to be written to the
    collector in the meantime and regularly yielding to the event loop

    Args:
        state: the state object
        collector: the collector with the training data
        generator: the generator that will create the compression dictionary
        identifier: the identifier for the compressor we are making
        close_fd: true to close the collectors tmpfile when done, regardless of
            success, false not to
    """

    our_event = asyncio.Event()
    try:
        # while we are running new data may be written to the collector;
        # in order to ensure that we don't get continuously stuck, we will
        # only wait until the data that is already pending in the collector
        # is ready while also preventing the tmpfile from being closed before
        # we've read that data

        usable_num_messages = collector.messages
        writing_to_usable = set(
            asyncio.create_task(event.wait()) for event in collector.pending
        )
        collector.pending.add(our_event)

        await asyncio.wait(writing_to_usable, return_when=asyncio.ALL_COMPLETED)
        for wait in writing_to_usable:
            wait.result()

        samples: List[bytes] = []
        pos = 0
        while len(samples) < usable_num_messages:
            collector.tmpfile.seek(pos, os.SEEK_SET)
            sample_len_bytes = read_exact(collector.tmpfile, 4)
            sample_len = int.from_bytes(sample_len_bytes, "big")
            sample = read_exact(collector.tmpfile, sample_len)
            samples.append(sample)
            pos += 4 + sample_len
            await asyncio.sleep(0)

        our_event.set()
        collector.pending.discard(our_event)

        zdict, level = await generator(samples)
        return CompressorReady(
            type=CompressorState.READY,
            identifier=identifier,
            level=level,
            min_size=state.broadcaster_config.compression_min_size,
            max_size=state.broadcaster_config.compression_trained_max_size,
            data=zdict,
            compressors=list(),
            decompressors=list(),
        )
    finally:
        our_event.set()
        collector.pending.discard(our_event)

        if close_fd:
            collector.tmpfile.close()


def make_preparing_compressor_from_collector_and_generator(
    state: StateOpen,
    collector: CompressorTrainingDataCollector,
    generator: _ConfigCompressorGenerator,
    identifier: int,
    close_fd: bool,
) -> CompressorPreparing:
    """Creates a preparing compressor that will use the data within the given
    collector to generate a new compression dictionary with the given generator,
    then use that to make a compressor and decompressor

    Args:
        state: the state object
        collector: the collector with the training data
        generator: the generator that will create the compression dictionary
        identifier: the identifier for the compressor
        close_fd: true to close the collectors tmpfile when done, regardless of
            success, false not to
    """
    return CompressorPreparing(
        type=CompressorState.PREPARING,
        identifier=identifier,
        task=asyncio.create_task(
            make_ready_compressor_from_collector_and_generator(
                state, collector, generator, identifier, close_fd
            )
        ),
    )


def rotate_in_compressor(state: StateOpen, compressor: Compressor) -> None:
    """Removes the a non-preset compressor in the state, if there are
    at least 2, and adds the given compressor to the state

    Assuming this is the only way non-preset compressors are added to
    the state, this will always remove the oldest non-preset compressor
    """

    first = True
    for candidate_idx in range(len(state.compressors) - 1, -1, -1):
        candidate = state.compressors[candidate_idx]
        if candidate.identifier < 65536:
            continue

        if first:
            first = False
            continue

        state.compressors.pop(candidate_idx)
        if candidate.type == CompressorState.PREPARING:
            candidate.task.cancel()

        send_simple_asap(
            state,
            serialize_b2s_disable_zstd_custom(
                B2S_DisableZstdCustom(
                    type=BroadcasterToSubscriberStatefulMessageType.DISABLE_ZSTD_CUSTOM,
                    identifier=candidate.identifier,
                ),
                minimal_headers=state.broadcaster_config.websocket_minimal_headers,
            ),
        )
        break

    state.compressors.append(compressor)


def maybe_advance_compressor_training_info(state: StateOpen) -> None:
    """If we are collecting data for training a compression dictionary and we
    have enough training data, rotates the new compressor in and advances
    the compressor training info
    """
    if state.compressor_training_info is None:
        return

    if (
        state.compressor_training_info.type
        == CompressorTrainingInfoType.WAITING_TO_REFRESH
    ):
        now = time.time()
        if (
            state.compressor_training_info.last_built
            + state.broadcaster_config.compression_retrain_interval_seconds
            > now
        ):
            return

        tmpfile = tempfile.SpooledTemporaryFile(
            max_size=state.broadcaster_config.message_body_spool_size
        )
        try:
            state.compressor_training_info = CompressorTrainingInfoBeforeHighWatermark(
                type=CompressorTrainingInfoType.BEFORE_HIGH_WATERMARK,
                collector=CompressorTrainingDataCollector(
                    messages=0, length=0, tmpfile=tmpfile, pending=set()
                ),
                compressor_id=state.compressor_training_info.compressor_id,
            )
            return
        except BaseException:
            tmpfile.close()
            raise

    if (
        state.compressor_training_info.type
        == CompressorTrainingInfoType.BEFORE_LOW_WATERMARK
    ):
        if (
            state.compressor_training_info.collector.length
            < state.broadcaster_config.compression_training_low_watermark
        ):
            return

        if (
            state.compressor_training_info.collector.length
            < state.broadcaster_config.compression_training_high_watermark
        ):
            rotate_in_compressor(
                state,
                make_preparing_compressor_from_collector_and_generator(
                    state,
                    state.compressor_training_info.collector,
                    state.broadcaster_config.train_compression_dict_low_watermark,
                    identifier=state.compressor_training_info.compressor_id,
                    close_fd=False,
                ),
            )
            state.compressor_training_info = CompressorTrainingInfoBeforeHighWatermark(
                type=CompressorTrainingInfoType.BEFORE_HIGH_WATERMARK,
                collector=state.compressor_training_info.collector,
                compressor_id=state.compressor_training_info.compressor_id + 1,
            )
            return

        state.compressor_training_info = CompressorTrainingInfoBeforeHighWatermark(
            type=CompressorTrainingInfoType.BEFORE_HIGH_WATERMARK,
            collector=state.compressor_training_info.collector,
            compressor_id=state.compressor_training_info.compressor_id,
        )

    if (
        state.compressor_training_info.collector.length
        < state.broadcaster_config.compression_training_high_watermark
    ):
        return

    rotate_in_compressor(
        state,
        make_preparing_compressor_from_collector_and_generator(
            state,
            state.compressor_training_info.collector,
            state.broadcaster_config.train_compression_dict_high_watermark,
            identifier=state.compressor_training_info.compressor_id,
            close_fd=True,
        ),
    )
    state.compressor_training_info = CompressorTrainingInfoWaitingToRefresh(
        type=CompressorTrainingInfoType.WAITING_TO_REFRESH,
        last_built=time.time(),
        compressor_id=state.compressor_training_info.compressor_id + 1,
    )


def maybe_store_small_message_for_training(state: StateOpen, data: bytes) -> None:
    """If we are collecting data for training a compression dictionary, store
    the given data from memory synchronously. These can be weaved in at any time
    even while large messages are being written to the collector over multiple
    event loops.
    """
    if state.compressor_training_info is None:
        return

    if len(data) < state.broadcaster_config.compression_min_size:
        return

    if len(data) >= state.broadcaster_config.compression_trained_max_size:
        return

    if (
        state.compressor_training_info.type
        == CompressorTrainingInfoType.WAITING_TO_REFRESH
    ):
        maybe_advance_compressor_training_info(state)
        if (
            state.compressor_training_info.type
            == CompressorTrainingInfoType.WAITING_TO_REFRESH
        ):
            return

    collector = state.compressor_training_info.collector

    try:
        collector.tmpfile.seek(0, os.SEEK_END)
        collector.tmpfile.write(len(data).to_bytes(4, "big"))
        collector.tmpfile.write(data)
    except BaseException:
        collector.tmpfile.close()
        raise


class CompressorLargeMessageWriter(Protocol):
    """Convenience protocol for writing large messages to the training
    collector, generated by maybe_write_large_message_for_training

    For convenience this acts as a synchronous context manager where enter
    does nothing and exit calls raise_if_not_done
    """

    @property
    def length(self) -> int:
        """the length of the message we are writing"""

    @property
    def remaining(self) -> int:
        """how many more bytes we expect to receive"""

    @property
    def is_void(self) -> bool:
        """true if this is a void writer, false if you need to write"""

    def skip_void(self) -> None:
        """Raises an error unless this is a void writer, in which case this
        sets remaining to zero
        """

    def raise_if_not_done(self) -> None:
        """cleans up and raises an error if remaining is not zero"""

    def write_chunk(self, data: bytes) -> None:
        """Writes a chunk of the message to the collector. If this is
        the last chunk, cleans up resources. To check if this was the
        last chunk, remaining will be zero
        """

    def __enter__(self) -> "CompressorLargeMessageWriter":
        """no-op"""

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        """calls raise_if_not_done"""


class VoidingCompressorLargeMessageWriter:
    def __init__(self, length: int) -> None:
        self.length = length
        self.remaining = length
        self.is_void = True

    def skip_void(self) -> None:
        self.remaining = 0

    def raise_if_not_done(self) -> None:
        if self.remaining != 0:
            raise ValueError("not done writing")

    def write_chunk(self, data: bytes) -> None:
        if self.remaining < len(data):
            raise ValueError("too much data")
        self.remaining -= len(data)

    def __enter__(self) -> "CompressorLargeMessageWriter":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.raise_if_not_done()


if TYPE_CHECKING:
    _: Type[CompressorLargeMessageWriter] = VoidingCompressorLargeMessageWriter


class _RealCompressorLargeMessageWriter:
    def __init__(self, collector: CompressorTrainingDataCollector, length: int) -> None:
        self._collector = collector
        self._event = asyncio.Event()
        self._collector.pending.add(self._event)
        self._pos = -1
        self.length = length
        self.remaining = length
        self.is_void = False

        try:
            self._pos = collector.tmpfile.seek(0, os.SEEK_END)
            collector.tmpfile.write(length.to_bytes(4, "big"))
        except BaseException:
            self._cleanup()
            collector.tmpfile.close()
            raise

        self._pos += 4

        if length == 0:
            self._cleanup()

    def skip_void(self) -> None:
        raise Exception("not a void writer")

    def raise_if_not_done(self) -> None:
        if self.remaining != 0:
            self._cleanup()
            raise ValueError("not done writing")

    def write_chunk(self, data: bytes) -> None:
        if self.remaining < len(data):
            self._cleanup()
            raise ValueError("too much data")

        self.remaining -= len(data)
        try:
            self._collector.tmpfile.seek(self._pos, os.SEEK_SET)
            self._collector.tmpfile.write(data)
        except BaseException:
            self._cleanup()
            self._collector.tmpfile.close()
            raise

        self._pos += len(data)
        if self.remaining == 0:
            self._cleanup()

    def _cleanup(self) -> None:
        self._event.set()
        self._collector.pending.discard(self._event)

    def __enter__(self) -> "CompressorLargeMessageWriter":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.raise_if_not_done()


if TYPE_CHECKING:
    __: Type[CompressorLargeMessageWriter] = _RealCompressorLargeMessageWriter


def maybe_write_large_message_for_training(
    state: StateOpen, length: int, never_store: bool = False
) -> CompressorLargeMessageWriter:
    """If we need to collect a message of the given length in the training
    collector, returns an object that will write a large message to the
    collector. Otherwise, return an object with the same protocol that will just
    verify it is being used correctly.

    Multiple of these writers can be used at the same time on the main asyncio
    thread, even while training is occuring (on the main asyncio thread), and it
    may be weaved with small messages being written to the collector (again, on
    the main asyncio thread)

    If `never_store` is set this will always return a void writer, which can be
    convenient when using this as a context manager
    """

    if (
        never_store
        or state.compressor_training_info is None
        or length < state.broadcaster_config.compression_min_size
        or length >= state.broadcaster_config.compression_trained_max_size
    ):
        return VoidingCompressorLargeMessageWriter(length)

    if (
        state.compressor_training_info.type
        == CompressorTrainingInfoType.WAITING_TO_REFRESH
    ):
        maybe_advance_compressor_training_info(state)
        if (
            state.compressor_training_info.type
            == CompressorTrainingInfoType.WAITING_TO_REFRESH
        ):
            return VoidingCompressorLargeMessageWriter(length)

    return _RealCompressorLargeMessageWriter(
        state.compressor_training_info.collector, length
    )
