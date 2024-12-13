from types import TracebackType
from typing import Optional, Type

from lonelypss.ws.state import CompressorReady, CompressorState, StateOpen

try:
    import zstandard
except ImportError:
    ...


class CompressorReservation:
    """A convenience object for generating or reserving an actual object
    capable of compressing data from a ready compressor configuration

    Acts as a synchronous context manager; must be entered to use the
    compressor and exited to either dispose of the compressor or return it
    to the pool based on the current pool size

    Doesn't return the compressor object to the pool if an exception is passed
    to exit, in case the compressor is in an invalid state
    """

    def __init__(self, compressor: CompressorReady, *, max_compressors: int) -> None:
        self.compressor_configuration = compressor
        """the configuration for the compressor that we generate"""

        self.max_compressors = max_compressors
        """the max number of compressors to hold in the pool"""

        self._compressor: "Optional[zstandard.ZstdCompressor]" = None
        """the compressor object that we are holding onto"""

    def __enter__(self) -> "zstandard.ZstdCompressor":
        assert self._compressor is None, "not re-entrant"

        if self.compressor_configuration.compressors:
            self._compressor = self.compressor_configuration.compressors.pop()
        else:
            self._compressor = zstandard.ZstdCompressor(
                level=self.compressor_configuration.level,
                dict_data=self.compressor_configuration.data,
                write_checksum=False,
                write_content_size=False,
                write_dict_id=False,
            )

        return self._compressor

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        if self._compressor is None or exc_type is not None:
            return

        if len(self.compressor_configuration.compressors) < self.max_compressors:
            self.compressor_configuration.compressors.append(self._compressor)

        self._compressor = None


class DecompressorReservation:
    """A convenience object for generating or reserving an actual object
    capable of decompressing data from a ready compressor configuration

    Doesn't return the decompressor object to the pool if an exception is passed
    to exit, in case the decompressor is in an invalid state
    """

    def __init__(
        self,
        compressor: CompressorReady,
        *,
        max_window_size: int,
        max_decompressors: int,
    ) -> None:
        self.compressor_configuration = compressor
        """the configuration for the compressor that we generate"""

        self.max_decompressors = max_decompressors
        """the max number of decompressors to hold in the pool"""

        self.max_window_size = max_window_size
        """the max window size if we need to create a new decompressor"""

        self._decompressor: "Optional[zstandard.ZstdDecompressor]" = None
        """the decompressor object that we are holding onto"""

    def __enter__(self) -> "zstandard.ZstdDecompressor":
        assert self._decompressor is None, "not re-entrant"

        if self.compressor_configuration.decompressors:
            self._decompressor = self.compressor_configuration.decompressors.pop()
        else:
            self._decompressor = zstandard.ZstdDecompressor(
                dict_data=self.compressor_configuration.data,
                max_window_size=self.max_window_size,
            )

        return self._decompressor

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        if self._decompressor is None or exc_type is not None:
            return

        if len(self.compressor_configuration.decompressors) < self.max_decompressors:
            self.compressor_configuration.decompressors.append(self._decompressor)

        self._decompressor = None


def reserve_compressor(
    state: StateOpen, compressor: CompressorReady
) -> CompressorReservation:
    """Recommended way to initialize a compressor reservation, which will handle
    extracting any required configuration options
    """
    return CompressorReservation(compressor, max_compressors=5)


def reserve_decompressor(
    state: StateOpen, compressor: CompressorReady
) -> DecompressorReservation:
    """Recommended way to initialize a decompressor reservation, which will handle
    extracting any required configuration options
    """
    return DecompressorReservation(
        compressor,
        max_window_size=state.broadcaster_config.decompression_max_window_size,
        max_decompressors=5,
    )


def choose_compressor_for_compression(
    state: StateOpen, length: int
) -> Optional[CompressorReady]:
    """Determines which compressor, if any, should be used for compressing
    a message of the given length
    """
    for candidate_idx in range(len(state.compressors) - 1, -1, -1):
        candidate_compressor = state.compressors[candidate_idx]
        if candidate_compressor.type == CompressorState.PREPARING:
            if candidate_compressor.task.done():
                candidate_compressor = candidate_compressor.task.result()
            else:
                continue

        if candidate_compressor.min_size <= length and (
            candidate_compressor.max_size is None
            or length < candidate_compressor.max_size
        ):
            return candidate_compressor
    return None
