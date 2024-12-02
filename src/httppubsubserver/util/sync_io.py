from io import BytesIO, FileIO
from typing import TYPE_CHECKING, Protocol, Type, Union


class SyncReadableBytesIOA(Protocol):
    """A type that represents a stream that can be read synchronously"""

    def read(self, n: int) -> bytes:
        """Reads n bytes from the file-like object"""
        raise NotImplementedError()


class SyncReadableBytesIOB(Protocol):
    """A type that represents a stream that can be read synchronously"""

    def read(self, n: int, /) -> bytes:
        """Reads n bytes from the file-like object"""
        raise NotImplementedError()


SyncReadableBytesIO = Union[SyncReadableBytesIOA, SyncReadableBytesIOB]


class SyncTellableBytesIO(Protocol):
    """A type that represents a stream with a synchronous tell method"""

    def tell(self) -> int:
        """Returns the current position in the file"""
        raise NotImplementedError()


class SyncSeekableBytesIO(Protocol):
    """A type that represents a stream with a synchronous seek method"""

    def seek(self, offset: int, whence: int = 0) -> int:
        """Seeks to a position in the file"""
        raise NotImplementedError()


class SyncStandardIOA(
    SyncReadableBytesIOA, SyncTellableBytesIO, SyncSeekableBytesIO, Protocol
): ...


class SyncStandardIOB(
    SyncReadableBytesIOA, SyncTellableBytesIO, SyncSeekableBytesIO, Protocol
): ...


SyncStandardIO = Union[SyncStandardIOA, SyncStandardIOB]


class SyncLengthIO(Protocol):
    """A type that represents a stream with a synchronous length method"""

    def __len__(self) -> int:
        """Returns the length of the file"""
        raise NotImplementedError()


class SyncStandardWithLengthIOA(
    SyncReadableBytesIOA,
    SyncTellableBytesIO,
    SyncSeekableBytesIO,
    SyncLengthIO,
    Protocol,
): ...


class SyncStandardWithLengthIOB(
    SyncReadableBytesIOA,
    SyncTellableBytesIO,
    SyncSeekableBytesIO,
    SyncLengthIO,
    Protocol,
): ...


SyncStandardWithLengthIO = Union[SyncStandardWithLengthIOA, SyncStandardWithLengthIOB]
"""A type that usually only occurs manually, since it doesn't match BytesIO or FileIO,
but is used to represent a stream where the length is known in advance
"""

if TYPE_CHECKING:
    # verifies BytesIO matches
    _a: Type[SyncReadableBytesIO] = BytesIO
    _b: Type[SyncTellableBytesIO] = BytesIO
    _c: Type[SyncSeekableBytesIO] = BytesIO
    _d: Type[SyncStandardIO] = BytesIO

    # verifies the result of open matches
    _e: Type[SyncReadableBytesIO] = FileIO
    _f: Type[SyncTellableBytesIO] = FileIO
    _g: Type[SyncSeekableBytesIO] = FileIO
    _h: Type[SyncStandardIO] = FileIO
