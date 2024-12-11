from typing import Protocol, Union


class AsyncReadableBytesIOA(Protocol):
    """A type that represents a stream that can be read synchronously"""

    async def read(self, n: int) -> bytes:
        """Reads n bytes from the file-like object"""
        raise NotImplementedError()


class AsyncReadableBytesIOB(Protocol):
    """A type that represents a stream that can be read synchronously"""

    async def read(self, n: int, /) -> bytes:
        """Reads n bytes from the file-like object"""
        raise NotImplementedError()


AsyncReadableBytesIO = Union[AsyncReadableBytesIOA, AsyncReadableBytesIOB]


async def async_read_exact(stream: AsyncReadableBytesIO, n: int) -> bytes:
    """Reads exactly n bytes from the stream, otherwise raises ValueError"""
    result = await stream.read(n)
    if len(result) != n:
        raise ValueError(f"expected {n} bytes, got {len(result)}")
    return result
