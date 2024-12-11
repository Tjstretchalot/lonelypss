from typing import TYPE_CHECKING, AsyncIterator, Type

from lonelypss.util.async_io import AsyncReadableBytesIO


class AsyncIterableAIO:
    """Adapts an AsyncIterable[bytes] to an asynchronous file-like object"""

    def __init__(self, iter: AsyncIterator[bytes]) -> None:
        self._iter: AsyncIterator[bytes] = iter
        self._buffer: bytes = b""
        self._finished: bool = False

    async def read(self, n: int) -> bytes:
        if n < 0:
            while not self._finished:
                try:
                    self._buffer += await self._iter.__anext__()
                except StopAsyncIteration:
                    self._finished = True

            tmp, self._buffer = self._buffer, b""
            return tmp

        while not self._finished and len(self._buffer) < n:
            try:
                self._buffer += await self._iter.__anext__()
            except StopAsyncIteration:
                self._finished = True

        result, self._buffer = self._buffer[:n], self._buffer[n:]
        return result


if TYPE_CHECKING:
    _: Type[AsyncReadableBytesIO] = AsyncIterableAIO
