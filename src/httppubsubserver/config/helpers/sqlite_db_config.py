from typing import TYPE_CHECKING, AsyncIterable, Literal, Type, Union

if TYPE_CHECKING:
    from httppubsubserver.config.config import DBConfig


class SqliteDBConfig:
    async def subscribe_exact(
        self, /, *, url: str, exact: bytes
    ) -> Literal["success", "conflict", "unavailable"]:
        return "unavailable"

    async def unsubscribe_exact(
        self, /, *, url: str, exact: bytes
    ) -> Literal["success", "not_found", "unavailable"]:
        return "unavailable"

    async def subscribe_glob(
        self, /, *, url: str, glob: str
    ) -> Literal["success", "conflict", "unavailable"]:
        return "unavailable"

    async def unsubscribe_glob(
        self, /, *, url: str, glob: str
    ) -> Literal["success", "not_found", "unavailable"]:
        return "unavailable"

    async def get_subscribers(
        self, /, *, topic: bytes
    ) -> AsyncIterable[Union[str, Literal[b"unavailable"]]]:
        yield b"unavailable"


if TYPE_CHECKING:
    _: Type[DBConfig] = SqliteDBConfig
