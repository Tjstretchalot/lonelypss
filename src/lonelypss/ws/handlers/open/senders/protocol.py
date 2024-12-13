from typing import Generic, Protocol, TypeVar

from lonelypss.ws.state import StateOpen

T_contra = TypeVar("T_contra", contravariant=True)


class Sender(Generic[T_contra], Protocol):
    """The protocol for the send_* functions within this folder; checked to
    ensure consistency
    """

    async def __call__(self, state: StateOpen, message: T_contra) -> None: ...
