from typing import Generic, Protocol, TypeVar

from lonelypss.ws.state import StateOpen

T_contra = TypeVar("T_contra", contravariant=True)


class S2B_MessageProcessor(Generic[T_contra], Protocol):
    """The protocol that the exported functions in this folder adhere to,
    checked to ensure consistency
    """

    async def __call__(self, state: StateOpen, message: T_contra) -> None:
        """Processes the given message received while in the given state. There
        will be no other messages being processed at the same time, but this does
        not give you any exclusive access to the state as send tasks have overlapping
        responsibilities (e.g., writing to the training data collector). Thus, this
        must be async-safe.
        """
