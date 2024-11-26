import io
from typing import Any


def _noop(*args: Any, **kwargs: Any) -> None:
    pass


class CloseGuardedIO(io.IOBase):
    """Wraps an IO object, except for close() which does nothing"""

    def __init__(self, child: io.IOBase) -> None:
        self._child = child

    def __getattribute__(self, name: str) -> Any:
        if name == "_child":
            return super().__getattribute__(name)
        if name == "close":
            return _noop
        return getattr(self._child, name)
