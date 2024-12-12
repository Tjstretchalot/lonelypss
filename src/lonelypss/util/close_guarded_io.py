import io
import sys
from typing import Any, Union

from lonelypss.util.sync_io import SyncIOBaseLikeIO

if sys.version_info <= (3, 10):
    from tempfile import SpooledTemporaryFile

    ChildType = Union[io.IOBase, SpooledTemporaryFile[bytes], SyncIOBaseLikeIO]
else:
    ChildType = Union[io.IOBase, SyncIOBaseLikeIO]


def _noop(*args: Any, **kwargs: Any) -> None:
    pass


class CloseGuardedIO(io.IOBase):
    """Wraps an IO object, except for close() which does nothing"""

    def __init__(self, child: ChildType) -> None:
        self._child = child

    def __getattribute__(self, name: str) -> Any:
        if name == "_child":
            return super().__getattribute__(name)
        if name == "close":
            return _noop
        return getattr(self._child, name)
