from enum import Enum, auto


class CheckResult(Enum):
    """The handler function is composed into parts; each part can either request
    we return back to the top of the handler function, continue to the next part
    of the handler function, or raise an exception to start the cleanup and disconnect
    process.

    Although the return type could be annotated as a boolean, for clarity
    we instead use an enum
    """

    RESTART = auto()
    """Return to the top of the handler function because we made progress"""

    CONTINUE = auto()
    """Continue to the next part of the handler function"""
