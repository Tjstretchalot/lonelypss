import asyncio
from typing import List, Set

from lonelypss.ws.handlers.open.check_result import CheckResult
from lonelypss.ws.handlers.open.errors import combine_multiple_exceptions
from lonelypss.ws.state import StateOpen


async def check_background_tasks(state: StateOpen) -> CheckResult:
    """Checks to see if any of the background tasks have finished so we can
    check for errors and release the references to them.

    Raises an exception to indicate we should move to the cleanup and disconnect
    process
    """

    for task in state.backgrounded:
        if task.done():
            break
    else:
        return CheckResult.CONTINUE

    done: Set[asyncio.Task[None]] = set()
    pending: Set[asyncio.Task[None]] = set()

    for task in state.backgrounded:
        if task.done():
            done.add(task)
        else:
            pending.add(task)

    state.backgrounded = pending
    excs: List[BaseException] = []
    for finished in done:
        try:
            finished.result()
        except BaseException as e:
            excs.append(e)

    if excs:
        raise combine_multiple_exceptions("error in background task", excs)

    return CheckResult.RESTART
