import asyncio

from lonelypss.ws.handlers.open.check_result import CheckResult
from lonelypss.ws.handlers.open.processors.processor import process_any
from lonelypss.ws.state import StateOpen


async def check_process_task(state: StateOpen) -> CheckResult:
    """If the process task is unset but there are unprocessed messages, sets
    the process task. Otherwise, if the process task has completed, handles the result.

    Raises an exception to indicate we should move to the cleanup and disconnect
    process
    """
    if state.process_task is None:
        if state.unprocessed_messages:
            state.process_task = asyncio.create_task(
                process_any(state, state.unprocessed_messages.popleft())
            )
            return CheckResult.RESTART
        return CheckResult.CONTINUE

    if not state.process_task.done():
        return CheckResult.CONTINUE

    finished_task = state.process_task
    state.process_task = None
    finished_task.result()

    if state.unprocessed_messages:
        # save an event loop cycle
        state.process_task = asyncio.create_task(
            process_any(state, state.unprocessed_messages.popleft())
        )

    return CheckResult.RESTART
