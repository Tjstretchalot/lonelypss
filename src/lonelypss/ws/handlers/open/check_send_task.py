import asyncio

from lonelypss.ws.handlers.open.check_result import CheckResult
from lonelypss.ws.handlers.open.senders.send_any import send_any
from lonelypss.ws.state import StateOpen


async def check_send_task(state: StateOpen) -> CheckResult:
    """If the send task is unset but there are unsent messages, sets
    the send task. Otherwise, if the send task has completed, handles the result.

    Raises an exception to indicate we should move to the cleanup and disconnect
    process
    """
    if state.send_task is None:
        if state.unsent_messages:
            state.send_task = asyncio.create_task(
                send_any(state, state.unsent_messages.popleft())
            )
            return CheckResult.RESTART
        return CheckResult.CONTINUE

    if not state.send_task.done():
        return CheckResult.CONTINUE

    finished_task = state.send_task
    state.send_task = None
    finished_task.result()

    if state.unsent_messages:
        # save an event loop cycle
        state.send_task = asyncio.create_task(
            send_any(state, state.unsent_messages.popleft())
        )

    return CheckResult.RESTART
