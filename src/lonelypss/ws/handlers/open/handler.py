import asyncio
from typing import TYPE_CHECKING
from lonelypss.ws.handlers.open.check_background_tasks import check_background_tasks
from lonelypss.ws.handlers.open.check_process_task import check_process_task
from lonelypss.ws.handlers.open.check_read_task import check_read_task
from lonelypss.ws.handlers.open.check_result import CheckResult
from lonelypss.ws.handlers.open.check_send_task import check_send_task
from lonelypss.ws.state import (
    CompressorState,
    CompressorTrainingInfoType,
    State,
    StateClosing,
    StateType,
    WaitingInternalMessageType,
)
from lonelypss.ws.handlers.protocol import StateHandler


async def handle_open(state: State) -> State:
    """Makes some progress, waiting if necessary, and returning the new state. This
    may be the same state reference, allowing the caller to manage the required looping.

    It is intended that this never raises exceptions
    """
    assert state.type == StateType.OPEN

    try:
        if await check_read_task(state) == CheckResult.RESTART:
            return state

        if await check_process_task(state) == CheckResult.RESTART:
            return state

        if await check_send_task(state) == CheckResult.RESTART:
            return state

        if await check_background_tasks(state) == CheckResult.RESTART:
            return state

        await asyncio.wait(
            [
                state.read_task,
                *([state.process_task] if state.process_task is not None else []),
                *([state.send_task] if state.send_task is not None else []),
                *state.backgrounded,
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )
        return state
    except BaseException as e:
        for compressor in state.compressors:
            if compressor.type == CompressorState.PREPARING:
                compressor.task.cancel()

        if state.compressor_training_info is not None:
            if (
                state.compressor_training_info.type
                != CompressorTrainingInfoType.WAITING_TO_REFRESH
            ):
                try:
                    state.compressor_training_info.collector.tmpfile.close()
                except BaseException:
                    ...

        state.read_task.cancel()
        state.internal_message_task.cancel()

        if state.notify_stream_state is not None:
            try:
                state.notify_stream_state.body.close()
            except BaseException:
                ...

        if state.send_task is not None:
            state.send_task.cancel()

        if state.process_task is not None:
            state.process_task.cancel()

        for msg in state.unsent_messages:
            if msg.type == WaitingInternalMessageType.SPOOLED_LARGE:
                try:
                    msg.stream.close()
                except BaseException:
                    ...

        for task in state.backgrounded:
            task.cancel()

        return StateClosing(
            type=StateType.CLOSING, websocket=state.websocket, exception=e
        )


if TYPE_CHECKING:
    _: StateHandler = handle_open
