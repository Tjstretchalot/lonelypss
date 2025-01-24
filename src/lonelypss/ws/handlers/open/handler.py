import asyncio
from typing import (
    TYPE_CHECKING,
    Any,
    List,
    Optional,
    cast,
)

from lonelypsp.compat import assert_never
from lonelypsp.util.bounded_deque import BoundedDeque
from lonelypsp.util.cancel_and_check import cancel_and_check

from lonelypss.ws.handlers.open.check_background_tasks import check_background_tasks
from lonelypss.ws.handlers.open.check_compressors import check_compressors
from lonelypss.ws.handlers.open.check_my_receiver_queue import (
    check_my_receiver_queue,
)
from lonelypss.ws.handlers.open.check_process_task import check_process_task
from lonelypss.ws.handlers.open.check_read_task import check_read_task
from lonelypss.ws.handlers.open.check_result import CheckResult
from lonelypss.ws.handlers.open.check_send_task import check_send_task
from lonelypss.ws.handlers.open.errors import (
    NormalDisconnectException,
    combine_multiple_exceptions,
)
from lonelypss.ws.handlers.open.processors.processor import process_any
from lonelypss.ws.handlers.open.senders.send_any import Sendable
from lonelypss.ws.handlers.protocol import StateHandler
from lonelypss.ws.state import (
    CompressorState,
    CompressorTrainingInfoType,
    InternalMessageType,
    State,
    StateClosing,
    StateOpen,
    StateType,
    WaitingInternalMessageType,
)


async def handle_open(state: State) -> State:
    """Makes some progress, waiting if necessary, and returning the new state. This
    may be the same state reference, allowing the caller to manage the required looping.

    It is intended that this never raises exceptions
    """
    assert state.type == StateType.OPEN

    _disconnected_receiver = False
    try:
        try:
            if await check_send_task(state) == CheckResult.RESTART:
                return state

            if await check_my_receiver_queue(state) == CheckResult.RESTART:
                return state

            if await check_read_task(state) == CheckResult.RESTART:
                return state

            if await check_process_task(state) == CheckResult.RESTART:
                return state

            if await check_background_tasks(state) == CheckResult.RESTART:
                return state

            if await check_compressors(state) == CheckResult.RESTART:
                return state

            owned_tasks: List[asyncio.Task[Any]] = []
            if state.my_receiver.queue.empty():
                owned_tasks.append(
                    asyncio.create_task(state.my_receiver.queue.wait_not_empty())
                )
            try:
                await asyncio.wait(
                    [
                        *([state.send_task] if state.send_task is not None else []),
                        *owned_tasks,
                        state.read_task,
                        *(
                            [state.process_task]
                            if state.process_task is not None
                            else []
                        ),
                        *state.backgrounded,
                        *[
                            compressor.task
                            for compressor in state.compressors
                            if compressor.type == CompressorState.PREPARING
                        ],
                    ],
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                for task in owned_tasks:
                    await cancel_and_check(task)
            return state
        except NormalDisconnectException:
            if state.send_task is not None:
                state.send_task.cancel()
                state.send_task = None

            state.send_task = cast(
                asyncio.Task[None], asyncio.create_task(asyncio.Event().wait())
            )
            old_unsent = state.unsent_messages
            state.unsent_messages = BoundedDeque(maxlen=0)

            while old_unsent:
                _cleanup(old_unsent.popleft())

            if not _disconnected_receiver:
                _disconnected_receiver = True
                await _disconnect_receiver(state)

            if state.process_task is not None:
                await state.process_task
                state.process_task = None

            while state.unprocessed_messages:
                await process_any(state, state.unprocessed_messages.popleft())

            raise
    except BaseException as cause_for_cleanup_exc:
        cleanup_exceptions: List[BaseException] = []

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
                except BaseException as e2:
                    cleanup_exceptions.append(e2)

        state.read_task.cancel()

        if state.notify_stream_state is not None:
            try:
                state.notify_stream_state.body.close()
            except BaseException as e2:
                cleanup_exceptions.append(e2)

        if state.send_task is not None:
            state.send_task.cancel()

        if state.process_task is not None:
            state.process_task.cancel()

        for msg in state.unsent_messages:
            if msg.type == WaitingInternalMessageType.SPOOLED_LARGE:
                try:
                    msg.stream.close()
                except BaseException as e2:
                    cleanup_exceptions.append(e2)

        for task in state.backgrounded:
            task.cancel()

        if not _disconnected_receiver:
            _disconnected_receiver = True
            try:
                await _disconnect_receiver(state)
            except BaseException as e2:
                cleanup_exceptions.append(e2)

        try:
            await state.client_session.close()
        except BaseException as e2:
            cleanup_exceptions.append(e2)

        result_exception: Optional[BaseException] = None

        if cleanup_exceptions:
            result_exception = combine_multiple_exceptions(
                "cleaning up from open state", cleanup_exceptions
            )
            result_exception.__context__ = cause_for_cleanup_exc
        elif not isinstance(cause_for_cleanup_exc, NormalDisconnectException):
            result_exception = cause_for_cleanup_exc

        return StateClosing(
            type=StateType.CLOSING,
            websocket=state.websocket,
            exception=result_exception,
        )


if TYPE_CHECKING:
    _: StateHandler = handle_open


async def _disconnect_receiver(state: StateOpen) -> None:
    excs: List[BaseException] = []

    try:
        await state.internal_receiver.unregister_receiver(state.my_receiver_id)
    except BaseException as e:
        excs.append(e)

    for topic in state.my_receiver.exact_subscriptions:
        try:
            await state.internal_receiver.decrement_exact(topic)
        except BaseException as e:
            excs.append(e)

    for _, glob in state.my_receiver.glob_subscriptions:
        try:
            await state.internal_receiver.decrement_glob(glob)
        except BaseException as e:
            excs.append(e)

    for msg in state.my_receiver.queue.drain():
        if msg.type == InternalMessageType.SMALL:
            continue

        if msg.type == InternalMessageType.LARGE:
            msg.finished.set()
            continue

        if msg.type == InternalMessageType.MISSED:
            continue

        assert_never(msg)

    if excs:
        raise combine_multiple_exceptions(
            "failed to properly disconnect receiver", excs
        )


def _cleanup(value: Sendable) -> None:
    if value.type == InternalMessageType.LARGE:
        value.finished.set()
    if value.type == WaitingInternalMessageType.SPOOLED_LARGE:
        value.stream.close()
