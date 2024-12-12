import asyncio
from collections import deque
from typing import TYPE_CHECKING, Iterable, SupportsIndex, Union, cast
from lonelypss.ws.handlers.open.check_background_tasks import check_background_tasks
from lonelypss.ws.handlers.open.check_compressors import check_compressors
from lonelypss.ws.handlers.open.check_internal_message_task import (
    check_internal_message_task,
)
from lonelypss.ws.handlers.open.check_process_task import check_process_task
from lonelypss.ws.handlers.open.check_read_task import check_read_task
from lonelypss.ws.handlers.open.check_result import CheckResult
from lonelypss.ws.handlers.open.check_send_task import check_send_task
from lonelypss.ws.handlers.open.errors import NormalDisconnectException
from lonelypss.ws.handlers.open.processors.processor import process_any
from lonelypss.ws.state import (
    CompressorState,
    CompressorTrainingInfoType,
    SimplePendingSendPreFormatted,
    State,
    StateClosing,
    StateOpen,
    StateType,
    WaitingInternalMessageType,
    WaitingInternalMessage,
)
from lonelypss.ws.handlers.protocol import StateHandler


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

            if await check_internal_message_task(state) == CheckResult.RESTART:
                return state

            if await check_read_task(state) == CheckResult.RESTART:
                return state

            if await check_process_task(state) == CheckResult.RESTART:
                return state

            if await check_background_tasks(state) == CheckResult.RESTART:
                return state

            if await check_compressors(state) == CheckResult.RESTART:
                return state

            await asyncio.wait(
                [
                    *([state.send_task] if state.send_task is not None else []),
                    state.internal_message_task,
                    state.read_task,
                    *([state.process_task] if state.process_task is not None else []),
                    *state.backgrounded,
                    *[
                        compressor.task
                        for compressor in state.compressors
                        if compressor.type == CompressorState.PREPARING
                    ],
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )
            return state
        except NormalDisconnectException:
            if state.send_task is not None:
                state.send_task.cancel()
                state.send_task = None

            state.send_task = cast(
                asyncio.Task[None], asyncio.create_task(asyncio.Event().wait())
            )
            old_unsent = state.unsent_messages
            state.unsent_messages = VoidingDeque()

            while old_unsent:
                _cleanup(old_unsent.popleft())

            state.internal_message_task.cancel()

            if not _disconnected_receiver:
                _disconnected_receiver = True
                await _disconnect_receiver(state)

            if state.process_task is not None:
                await state.process_task
                state.process_task = None

            while state.unprocessed_messages:
                await process_any(state, state.unprocessed_messages.popleft())

            raise
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

        if not _disconnected_receiver:
            _disconnected_receiver = True
            await _disconnect_receiver(state)

        return StateClosing(
            type=StateType.CLOSING,
            websocket=state.websocket,
            exception=e if not isinstance(e, NormalDisconnectException) else None,
        )


if TYPE_CHECKING:
    _: StateHandler = handle_open


async def _disconnect_receiver(state: StateOpen) -> None:
    try:
        await state.internal_receiver.unregister_receiver(state.my_receiver_id)
    except BaseException:
        ...

    for topic in state.my_receiver.exact_subscriptions:
        try:
            await state.internal_receiver.decrement_exact(topic)
        except BaseException:
            ...

    for _, glob in state.my_receiver.glob_subscriptions:
        try:
            await state.internal_receiver.decrement_glob(glob)
        except BaseException:
            ...


SendT = Union[WaitingInternalMessage, SimplePendingSendPreFormatted]


def _cleanup(value: SendT) -> None:
    if value.type == WaitingInternalMessageType.SPOOLED_LARGE:
        value.stream.close()


class VoidingDeque(deque[SendT]):
    def append(self, value: SendT, /) -> None:
        _cleanup(value)

    def appendleft(self, value: SendT, /) -> None:
        _cleanup(value)

    def insert(self, i: int, x: SendT, /) -> None:
        _cleanup(x)

    def extend(self, iterable: Iterable[SendT], /) -> None:
        for v in iterable:
            _cleanup(v)

    def extendleft(self, iterable: Iterable[SendT], /) -> None:
        for v in iterable:
            _cleanup(v)

    def __setitem__(
        self,
        key: Union[int, slice, SupportsIndex],
        value: Union[SendT, Iterable[SendT]],
        /,
    ) -> None:
        if isinstance(key, slice):
            for v in cast(Iterable[SendT], value):
                _cleanup(v)
        else:
            _cleanup(cast(SendT, value))

    def __iadd__(self, other: Iterable[SendT], /) -> "VoidingDeque":
        for v in other:
            _cleanup(v)
        return self

    def __add__(self, other: deque[SendT], /) -> "VoidingDeque":
        for v in other:
            _cleanup(v)
        return self
