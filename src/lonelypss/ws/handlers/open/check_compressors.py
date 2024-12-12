from lonelypss.ws.handlers.open.check_result import CheckResult
from lonelypss.ws.state import CompressorState, StateOpen


async def check_compressors(state: StateOpen) -> CheckResult:
    """Checks to see if any of the compressors are in the preparing state
    but the preparation is finished

    Raises an exception to indicate we should move to the cleanup and disconnect
    process
    """

    did_something = False
    for idx in range(len(state.compressors)):
        compressor = state.compressors[idx]
        if compressor.type == CompressorState.PREPARING and compressor.task.done():
            state.compressors[idx] = compressor.task.result()
            did_something = True

    return CheckResult.RESTART if did_something else CheckResult.CONTINUE
