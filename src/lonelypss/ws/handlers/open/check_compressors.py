from lonelypsp.stateful.constants import BroadcasterToSubscriberStatefulMessageType
from lonelypsp.stateful.messages.enable_zstd_custom import (
    B2S_EnableZstdCustom,
    serialize_b2s_enable_zstd_custom,
)
from lonelypsp.stateful.messages.enable_zstd_preset import (
    B2S_EnableZstdPreset,
    serialize_b2s_enable_zstd_preset,
)

from lonelypss.ws.handlers.open.check_result import CheckResult
from lonelypss.ws.handlers.open.send_simple_asap import send_simple_asap
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
            new_compressor = compressor.task.result()
            state.compressors[idx] = new_compressor

            if new_compressor.identifier < 65536:
                send_simple_asap(
                    state,
                    serialize_b2s_enable_zstd_preset(
                        B2S_EnableZstdPreset(
                            type=BroadcasterToSubscriberStatefulMessageType.ENABLE_ZSTD_PRESET,
                            identifier=new_compressor.identifier,
                            compression_level=new_compressor.level,
                            min_size=(
                                state.broadcaster_config.compression_min_size
                                if new_compressor.identifier != 1
                                else state.broadcaster_config.compression_trained_max_size
                            ),
                            max_size=2**64 - 1,
                        ),
                        minimal_headers=state.broadcaster_config.websocket_minimal_headers,
                    ),
                )
            else:
                assert (
                    new_compressor.data is not None
                ), f"compressor identifier {compressor.identifier}>=2**16 must have data"
                send_simple_asap(
                    state,
                    serialize_b2s_enable_zstd_custom(
                        B2S_EnableZstdCustom(
                            type=BroadcasterToSubscriberStatefulMessageType.ENABLE_ZSTD_CUSTOM,
                            identifier=new_compressor.identifier,
                            compression_level=new_compressor.level,
                            min_size=state.broadcaster_config.compression_min_size,
                            max_size=state.broadcaster_config.compression_trained_max_size,
                            dictionary=new_compressor.data.as_bytes(),
                        ),
                        minimal_headers=state.broadcaster_config.websocket_minimal_headers,
                    ),
                )

            did_something = True

    return CheckResult.RESTART if did_something else CheckResult.CONTINUE
