from lonelypss.ws.state import StateOpen


def make_for_send_websocket_url_and_change_counter(state: StateOpen) -> str:
    """Creates the websocket URL that the broadcaster should use when contacting
    the subscriber. The result value is always unique and deterministic, and
    should be independently calculated by the subscriber (and checked for a
    match)
    """
    ctr = state.broadcaster_counter
    state.broadcaster_counter += 1
    return f"websocket:{state.nonce_b64}:{ctr:x}"


def make_for_receive_websocket_url_and_change_counter(state: StateOpen) -> str:
    """Creates the websocket URL that the subscriber should use when contacting
    the broadcaster. The result value is always unique and deterministic, and
    should be independently calculated by the broadcaster (and checked for a
    match)
    """
    ctr = state.subscriber_counter
    state.subscriber_counter -= 1
    return f"websocket:{state.nonce_b64}:{ctr:x}"
