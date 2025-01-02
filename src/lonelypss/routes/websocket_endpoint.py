from fastapi import APIRouter, WebSocket

from lonelypss.middleware.config import get_config_from_request
from lonelypss.middleware.ws_receiver import get_ws_receiver_from_request
from lonelypss.ws.handlers.handler import handle_any
from lonelypss.ws.state import State, StateAccepting, StateType

router = APIRouter()


@router.websocket("/v1/websocket")
async def websocket_endpoint(websocket: WebSocket) -> None:
    """Allows sending and receiving notifications over a websocket connection,
    as opposed to the typical way this library is used (HTTP requests). This is
    helpful for the following scenarios:

    - You need to send a large number of notifications, OR
    - You need to receive a large number of notifications, OR
    - You need to receive notifications for a short period of time before unsubscribing, OR
    - You need to receive some notifications, but you cannot accept incoming HTTP requests

    For maximum compatibility with websocket clients, we only communicate
    over the websocket itself (not the http-level header fields).

    ## COMPRESSION

    For notifications (both posted and received) over websockets, this supports
    using zstandard compression. It will either use an embedded dictionary, a
    precomputed dictionary, or a trained dictionary. Under the typical settings, this:

    - Only considers messages that are between 32 and 16384 bytes for training
    - Will train once after 100kb of data is ready, and once more after 10mb of data is ready,
      then will sample 10mb every 24 hours
    - Will only used the trained dictionary on messages that would be used for training

    ## MESSAGES

    messages always begin as follows

    - 2 bytes (F): flags (interpret as big-endian):
        - least significant bit (1): 0 if headers are expanded, 1 if headers are minimal
    - 2 bytes (T): type of message; see below, depends on if it's sent by a subscriber
      or the broadcaster big-endian encoded, unsigned

    EXPANDED HEADERS:
        - 2 bytes (N): number of headers, big-endian encoded, unsigned
        - REPEAT N:
            - 2 bytes (M): length of header name, big-endian encoded, unsigned
            - M bytes: header name, ascii-encoded
            - 2 bytes (L): length of header value, big-endian encoded, unsigned
            - L bytes: header value

    MINIMAL HEADERS:
    the order of the headers are fixed based on the type, in the order documented.
    Given N headers:
    - Repeat N:
        - 2 bytes (L): length of header value, big-endian encoded, unsigned
        - L bytes: header value

    ## Messages Sent to the Broadcaster

    1: Configure:
        configures the broadcasters behavior; may be set at most once and must be
        sent and confirmed before doing anything else if the url is relevant for
        the authorization header

        headers:
        - x-subscriber-nonce: 32 random bytes representing the subscriber's contribution
            to the nonce. The broadcaster will provide its contribution in the response.
        - x-enable-zstd: 1 byte, big-endian, unsigned. 0 to disable zstandard compression,
            1 to indicate the client is willing to receive zstandard compressed messages.
        - x-enable-training: 1 byte, big-endian, unsigned. 0 to indicate the client will not
        accept custom compression dictionaries, 1 to indicate the client may accept them.
        - x-initial-dict: 2 bytes, big-endian, unsigned. 0 to indicate the client does not
        have a specific preset dictionary in mind to use, otherwise, the id of the preset
        dictionary the client thinks is a good fit for this connection

        body:
            none
    2: Subscribe Exact:
        subscribe to an exact topic

        headers:
            - authorization (url: websocket:<nonce>:<ctr>, see below)
            - x-topic: the topic to subscribe to
        body: none
    3: Subscribe Glob:
        subscribe to a glob pattern

        headers:
            - authorization (url: websocket:<nonce>:<ctr>, see below)
            - x-glob: the glob pattern to subscribe to
        body: none
    4: Unsubscribe Exact:
        unsubscribe from an exact topic

        headers:
            - authorization (url: websocket:<nonce>:<ctr>, see below)
            - x-topic: the topic to unsubscribe from
    5: Unsubscribe Glob:
        unsubscribe from a glob pattern

        headers:
            - authorization (url: websocket:<nonce>:<ctr>, see below)
            - x-glob: the glob pattern to unsubscribe from
    6: Notify:
        send a notification within a single websocket message (typically, max 16MB). this
        can be suitable for arbitrary websocket sizes depending on the configuration of the
        broadcaster (e.g., uvicorn and all intermediaries might limit max ws message sizes)

        headers:
            - authorization (url: websocket:<nonce>:<ctr>, see below)
            - x-identifier identifies the notification so the broadcaster can confirm it
            - x-topic is the topic of the notification
            - x-compressor is a big-endian unsigned integer representing one of the previous
            compression methods enabled by the broadcaster
            - x-compressed-length the total length of the compressed body, big-endian, unsigned, max 8 bytes
            - x-decompressed-length the total length of the decompressed body, big-endian, unsigned, max 8 bytes
            - x-compressed-sha512 the sha-512 hash of the compressed content once all parts are concatenated, 64 bytes

        body: the message to send. must have the same hash as the provided hash
    7: Notify Stream:
        send a notification over multiple websocket messages. this is more likely to work on
        typical setups when the notification payload exceeds 16MB.

        headers:
            - authorization (url: websocket:<nonce>:<ctr>, see below)
            - x-identifier identifies the notify whose compressed body is being appended. arbitrary blob, max 64 bytes
            - x-part-id starts at 0 and increments by 1 for each part. interpreted unsigned, big-endian, max 8 bytes
            - x-topic iff x-part-id is 0, the topic of the notification
            - x-compressor iff x-part-id is 0, either 0 for no compression, 1
              for zstandard compression without a custom dictionary, and
              otherwise the id of the compressor from one of the
              "Enable X compression" broadcaster->subscriber messages
            - x-compressed-length iff x-part-id is 0, the total length of the compressed body, big-endian, unsigned, max 8 bytes
            - x-decompressed-length iff x-part-id is 0, the total length of the decompressed body, big-endian, unsigned, max 8 bytes
            - x-compressed-sha512 iff x-part-id is 0, the sha-512 hash of the compressed content once all parts are concatenated, 64 bytes

        body:
            - blob of data to append to the compressed notification body
    8: Continue Receive:
        confirms that the subscriber received part of a streamed notification and needs more

        headers:
        - x-identifier: the identifier of the notification the subscriber needs more parts for
        - x-part-id: the part id that they received up to, big-endian, unsigned, max 8 bytes

        body: none
    9. Confirm Receive:
        confirms that the subscriber received a streamed notification

        headers:
        - x-identifier: the identifier of the notification that was sent

        body: none

    ## Messages Sent to the Subscriber

    1: Configure Confirmation:
        confirms we received the configuration options from the subscriber

        headers:
            - `x-broadcaster-nonce`: (32 bytes)
                the broadcasters contribution for random bytes to the nonce.
                the connection nonce is SHA256(subscriber_nonce CONCAT broadcaster_nonce),
                which is used in the url for generating the authorization header
                when the broadcaster sends a notification to the receiver over
                this websocket and when the subscriber subscribers to a topic over
                this websocket.

                the url is of the form `websocket:<nonce>:<ctr>`, where the ctr is
                a signed 8-byte integer that starts at 1 (or -1) and that depends on if it
                was sent by the broadcaster or subscriber. Both the subscriber and
                broadcaster keep track of both counters; the subscribers counter
                is always negative and decremented by 1 after each subscribe or unsubscribe
                request, the broadcasters counter is always positive and incremented by 1 after
                each notification sent. The nonce is base64url encoded, the ctr is
                hex encoded without a leading 0x and unpadded, e.g.,
                `websocket:abc123:10ffffffffffffff` or `websocket:abc123:-1a`. note that
                the counter changes every time an authorization header is provided,
                even within a single "operation", so e.g. a Notify Stream message broken
                into 6 parts will change the counter 6 times.

    2: Subscribe Exact Confirmation:
        confirms that the subscriber will receive notifications for the given topic

        headers:
            - x-topic: the topic that the subscriber is now subscribed to

        body: none
    3. Subscribe Glob Confirmation:
        confirms that the subscriber will receive notifications for the given glob pattern

        headers:
            - x-glob: the pattern that the subscriber is now subscribed to

        body: none
    4: Unsubscribe Exact Confirmation:
        confirms that the subscriber will no longer receive notifications for the given topic

        headers:
            - x-topic: the topic that the subscriber is now unsubscribed from

        body: none
    5: Unsubscribe Glob Confirmation:
        confirms that the subscriber will no longer receive notifications for the given glob pattern

        headers:
            - x-glob: the pattern that the subscriber is now unsubscribed from

        body: none
    6: Notify Confirmation:
        confirms that we sent a notification to subscribers; this is also sent
        for streamed notifications after the last part was received by the broadcaster

        headers:
            - x-identifier: the identifier of the notification that was sent
            - x-subscribers: the number of subscribers that received the notification

        body: none
    7: Notify Continue:
        confirms that we received a part of a streamed notification but need more. You
        do not need to wait for this before continuing, and should never retry WS messages
        as the underlying protocol already handles retries. to abort a send, close the WS
        and reconnect

        headers:
            - x-identifier: the identifier of the notification we need more parts for
            - x-part-id: the part id that we received up to, big-endian, unsigned

        body: none
    8: Receive Stream
        tells the subscriber about a notification on a topic they are subscribed to, possibly
        over multiple messages

        headers:
            - authorization (url: websocket:<nonce>:<ctr>, see above)
            - x-identifier identifies the notify whose compressed body is being appended. arbitrary blob, max 64 bytes
            - x-part-id starts at 0 and increments by 1 for each part. interpreted unsigned, big-endian, max 8 bytes
            - x-topic iff x-part-id is 0, the topic of the notification
            - x-compressor iff x-part-id is 0, either 0 for no compression, 1 for no custom dictionary zstd, and
              otherwise the id of the compressor from one of
              the "Enable X compression" broadcaster->subscriber messages
            - x-compressed-length iff x-part-id is 0, the total length of the compressed body, big-endian, unsigned, max 8 bytes
            - x-decompressed-length iff x-part-id is 0, the total length of the decompressed body, big-endian, unsigned, max 8 bytes
            - x-compressed-sha512 iff x-part-id is 0, the sha-512 hash of the compressed content once all parts are concatenated, 64 bytes

        body:
            - blob of data to append to the compressed notification body
    9: Enable zstandard compression with preset dictionary
        configures the subscriber to expect and use a dictionary that it already has available.
        this may use precomputed dictionaries that were specified during the broadcaster's
        configuration with the assumption the subscriber has them

        headers:
            x-identifier: which compressor is enabled, unsigned, big-endian, max 2 bytes, min 1.
                A value of 1 means compression without a custom dictionary.
            x-compression-level: what compression level we think is best when using
                this dictionary. signed, big-endian, max 2 bytes, max 22. the subscriber
                is free to choose a different compression level
            x-min-size: 4 bytes, big-endian, unsigned. a hint to the client for the smallest
                payload for which we think this dictionary is useful. the client can use this
                dictionary on smaller messages if it wants
            x-max-size: 8 bytes, big-endian, unsigned. a hint to the client for the largest
                payload for which we think this dictionary is useful. uses 2**64-1 to indicate
                no upper bound. the client can use this dictionary on larger messages if it wants

        body: none
    10: Enable zstandard compression with a custom dictionary
        configures the subscriber to use a dictionary we just trained

        headers:
            x-identifier: the id we are assigning to this dictionary, unsigned, big-endian, max 8 bytes,
                min 65536. if not unique, disconnect
            x-compression-level: what compression level we think is best when using
                this dictionary. signed, big-endian, max 2 bytes, max 22. the subscriber
                is free to choose a different compression level
            x-min-size: inclusive, max 4 bytes, big-endian, unsigned. a hint to the subscriber for the smallest
                payload for which we think this dictionary is useful. the subscriber can use this
                dictionary on smaller messages if it wants
            x-max-size: exclusive, max 8 bytes, big-endian, unsigned. a hint to the subscriber for the largest
                payload for which the broadcaster will compress with this dictionary. uses 2**64-1 to indicate
                no upper bound. the subscriber can use this dictionary on larger messages if it wants

        body: the dictionary, typically 16-64kb

    11. Disable zstandard compression with a specific custom dictionary
        configures the subscriber to stop using a dictionary the broadcaster previously trained
        and transmitted, and indicates the broadcaster will not use it in the future
        (i.e., the subscriber can clear it from memory)

        headers:
            x-identifier: the id of the dictionary to stop using

        body: none
    """
    config = get_config_from_request(websocket)
    receiver = get_ws_receiver_from_request(websocket)

    state: State = StateAccepting(
        type=StateType.ACCEPTING,
        websocket=websocket,
        broadcaster_config=config,
        internal_receiver=receiver,
    )
    while state.type != StateType.CLOSED:
        state = await handle_any(state)
