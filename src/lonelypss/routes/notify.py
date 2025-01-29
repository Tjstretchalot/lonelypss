import base64
import hashlib
import io
import logging
import secrets
import tempfile
import time
from dataclasses import dataclass
from enum import Enum, auto
from typing import Annotated, Dict, Generic, Literal, Optional, TypeVar, Union

import aiohttp
from fastapi import APIRouter, Header, Request, Response
from lonelypsp.auth.config import AuthResult
from lonelypsp.compat import assert_never, fast_dataclass
from lonelypsp.stateless.constants import (
    BroadcasterToSubscriberStatelessMessageType,
    SubscriberToBroadcasterStatelessMessageType,
)
from lonelypsp.sync_io import PreallocatedBytesIO
from lonelypsp.tracing.shared.handle_trusted_notify import (
    HandledTrustedNotify,
    HandledTrustedNotifyHandleMissedStart,
)

from lonelypss.config.config import (
    Config,
    MissedInfo,
    SubscriberInfoExact,
    SubscriberInfoGlob,
    SubscriberInfoType,
)
from lonelypss.middleware.config import get_config_from_request
from lonelypss.util.async_io import AsyncReadableBytesIO, async_read_exact
from lonelypss.util.sync_io import (
    PositionedSyncStandardIO,
    PrefixedSyncStandardIO,
    SyncIOBaseLikeIO,
    read_exact,
)

router = APIRouter()


@router.post(
    "/v1/notify",
    status_code=200,
    responses={
        "400": {"description": "The body was not formatted correctly"},
        "401": {"description": "Authorization header is required but not provided"},
        "403": {"description": "Authorization header is provided but invalid"},
        "500": {"description": "Unexpected error occurred"},
        "503": {"description": "Service is unavailable, try again soon"},
    },
)
async def notify(
    request: Request, authorization: Annotated[Optional[str], Header()] = None
) -> Response:
    """Sends the given message to subscribers for the given topic. The body should be
    formatted as the following sequence:

    - 2 bytes (N): length of the topic, big-endian, unsigned
    - N bytes: the topic. if utf-8 decodable then we will attempt to match glob
      patterns, otherwise, only goes to exact subscriptions
    - 64 bytes: sha-512 hash of the message, will be rechecked
    - 2 bytes (T): length of the tracing data, big-endian, unsigned
    - T bytes: the tracing data
    - 1 bytes (I): length of the identifier, big-endian, unsigned
    - I bytes: the identifier
    - 8 bytes (M): length of the message, big-endian, unsigned
    - M bytes: the message

    The response has one of the following status codes, where the body is arbitrary
    unless otherwise specified.

    - 200 Okay: subscribers were notified. response is as follows:
        - 2 bytes (type): int(RESPONSE_NOTIFY), big endian, unsigned
        - 2 bytes (A): big-endian, unsigned, the length of the authorization. broadcaster
            side authorization is always used because hmac over http is supported
        - A bytes: the authorization
        - 2 bytes (T): big-endian, unsigned, the length of tracing data
        - T bytes: the tracing data
        - 2 bytes: big-endian, unsigned, the number of subscribers notified
        - 1 byte (I): length of the identifier, big-endian, unsigned
        - I bytes: the identifier
    - 400 Bad Request: the body was not formatted correctly
    - 401 Unauthorized: authorization is required but not provided
    - 403 Forbidden: authorization is provided but invalid
    - 500 Internal Server Error: unexpected error occurred
    - 503 Service Unavailable: servce (generally, database) is unavailable
    """
    config = get_config_from_request(request)

    with config.tracer.stateless.receive_notify(None) as initial_trace:
        on_auth_result_trace = initial_trace.on_received()
        del initial_trace

        with tempfile.SpooledTemporaryFile(
            max_size=config.message_body_spool_size, mode="w+b"
        ) as request_body:
            read_length = 0
            saw_end = False

            stream_iter = request.stream().__aiter__()
            while True:
                try:
                    chunk = await stream_iter.__anext__()
                except StopAsyncIteration:
                    saw_end = True
                    break

                request_body.write(chunk)
                read_length += len(chunk)
                if read_length >= 2 + 65535 + 64 + 2 + 65535 + 1 + 255 + 8:
                    break

            request_body.seek(0)
            try:
                topic_length = int.from_bytes(read_exact(request_body, 2), "big")
                topic = read_exact(request_body, topic_length)
                message_hash = read_exact(request_body, 64)
                tracing_length = int.from_bytes(read_exact(request_body, 2), "big")
                tracing = read_exact(request_body, tracing_length)
                identifier_length = int.from_bytes(read_exact(request_body, 1), "big")
                identifier = read_exact(request_body, identifier_length)
                message_length = int.from_bytes(read_exact(request_body, 8), "big")
            except ValueError:
                on_auth_result_trace.on_bad_request()
                return Response(status_code=400)
            header_length = request_body.tell()

            auth_at = time.time()
            auth_result = await config.is_notify_allowed(
                tracing=tracing,
                topic=topic,
                identifier=identifier,
                message_sha512=message_hash,
                now=auth_at,
                authorization=authorization,
            )

            if auth_result == AuthResult.UNAUTHORIZED:
                on_auth_result_trace.on_bad_auth_result(result=auth_result)
                return Response(status_code=401)
            elif auth_result == AuthResult.FORBIDDEN:
                on_auth_result_trace.on_bad_auth_result(result=auth_result)
                return Response(status_code=403)
            elif auth_result == AuthResult.UNAVAILABLE:
                on_auth_result_trace.on_bad_auth_result(result=auth_result)
                return Response(status_code=503)
            elif auth_result != AuthResult.OK:
                assert_never(auth_result)

            hasher = hashlib.sha512()
            on_auth_verified_trace = on_auth_result_trace.on_auth_tentatively_accepted()
            del on_auth_result_trace

            while True:
                chunk = request_body.read(io.DEFAULT_BUFFER_SIZE)
                if not chunk:
                    break
                hasher.update(chunk)

            if not saw_end:
                while True:
                    try:
                        chunk = await stream_iter.__anext__()
                    except StopAsyncIteration:
                        saw_end = True
                        break

                    hasher.update(chunk)
                    request_body.write(chunk)
                    read_length += len(chunk)

                    if read_length > header_length + message_length:
                        on_auth_verified_trace.on_auth_mismatch()
                        return Response(status_code=400)

            if read_length != header_length + message_length:
                on_auth_verified_trace.on_auth_mismatch()
                return Response(status_code=400)

            actual_hash = hasher.digest()
            if actual_hash != message_hash:
                on_auth_verified_trace.on_auth_mismatch()
                return Response(status_code=400)

            handle_trusted_notify_trace = on_auth_verified_trace.on_auth_accepted(
                topic=topic,
                length=message_length,
                identifier=identifier,
                tracing=tracing,
            )
            del on_auth_verified_trace

            request_body.seek(header_length)
            notify_result = await handle_trusted_notify(
                topic,
                request_body,
                config=config,
                session=config.http_notify_client_session,
                content_length=message_length,
                sha512=actual_hash,
                tracer=handle_trusted_notify_trace,
            )
            del handle_trusted_notify_trace

            if notify_result.type == TrustedNotifyResultType.UNAVAILABLE:
                return Response(status_code=503)

            resp_tracing = notify_result.tracer.on_sending_response()
            resp_authorization = await config.authorize_confirm_notify(
                tracing=resp_tracing,
                identifier=identifier,
                subscribers=notify_result.succeeded,
                topic=topic,
                message_sha512=message_hash,
                now=time.time(),
            )
            resp_authorization_bytes = (
                b""
                if resp_authorization is None
                else resp_authorization.encode("utf-8")
            )
            resp = PreallocatedBytesIO(
                2
                + 2
                + len(resp_authorization_bytes)
                + 2
                + len(resp_tracing)
                + 4
                + 1
                + len(identifier)
            )
            resp.write(
                int(
                    BroadcasterToSubscriberStatelessMessageType.RESPONSE_NOTIFY
                ).to_bytes(2, "big")
            )
            resp.write(len(resp_authorization_bytes).to_bytes(2, "big"))
            resp.write(resp_authorization_bytes)
            resp.write(len(resp_tracing).to_bytes(2, "big"))
            resp.write(resp_tracing)
            resp.write(notify_result.succeeded.to_bytes(4, "big"))
            resp.write(len(identifier).to_bytes(1, "big"))
            resp.write(identifier)

            return Response(
                status_code=200,
                content=memoryview(resp.buffer),
                headers={
                    "Content-Type": "application/octet-stream",
                },
            )


class TrustedNotifyResultType(Enum):
    UNAVAILABLE = auto()
    """We had trouble accessing the data store and may not have attempted all
    subscribers
    """
    OK = auto()
    """We at least attempted all subscribers"""


T = TypeVar("T")


@dataclass
class TrustedNotifyResultOK(Generic[T]):
    type: Literal[TrustedNotifyResultType.OK]
    """discriminator type"""
    succeeded: int
    """The number of subscribers we reached"""
    failed: int
    """The number of subscribers we could not reach"""
    tracer: T
    """The new tracer"""


@dataclass
class TrustedNotifyResultUnavailable:
    type: Literal[TrustedNotifyResultType.UNAVAILABLE]
    """discriminator type"""
    partial_succeeded: int
    """The number of subscribers we reached"""
    partial_failed: int
    """The number of subscribers we could not reach"""


async def _handle_missed(
    config: Config,
    topic: bytes,
    subscriber: Union[SubscriberInfoExact, SubscriberInfoGlob],
    tracer: HandledTrustedNotifyHandleMissedStart[T],
) -> T:
    on_done_tracer = tracer.on_handle_missed_start()
    del tracer

    if subscriber.recovery is None:
        return on_done_tracer.on_handle_missed_skipped(
            recovery=subscriber.recovery,
            next_retry_at=None,
        )

    next_retry_at = await config.get_delay_for_next_missed_retry(
        receive_url=subscriber.url,
        missed_url=subscriber.recovery,
        topic=topic,
        attempts=0,
    )
    if next_retry_at is None:
        return on_done_tracer.on_handle_missed_skipped(
            recovery=subscriber.recovery, next_retry_at=None
        )

    db_result = await config.upsert_missed(
        info=MissedInfo(
            topic=topic,
            attempts=0,
            next_retry_at=next_retry_at,
            subscriber_info=subscriber,
        )
    )

    if db_result == "unavailable":
        return on_done_tracer.on_handle_missed_unavailable(
            recovery=subscriber.recovery,
            next_retry_at=next_retry_at,
        )
    if db_result != "success":
        assert_never(db_result)

    return on_done_tracer.on_handle_missed_success(
        recovery=subscriber.recovery,
        next_retry_at=next_retry_at,
    )


async def handle_trusted_notify(
    topic: bytes,
    data: SyncIOBaseLikeIO,
    /,
    *,
    config: Config,
    session: aiohttp.ClientSession,
    content_length: int,
    sha512: bytes,
    tracer: HandledTrustedNotify[T],
) -> Union[TrustedNotifyResultOK[T], TrustedNotifyResultUnavailable]:
    """Notifies subscribers to the given topic with the given data.

    Args:
        topic (bytes): the topic the message was sent to
        data (file-like, readable, bytes): the message that was sent
        config (Config): the broadcaster configuration to use
        session (aiohttp.ClientSession): the aiohttp client session to
            send requests to clients in
        content_length (int): the length of the message in bytes. The stream
            MUST not have more than this amount of data in it, as we will read
            past this length if it does
        sha512 (bytes): the sha512 hash of the content (64 bytes)
        tracer (HandledTrustedNotify[T]): the tracer for debugging. should be
            considered unusable after this call (if OK, the return value has the new
            tracer)
    """
    succeeded = 0
    failed = 0
    headers: Dict[str, str] = {
        "Content-Type": "application/octet-stream",
        "Repr-Digest": f"sha-512={base64.b64encode(sha512).decode('ascii')}",
    }

    message_starts_at = data.tell()

    async for subscriber in config.get_subscribers(topic=topic):
        if subscriber.type == SubscriberInfoType.UNAVAILABLE:
            tracer.on_unavailable()
            return TrustedNotifyResultUnavailable(
                type=TrustedNotifyResultType.UNAVAILABLE,
                partial_succeeded=succeeded,
                partial_failed=failed,
            )

        if subscriber.type == SubscriberInfoType.EXACT:
            sending_receive_tracer = tracer.on_exact_subscriber_found(
                url=subscriber.url
            )
            del tracer
        elif subscriber.type == SubscriberInfoType.GLOB:
            sending_receive_tracer = tracer.on_glob_subscriber_found(
                glob=subscriber.glob, url=subscriber.url
            )
            del tracer
        else:
            assert_never(subscriber)

        failed += 1
        my_identifier = secrets.token_bytes(8)
        my_tracing_and_followup = sending_receive_tracer.on_sending_receive(
            identifier=my_identifier
        )
        del sending_receive_tracer
        my_tracing = my_tracing_and_followup.tracing
        on_received_response_tracer = my_tracing_and_followup.followup
        my_authorization = await config.authorize_receive(
            tracing=my_tracing,
            url=subscriber.url,
            topic=topic,
            message_sha512=sha512,
            identifier=my_identifier,
            now=time.time(),
        )
        if my_authorization is None:
            headers.pop("Authorization", None)
        else:
            headers["Authorization"] = my_authorization

        message_prefix = PreallocatedBytesIO(
            2 + len(my_tracing) + 2 + len(topic) + 1 + len(my_identifier) + 8
        )
        message_prefix.write(len(my_tracing).to_bytes(2, "big"))
        message_prefix.write(my_tracing)
        message_prefix.write(len(topic).to_bytes(2, "big"))
        message_prefix.write(topic)
        message_prefix.write(len(my_identifier).to_bytes(1, "big"))
        message_prefix.write(my_identifier)
        message_prefix.write(content_length.to_bytes(8, "big"))
        guarded_request_body = PrefixedSyncStandardIO(
            prefix=PositionedSyncStandardIO(
                stream=message_prefix, start_idx=0, end_idx=len(message_prefix.buffer)
            ),
            child=PositionedSyncStandardIO(
                stream=data,
                start_idx=message_starts_at,
                end_idx=message_starts_at + content_length,
            ),
        )
        headers["Content-Length"] = str(len(guarded_request_body))
        try:
            raw_resp = session.post(
                subscriber.url,
                # requires monkey patching for reuse
                # https://github.com/aio-libs/aiohttp/issues/10325
                data=guarded_request_body,
                headers=headers,
            )
            resp = await raw_resp.__aenter__()
        except aiohttp.ClientError:
            handle_missed_start_tracer = on_received_response_tracer.on_network_error()
            del on_received_response_tracer
            logging.error(
                f"Failed to notify {subscriber.url} about {topic!r}", exc_info=True
            )
            tracer = await _handle_missed(
                config, topic, subscriber, handle_missed_start_tracer
            )
            continue

        on_auth_result_tracer = on_received_response_tracer.on_response_received(
            status_code=resp.status
        )
        del on_received_response_tracer

        try:
            try:
                parsed_resp = await _parse_receive_response(resp.content)
            finally:
                await raw_resp.__aexit__(None, None, None)
        except Exception:
            logging.error(
                f"Failed to notify {subscriber.url} about {topic!r}", exc_info=True
            )
            handle_missed_start_tracer = on_auth_result_tracer.on_bad_receive_response()
            del on_auth_result_tracer
            tracer = await _handle_missed(
                config, topic, subscriber, handle_missed_start_tracer
            )
            del handle_missed_start_tracer
            continue

        if (
            parsed_resp.type
            == SubscriberToBroadcasterStatelessMessageType.RESPONSE_UNSUBSCRIBE_IMMEDIATE
        ):
            on_unsubscribe_immediate_done_tracer = (
                on_auth_result_tracer.on_unsubscribe_immediate_requested()
            )
            del on_auth_result_tracer

            if subscriber.type == SubscriberInfoType.EXACT:
                db_result = await config.unsubscribe_exact(
                    url=subscriber.url, exact=topic
                )
            elif subscriber.type == SubscriberInfoType.GLOB:
                db_result = await config.unsubscribe_glob(
                    url=subscriber.url, glob=subscriber.glob
                )
            else:
                assert_never(subscriber)

            if db_result == "unavailable":
                on_unsubscribe_immediate_done_tracer.on_unsubscribe_immediate_unavailable()
                raise Exception("db unavailable to process unsubscribe immediate")

            if db_result == "not_found":
                tracer = (
                    on_unsubscribe_immediate_done_tracer.on_unsubscribe_immediate_not_found()
                )
                continue

            if db_result != "success":
                assert_never(db_result)

            tracer = (
                on_unsubscribe_immediate_done_tracer.on_unsubscribe_immediate_success()
            )
            continue

        if (
            parsed_resp.type
            != SubscriberToBroadcasterStatelessMessageType.RESPONSE_CONFIRM_RECEIVE
        ):
            assert_never(parsed_resp)

        auth_result = await config.is_confirm_receive_allowed(
            tracing=parsed_resp.tracing,
            identifier=parsed_resp.identifier,
            num_subscribers=parsed_resp.num_subscribers,
            url=subscriber.url,
            now=time.time(),
            authorization=parsed_resp.authorization,
        )

        if auth_result != AuthResult.OK:
            handle_missed_start_tracer = (
                on_auth_result_tracer.on_bad_receive_auth_result(result=auth_result)
            )
            del on_auth_result_tracer
            tracer = await _handle_missed(
                config, topic, subscriber, handle_missed_start_tracer
            )
            del handle_missed_start_tracer
            continue

        tracer = on_auth_result_tracer.on_receive_confirmed(
            tracing=parsed_resp.tracing,
            num_subscribers=parsed_resp.num_subscribers,
        )
        del on_auth_result_tracer

        failed -= 1
        succeeded += parsed_resp.num_subscribers

    final_tracer = tracer.on_no_more_subscribers()
    del tracer

    return TrustedNotifyResultOK(
        type=TrustedNotifyResultType.OK,
        succeeded=succeeded,
        failed=failed,
        tracer=final_tracer,
    )


@fast_dataclass
class _ReceiveResponseUnsubscribeImmediate:
    type: Literal[
        SubscriberToBroadcasterStatelessMessageType.RESPONSE_UNSUBSCRIBE_IMMEDIATE
    ]


@fast_dataclass
class _ReceiveResponseConfirmReceive:
    type: Literal[SubscriberToBroadcasterStatelessMessageType.RESPONSE_CONFIRM_RECEIVE]
    authorization: Optional[str]
    tracing: bytes
    identifier: bytes
    num_subscribers: int


async def _parse_receive_response(
    rdr: AsyncReadableBytesIO,
) -> Union[_ReceiveResponseUnsubscribeImmediate, _ReceiveResponseConfirmReceive]:
    """Raises ValueError typically if the body is bad"""
    header_type_bytes = await async_read_exact(rdr, 2)
    header_type = int.from_bytes(header_type_bytes, "big")

    if header_type == int(
        SubscriberToBroadcasterStatelessMessageType.RESPONSE_UNSUBSCRIBE_IMMEDIATE
    ):
        return _ReceiveResponseUnsubscribeImmediate(
            type=SubscriberToBroadcasterStatelessMessageType.RESPONSE_UNSUBSCRIBE_IMMEDIATE
        )

    if header_type != int(
        SubscriberToBroadcasterStatelessMessageType.RESPONSE_CONFIRM_RECEIVE
    ):
        raise ValueError(f"unexpected response first 2 bytes: {header_type!r}")

    authorization_length_bytes = await async_read_exact(rdr, 2)
    authorization_length = int.from_bytes(authorization_length_bytes, "big")
    authorization_bytes = await async_read_exact(rdr, authorization_length)
    authorization = (
        authorization_bytes.decode("utf-8") if authorization_bytes != b"" else None
    )

    tracing_length_bytes = await async_read_exact(rdr, 2)
    tracing_length = int.from_bytes(tracing_length_bytes, "big")
    tracing = await async_read_exact(rdr, tracing_length)

    identifier_length_bytes = await async_read_exact(rdr, 1)
    identifier_length = int.from_bytes(identifier_length_bytes, "big")
    identifier = await async_read_exact(rdr, identifier_length)

    num_subscribers_bytes = await async_read_exact(rdr, 4)
    num_subscribers = int.from_bytes(num_subscribers_bytes, "big")
    return _ReceiveResponseConfirmReceive(
        type=SubscriberToBroadcasterStatelessMessageType.RESPONSE_CONFIRM_RECEIVE,
        authorization=authorization,
        tracing=tracing,
        identifier=identifier,
        num_subscribers=num_subscribers,
    )
