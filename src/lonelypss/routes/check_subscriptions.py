import time
from typing import Annotated, Optional

from fastapi import APIRouter, Header, Request
from fastapi.responses import Response
from lonelypsp.auth.config import AuthResult
from lonelypsp.compat import assert_never
from lonelypsp.stateless.constants import BroadcasterToSubscriberStatelessMessageType
from lonelypsp.sync_io import PreallocatedBytesIO

from lonelypss.middleware.config import get_config_from_request
from lonelypss.util.async_io import async_read_exact
from lonelypss.util.request_body_io import AsyncIterableAIO

router = APIRouter()


@router.post(
    "/v1/check_subscriptions",
    status_code=200,
    responses={
        401: {"description": "Authorization header is required but not provided"},
        403: {"description": "Authorization header is provided but invalid"},
        500: {"description": "Unexpected error occurred"},
        503: {"description": "Service is unavailable, try again soon"},
    },
)
async def check_subscriptions(
    request: Request, authorization: Annotated[Optional[str], Header()] = None
) -> Response:
    """Retrieves the strong etag representing the subscriptions associated with
    the URL indicated in the request body. The exact way that the etag is
    produced is in the lonelypsp documentation; it is expected that the
    subscriber uses this endpoint to confirm the subscriptions for the url
    are still as expected by computing the expected strong etag, then
    comparing it with the etag provided in the response. If incorrect, the
    subscriber is expected to use `/v1/set_subscriptions` to update the
    subscriptions.

    NOTE: this operation is not atomic; if subscriptions are updated during
    the call, then only the following is guarranteed:

    - etag includes every subscription in the db the entire call
    - etag does not include any subscription never in the db the entire call
    - etag does not include duplicates

    ### request body
    - 2 bytes (N): length of the subscriber url to check, big-endian, unsigned
    - N bytes: the url to check, utf-8 encoded

    ### response body
    - 2 bytes (type): int(RESPONSE_CHECK_SUBSCRIPTIONS), big endian, unsigned
    - 2 bytes (A): big-endian, unsigned, the length of the authorization
    - A bytes: the authorization
    - 2 bytes (T): big-endian, unsigned, the length of tracing data
    - T bytes: the tracing data
    - 1 byte (reserved for etag format): 0
    - 64 bytes: the etag
    """
    config = get_config_from_request(request)

    try:
        stream = request.stream()
        try:
            body = AsyncIterableAIO(stream.__aiter__())

            tracing_length_bytes = await async_read_exact(body, 2)
            tracing_length = int.from_bytes(tracing_length_bytes, "big")
            tracing = await async_read_exact(body, tracing_length)
            url_length_bytes = await async_read_exact(body, 2)
            url_length = int.from_bytes(url_length_bytes, "big")
            url_bytes = await async_read_exact(body, url_length)
            url = url_bytes.decode("utf-8")
        finally:
            await stream.aclose()
    except ValueError:
        return Response(status_code=400)

    auth_at = time.time()
    auth_result = await config.is_check_subscriptions_allowed(
        tracing=tracing, url=url, now=auth_at, authorization=authorization
    )

    if auth_result == AuthResult.UNAUTHORIZED:
        return Response(status_code=401)
    elif auth_result == AuthResult.FORBIDDEN:
        return Response(status_code=403)
    elif auth_result == AuthResult.UNAVAILABLE:
        return Response(status_code=503)
    elif auth_result != AuthResult.OK:
        assert_never(auth_result)

    etag = await config.check_subscriptions(url=url)

    resp_tracing = b""  # TODO: tracing
    resp_authorization = await config.authorize_check_subscriptions_response(
        tracing=resp_tracing,
        strong_etag=etag,
        now=time.time(),
    )
    resp_authorization_bytes = (
        b"" if resp_authorization is None else resp_authorization.encode("utf-8")
    )
    result = PreallocatedBytesIO(
        2
        + 2
        + len(resp_authorization_bytes)
        + 2
        + len(resp_tracing)
        + 1
        + len(etag.etag)
    )
    result.write(
        int(
            BroadcasterToSubscriberStatelessMessageType.RESPONSE_CHECK_SUBSCRIPTIONS
        ).to_bytes(2, "big")
    )
    result.write(len(resp_authorization_bytes).to_bytes(2, "big"))
    result.write(resp_authorization_bytes)
    result.write(len(resp_tracing).to_bytes(2, "big"))
    result.write(resp_tracing)
    result.write(etag.format.to_bytes(1, "big"))
    result.write(etag.etag)
    return Response(
        content=memoryview(result.buffer),
        headers={"Content-Type": "application/octet-stream"},
        status_code=200,
    )
