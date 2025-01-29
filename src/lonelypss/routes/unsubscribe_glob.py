import time
from typing import Annotated, Optional

from fastapi import APIRouter, Header, Request, Response
from lonelypsp.auth.config import AuthResult
from lonelypsp.compat import assert_never
from lonelypsp.stateless.constants import BroadcasterToSubscriberStatelessMessageType
from lonelypsp.sync_io import PreallocatedBytesIO

from lonelypss.middleware.config import get_config_from_request
from lonelypss.util.async_io import async_read_exact
from lonelypss.util.request_body_io import AsyncIterableAIO

router = APIRouter()


@router.post(
    "/v1/unsubscribe/glob",
    status_code=200,
    responses={
        "400": {"description": "The body was not formatted correctly"},
        "401": {"description": "Authorization header is required but not provided"},
        "403": {"description": "Authorization header is provided but invalid"},
        "409": {"description": "The subscription does not exist"},
        "500": {"description": "Unexpected error occurred"},
        "503": {"description": "Service is unavailable, try again soon"},
    },
)
async def unsubscribe_exact(
    request: Request, authorization: Annotated[Optional[str], Header()] = None
) -> Response:
    """Unsubscribes the given URL from the given glob. The body should be
    formatted as the following sequence:

    - 2 bytes (N): length of the url that the subscriber can be reached at, big-endian, unsigned
    - N bytes: the url that the subscriber can be reached at, utf-8 encoded
    - 2 bytes (M): the length of the glob pattern, big-endian, unsigned
    - M bytes: the glob pattern, utf-8 encoded
    - 2 bytes (T): length of the tracing data, big-endian, unsigned
    - T bytes: the tracing data

    The response has an arbitrary body (generally empty) and one of the
    following status codes:

    - 200 Okay: the subscription was removed
    - 400 Bad Request: the body was not formatted correctly
    - 401 Unauthorized: authorization is required but not provided
    - 403 Forbidden: authorization is provided but invalid
    - 409 Conflict: the subscription already exists
    - 500 Internal Server Error: unexpected error occurred
    - 503 Service Unavailable: servce (generally, database) is unavailable

    ### response body for 200 Okay
    - `BroadcasterToSubscriberStatelessMessageType.RESPONSE_GENERIC`
    """
    config = get_config_from_request(request)

    try:
        stream = request.stream()
        try:
            body = AsyncIterableAIO(stream.__aiter__())

            url_length_bytes = await async_read_exact(body, 2)
            url_length = int.from_bytes(url_length_bytes, "big")
            url_bytes = await async_read_exact(body, url_length)
            url = url_bytes.decode("utf-8")

            glob_length_bytes = await async_read_exact(body, 2)
            glob_length = int.from_bytes(glob_length_bytes, "big")
            glob_bytes = await async_read_exact(body, glob_length)
            glob = glob_bytes.decode("utf-8")

            tracing_length_bytes = await async_read_exact(body, 2)
            tracing_length = int.from_bytes(tracing_length_bytes, "big")
            tracing = await async_read_exact(body, tracing_length)
        finally:
            await stream.aclose()
    except ValueError:
        return Response(status_code=400)

    auth_at = time.time()
    auth_result = await config.is_subscribe_glob_allowed(
        tracing=tracing,
        url=url,
        recovery=None,
        glob=glob,
        now=auth_at,
        authorization=authorization,
    )

    if auth_result == AuthResult.UNAUTHORIZED:
        return Response(status_code=401)
    elif auth_result == AuthResult.FORBIDDEN:
        return Response(status_code=403)
    elif auth_result == AuthResult.UNAVAILABLE:
        return Response(status_code=503)
    elif auth_result != AuthResult.OK:
        assert_never(auth_result)

    db_result = await config.unsubscribe_glob(url=url, glob=glob)

    if db_result == "not_found":
        return Response(status_code=409)
    elif db_result == "unavailable":
        return Response(status_code=503)
    elif db_result != "success":
        assert_never(db_result)

    resp_tracing = b""  # TODO: tracing
    resp_authorization = await config.authorize_confirm_subscribe_glob(
        tracing=resp_tracing,
        url=url,
        recovery=None,
        glob=glob,
        now=auth_at,
    )
    resp_authorization_bytes = (
        b"" if resp_authorization is None else resp_authorization.encode("utf-8")
    )

    resp_body = PreallocatedBytesIO(
        2 + 2 + len(resp_authorization_bytes) + 2 + len(resp_tracing)
    )
    resp_body.write(
        BroadcasterToSubscriberStatelessMessageType.RESPONSE_GENERIC.to_bytes(2, "big")
    )
    resp_body.write(len(resp_authorization_bytes).to_bytes(2, "big"))
    resp_body.write(resp_authorization_bytes)
    resp_body.write(len(resp_tracing).to_bytes(2, "big"))
    resp_body.write(resp_tracing)

    return Response(
        status_code=200,
        headers={"Content-Type": "application/octet-stream"},
        content=memoryview(resp_body.buffer),
    )
