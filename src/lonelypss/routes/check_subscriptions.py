import time
from typing import Annotated, Optional

from fastapi import APIRouter, Header, Request
from fastapi.responses import Response

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
    - 1 byte (reserved for etag format): 0
    - 64 bytes: the etag
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
        finally:
            await stream.aclose()
    except ValueError:
        return Response(status_code=400)

    auth_at = time.time()
    auth_result = await config.is_check_subscriptions_allowed(
        url=url, now=auth_at, authorization=authorization
    )

    if auth_result == "unauthorized":
        return Response(status_code=401)
    elif auth_result == "forbidden":
        return Response(status_code=403)
    elif auth_result == "unavailable":
        return Response(status_code=503)
    assert auth_result == "ok"

    etag = await config.check_subscriptions(url=url)

    result = bytearray(1 + len(etag.etag))
    result[0] = etag.format
    result[1:] = etag.etag
    return Response(
        content=result,
        headers={"Content-Type": "application/octet-stream"},
        status_code=200,
    )
