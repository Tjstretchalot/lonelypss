import time
from typing import Annotated, Optional

from fastapi import APIRouter, Header, Request, Response

from lonelypss.middleware.config import get_config_from_request
from lonelypss.util.async_io import async_read_exact
from lonelypss.util.request_body_io import AsyncIterableAIO

router = APIRouter()


@router.post(
    "/v1/unsubscribe/glob",
    status_code=202,
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

    - 2 bytes: the length of the url, big-endian, unsigned
    - N bytes: the url. must be valid utf-8
    - 2 bytes: the length of the glob
    - M bytes: the glob. must be valid utf-8

    The response has an arbitrary body (generally empty) and one of the
    following status codes:

    - 200 Okay: the subscription was removed
    - 400 Bad Request: the body was not formatted correctly
    - 401 Unauthorized: authorization is required but not provided
    - 403 Forbidden: authorization is provided but invalid
    - 409 Conflict: the subscription already exists
    - 500 Internal Server Error: unexpected error occurred
    - 503 Service Unavailable: servce (generally, database) is unavailable
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
        finally:
            await stream.aclose()
    except ValueError:
        return Response(status_code=400)

    auth_at = time.time()
    auth_result = await config.is_subscribe_glob_allowed(
        url=url, recovery=None, glob=glob, now=auth_at, authorization=authorization
    )

    if auth_result == "unauthorized":
        return Response(status_code=401)
    elif auth_result == "forbidden":
        return Response(status_code=403)
    elif auth_result == "unavailable":
        return Response(status_code=503)
    elif auth_result != "ok":
        return Response(status_code=500)

    db_result = await config.unsubscribe_glob(url=url, glob=glob)

    if db_result == "not_found":
        return Response(status_code=409)
    elif db_result == "unavailable":
        return Response(status_code=503)
    elif db_result != "success":
        return Response(status_code=500)

    return Response(status_code=200)
