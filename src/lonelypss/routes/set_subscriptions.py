import asyncio
import io
import os
import time
from enum import Enum, auto
from tempfile import SpooledTemporaryFile
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Annotated,
    AsyncIterator,
    Literal,
    Optional,
    Type,
    Union,
    cast,
)

from fastapi import APIRouter, Header, Request
from fastapi.responses import Response
from lonelypsp.stateless.make_strong_etag import (
    GlobAndRecovery,
    StrongEtag,
    TopicAndRecovery,
    create_strong_etag_generator,
)

from lonelypss.config.set_subscriptions_info import SetSubscriptionsInfo
from lonelypss.middleware.config import get_config_from_request
from lonelypss.util.async_io import AsyncReadableBytesIO, async_read_exact
from lonelypss.util.request_body_io import AsyncIterableAIO
from lonelypss.util.sync_io import SyncIOBaseLikeIO, SyncReadableBytesIO, read_exact

router = APIRouter()


class _SubscriptionInfoFromStreamState(Enum):
    NOT_ENTERED = auto()
    TOPICS = auto()
    GLOBS_NOT_PEEKED = auto()
    GLOBS = auto()
    CLOSED = auto()


class _SubscriptionInfoFromStreamTopicsIter:
    def __init__(
        self,
        parent: "Union[_SubscriptionInfoFromSyncStream, _SubscriptionInfoFromAsyncStream]",
    ) -> None:
        self.parent = parent

    def __aiter__(self) -> "_SubscriptionInfoFromStreamTopicsIter":
        return self

    async def __anext__(self) -> TopicAndRecovery:
        assert self.parent.state == _SubscriptionInfoFromStreamState.TOPICS, "destroyed"
        return cast(TopicAndRecovery, await self.parent.__anext__())


class _SubscriptionInfoFromStreamGlobsIter:
    def __init__(
        self,
        parent: "Union[_SubscriptionInfoFromSyncStream, _SubscriptionInfoFromAsyncStream]",
    ) -> None:
        self.parent = parent

    def __aiter__(self) -> "_SubscriptionInfoFromStreamGlobsIter":
        return self

    async def __anext__(self) -> GlobAndRecovery:
        assert (
            self.parent.state == _SubscriptionInfoFromStreamState.GLOBS_NOT_PEEKED
            or self.parent.state == _SubscriptionInfoFromStreamState.GLOBS
        ), "destroyed"
        return cast(GlobAndRecovery, await self.parent.__anext__())


class _SubscriptionInfoFromSyncStream:
    def __init__(self, stream: SyncReadableBytesIO) -> None:
        self.stream = stream
        self.state: _SubscriptionInfoFromStreamState = (
            _SubscriptionInfoFromStreamState.NOT_ENTERED
        )
        self.remaining: int = 0

    def __enter__(self) -> "_SubscriptionInfoFromSyncStream":
        assert (
            self.state == _SubscriptionInfoFromStreamState.NOT_ENTERED
        ), "not reentrant"

        try:
            num_topics_bytes = read_exact(self.stream, 4)
            num_topics = int.from_bytes(num_topics_bytes, "big")
            self.remaining = num_topics
            self.state = _SubscriptionInfoFromStreamState.TOPICS
        except BaseException:
            self.state = _SubscriptionInfoFromStreamState.CLOSED
            raise

        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        assert self.state != _SubscriptionInfoFromStreamState.NOT_ENTERED, "not entered"
        self.state = _SubscriptionInfoFromStreamState.CLOSED

    async def __anext__(self) -> Union[GlobAndRecovery, TopicAndRecovery]:
        if self.state == _SubscriptionInfoFromStreamState.TOPICS:
            if self.remaining <= 0:
                raise StopAsyncIteration

            try:
                topic_length_bytes = read_exact(self.stream, 2)
                topic_length = int.from_bytes(topic_length_bytes, "big")
                topic = read_exact(self.stream, topic_length)
                recovery_length_bytes = read_exact(self.stream, 2)
                recovery_length = int.from_bytes(recovery_length_bytes, "big")
                recovery_bytes = (
                    None
                    if recovery_length == 0
                    else read_exact(self.stream, recovery_length)
                )
                recovery_str = (
                    None if recovery_bytes is None else recovery_bytes.decode("utf-8")
                )
                self.remaining -= 1
            except BaseException:
                self.state = _SubscriptionInfoFromStreamState.CLOSED
                raise

            return TopicAndRecovery(topic, recovery_str)

        if self.state == _SubscriptionInfoFromStreamState.GLOBS_NOT_PEEKED:
            try:
                while self.remaining > 0:
                    topic_length_bytes = read_exact(self.stream, 2)
                    topic_length = int.from_bytes(topic_length_bytes, "big")
                    read_exact(self.stream, topic_length)
                    recovery_length_bytes = read_exact(self.stream, 2)
                    recovery_length = int.from_bytes(recovery_length_bytes, "big")
                    if recovery_length > 0:
                        read_exact(self.stream, recovery_length)
                    self.remaining -= 1
                    if self.remaining % 10 == 0:
                        await asyncio.sleep(0)

                num_globs_bytes = read_exact(self.stream, 4)
                num_globs = int.from_bytes(num_globs_bytes, "big")
                self.remaining = num_globs
                self.state = _SubscriptionInfoFromStreamState.GLOBS
            except BaseException:
                self.state = _SubscriptionInfoFromStreamState.CLOSED
                raise

        if self.state == _SubscriptionInfoFromStreamState.GLOBS:
            if self.remaining <= 0:
                raise StopAsyncIteration

            try:
                glob_length_bytes = read_exact(self.stream, 2)
                glob_length = int.from_bytes(glob_length_bytes, "big")
                glob_bytes = read_exact(self.stream, glob_length)
                glob = glob_bytes.decode("utf-8")
                recovery_length_bytes = read_exact(self.stream, 2)
                recovery_length = int.from_bytes(recovery_length_bytes, "big")
                recovery_bytes = (
                    None
                    if recovery_length == 0
                    else read_exact(self.stream, recovery_length)
                )
                recovery_str = (
                    None if recovery_bytes is None else recovery_bytes.decode("utf-8")
                )
                self.remaining -= 1
            except BaseException:
                self.state = _SubscriptionInfoFromStreamState.CLOSED
                raise

            return GlobAndRecovery(glob, recovery_str)

        raise AssertionError("not topics or globs")

    def __aiter__(self) -> "_SubscriptionInfoFromSyncStream":
        return self

    def topics(self) -> AsyncIterator[TopicAndRecovery]:
        assert self.state == _SubscriptionInfoFromStreamState.TOPICS, "not topics"
        return _SubscriptionInfoFromStreamTopicsIter(self)

    def globs(self) -> AsyncIterator[GlobAndRecovery]:
        if self.state == _SubscriptionInfoFromStreamState.TOPICS:
            self.state = _SubscriptionInfoFromStreamState.GLOBS_NOT_PEEKED

        if (
            self.state != _SubscriptionInfoFromStreamState.GLOBS_NOT_PEEKED
            and self.state != _SubscriptionInfoFromStreamState.GLOBS
        ):
            raise AssertionError("not globs")

        return _SubscriptionInfoFromStreamGlobsIter(self)


class _SubscriptionInfoFromAsyncStream:
    def __init__(self, stream: AsyncReadableBytesIO) -> None:
        self.stream = stream
        self.state: _SubscriptionInfoFromStreamState = (
            _SubscriptionInfoFromStreamState.NOT_ENTERED
        )
        self.remaining: int = 0

    async def __aenter__(self) -> "_SubscriptionInfoFromAsyncStream":
        assert (
            self.state == _SubscriptionInfoFromStreamState.NOT_ENTERED
        ), "not reentrant"

        try:
            num_topics_bytes = await async_read_exact(self.stream, 4)
            num_topics = int.from_bytes(num_topics_bytes, "big")
            self.remaining = num_topics
            self.state = _SubscriptionInfoFromStreamState.TOPICS
        except BaseException:
            self.state = _SubscriptionInfoFromStreamState.CLOSED
            raise

        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        assert self.state != _SubscriptionInfoFromStreamState.NOT_ENTERED, "not entered"
        self.state = _SubscriptionInfoFromStreamState.CLOSED

    async def __anext__(self) -> Union[GlobAndRecovery, TopicAndRecovery]:
        if self.state == _SubscriptionInfoFromStreamState.TOPICS:
            if self.remaining <= 0:
                raise StopAsyncIteration

            try:
                topic_length_bytes = await async_read_exact(self.stream, 2)
                topic_length = int.from_bytes(topic_length_bytes, "big")
                topic = await async_read_exact(self.stream, topic_length)
                recovery_length_bytes = await async_read_exact(self.stream, 2)
                recovery_length = int.from_bytes(recovery_length_bytes, "big")
                recovery_bytes = (
                    None
                    if recovery_length == 0
                    else await async_read_exact(self.stream, recovery_length)
                )
                recovery_str = (
                    None if recovery_bytes is None else recovery_bytes.decode("utf-8")
                )
                self.remaining -= 1
            except BaseException:
                self.state = _SubscriptionInfoFromStreamState.CLOSED
                raise

            return TopicAndRecovery(topic, recovery_str)

        if self.state == _SubscriptionInfoFromStreamState.GLOBS_NOT_PEEKED:
            try:
                while self.remaining > 0:
                    topic_length_bytes = await async_read_exact(self.stream, 2)
                    topic_length = int.from_bytes(topic_length_bytes, "big")
                    await async_read_exact(self.stream, topic_length)
                    recovery_length_bytes = await async_read_exact(self.stream, 2)
                    recovery_length = int.from_bytes(recovery_length_bytes, "big")
                    if recovery_length > 0:
                        await async_read_exact(self.stream, recovery_length)
                    self.remaining -= 1

                num_globs_bytes = await async_read_exact(self.stream, 4)
                num_globs = int.from_bytes(num_globs_bytes, "big")
                self.remaining = num_globs
                self.state = _SubscriptionInfoFromStreamState.GLOBS
            except BaseException:
                self.state = _SubscriptionInfoFromStreamState.CLOSED
                raise

        if self.state == _SubscriptionInfoFromStreamState.GLOBS:
            if self.remaining <= 0:
                raise StopAsyncIteration

            try:
                glob_length_bytes = await async_read_exact(self.stream, 2)
                glob_length = int.from_bytes(glob_length_bytes, "big")
                glob_bytes = await async_read_exact(self.stream, glob_length)
                glob = glob_bytes.decode("utf-8")
                recovery_length_bytes = await async_read_exact(self.stream, 2)
                recovery_length = int.from_bytes(recovery_length_bytes, "big")
                recovery_bytes = (
                    None
                    if recovery_length == 0
                    else await async_read_exact(self.stream, recovery_length)
                )
                recovery_str = (
                    None if recovery_bytes is None else recovery_bytes.decode("utf-8")
                )
                self.remaining -= 1
            except BaseException:
                self.state = _SubscriptionInfoFromStreamState.CLOSED
                raise

            return GlobAndRecovery(glob, recovery_str)

        raise AssertionError("not topics or globs")

    def __aiter__(self) -> "_SubscriptionInfoFromAsyncStream":
        return self

    def topics(self) -> AsyncIterator[TopicAndRecovery]:
        assert self.state == _SubscriptionInfoFromStreamState.TOPICS, "not topics"
        return _SubscriptionInfoFromStreamTopicsIter(self)

    def globs(self) -> AsyncIterator[GlobAndRecovery]:
        if self.state == _SubscriptionInfoFromStreamState.TOPICS:
            self.state = _SubscriptionInfoFromStreamState.GLOBS_NOT_PEEKED

        if (
            self.state != _SubscriptionInfoFromStreamState.GLOBS_NOT_PEEKED
            and self.state != _SubscriptionInfoFromStreamState.GLOBS
        ):
            raise AssertionError("not globs")

        return _SubscriptionInfoFromStreamGlobsIter(self)


class _CopyPassthroughStream:
    def __init__(self, source: AsyncReadableBytesIO, dest: SyncIOBaseLikeIO):
        self.source = source
        self.dest = dest

    async def read(self, n: int, /) -> bytes:
        data = await self.source.read(n)
        self.dest.write(data)
        return data


if TYPE_CHECKING:
    _a: Type[SetSubscriptionsInfo] = _SubscriptionInfoFromSyncStream
    _b: Type[SetSubscriptionsInfo] = _SubscriptionInfoFromAsyncStream
    _c: Type[AsyncIterator[Union[GlobAndRecovery, TopicAndRecovery]]] = (
        _SubscriptionInfoFromSyncStream
    )
    _d: Type[AsyncIterator[TopicAndRecovery]] = _SubscriptionInfoFromStreamTopicsIter
    _e: Type[AsyncIterator[GlobAndRecovery]] = _SubscriptionInfoFromStreamGlobsIter
    _f: Type[AsyncReadableBytesIO] = _CopyPassthroughStream


@router.post(
    "/v1/set_subscriptions",
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
    """Sets the subscriptions associated with the URL to a specific value. This
    is an idempotent variant of the subscribe/unsubscribe endpoints. Most of the
    time, it is easier, more efficient, and more resilient to use this endpoint
    instead of mutating the subscriptions in-place. The exceptions are:

    - in a websocket, since each websocket is self-contained, the subscribe/unsubscribe
      options are sufficient

    - if there are actually a large number of partial changes (especially with a large
      number of subscriptions) it may be more efficient to use the subscribe/unsubscribe
      endpoints

    - if there are an extremely large number of subscriptions, it may be preferable
      to use this to clear out the subscriptions, then add them via separate
      subscribe calls to allow for resumable processing

    NOTE: this is not atomic but there are some guarrantees; see the lonelypsp
    documentation for stateless `SET_SUBSCRIPTION`

    ### request body
    - 2 bytes (N): length of the subscriber url to set, big-endian, unsigned
    - N bytes: the url to set, utf-8 encoded
    - 1 byte (reserved for etag format): 0
    - 64 bytes: the strong etag, will be rechecked
    - 4 bytes (E): the number of exact topics to set, big-endian, unsigned
    - REPEAT E TIMES: (in ascending lexicographic order)
      - 2 bytes (L): length of the topic, big-endian, unsigned
      - L bytes: the topic
    - 4 bytes (G): the number of glob patterns to set, big-endian, unsigned
    - REPEAT G TIMES: (in ascending lexicographic order)
      - 2 bytes (L): length of the glob pattern, big-endian, unsigned
      - L bytes: the glob pattern, utf-8 encoded

    ### response body
    empty
    """
    config = get_config_from_request(request)

    try:
        with SpooledTemporaryFile(
            max_size=config.message_body_spool_size, mode="w+b"
        ) as subs_info_buffer:
            stream = request.stream()

            try:
                body = AsyncIterableAIO(stream.__aiter__())

                url_length_bytes = await async_read_exact(body, 2)
                url_length = int.from_bytes(url_length_bytes, "big")
                url_bytes = await async_read_exact(body, url_length)
                url = url_bytes.decode("utf-8")

                if (await async_read_exact(body, 1))[0] != 0:
                    return Response(status_code=400)
                etag_format: Literal[0] = 0

                etag = await async_read_exact(body, 64)

                auth_at = time.time()

                # note how we allow for checking some or all of the authorization
                # header before reading an arbitrary amount of data from the body
                # (at most we've read 2 + 64kibibytes + 1 + 64 bytes so far)
                cp_stream = _CopyPassthroughStream(body, subs_info_buffer)
                async with _SubscriptionInfoFromAsyncStream(
                    cp_stream
                ) as subscription_info:
                    auth_result = await config.is_set_subscriptions_allowed(
                        url=url,
                        strong_etag=StrongEtag(format=0, etag=etag),
                        subscriptions=subscription_info,
                        now=auth_at,
                        authorization=authorization,
                    )

                if auth_result == "unauthorized":
                    return Response(status_code=401)
                elif auth_result == "forbidden":
                    return Response(status_code=403)
                elif auth_result == "unavailable":
                    return Response(status_code=503)
                elif auth_result != "ok":
                    return Response(status_code=500)

                while True:
                    chunk = await cp_stream.read(io.DEFAULT_BUFFER_SIZE)
                    if not chunk:
                        break
            finally:
                await stream.aclose()

            # check etag was actually correct
            subs_info_buffer.seek(0, os.SEEK_SET)
            topics_gen = create_strong_etag_generator(url, recheck_sort=True)
            with _SubscriptionInfoFromSyncStream(subs_info_buffer) as subscription_info:
                async for topic in subscription_info.topics():
                    topics_gen.add_topic(topic)

                glob_gen = topics_gen.finish_topics()
                del topics_gen
                async for glob in subscription_info.globs():
                    glob_gen.add_glob(glob)

                real_etag = glob_gen.finish()

            if real_etag.format != etag_format or real_etag.etag != etag:
                return Response(status_code=400)

            # actually set the subscriptions
            subs_info_buffer.seek(0, os.SEEK_SET)
            with _SubscriptionInfoFromSyncStream(subs_info_buffer) as subscription_info:
                set_result = await config.set_subscriptions(
                    url=url, strong_etag=real_etag, subscriptions=subscription_info
                )

            if set_result == "unavailable":
                return Response(status_code=503)

            assert set_result == "success"
            return Response(status_code=200)

    except ValueError:
        return Response(status_code=400)
