import asyncio
import base64
import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import aiohttp
from lonelypsp.util.cancel_and_check import cancel_and_check

from lonelypss.config.config import Config, MutableMissedInfo


async def sweep_missed_once(config: Config) -> None:
    """
    Sweeps the unsent missed messages within the database one time, without
    looping. Generally, `sweep_missed_forever` or `sweep_missed` (async context manager)
    to have the looping handled as well

    This is concurrency-safe, i.e., there may be multiple concurrently running asyncio
    coroutines or this may be running in multiple processes connected to the same db
    without unexpected behavior (e.g., duplicate MISSED messages or not properly
    incrementing attempts) given reasonable assumptions (locks are not stolen due to
    timeouts)
    """

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(
            total=config.outgoing_http_timeout_total,
            connect=config.outgoing_http_timeout_connect,
            sock_read=config.outgoing_http_timeout_sock_read,
            sock_connect=config.outgoing_http_timeout_sock_connect,
        )
    ) as session:
        async for msg in config.get_overdue_missed_with_lock(now=time.time()):
            if msg.info.subscriber_info.recovery is None:
                if (res := await msg.release(new_info=None)) != "ok":
                    return
                continue

            try:
                authorization = await config.authorize_missed(
                    recovery=msg.info.subscriber_info.recovery,
                    topic=msg.info.topic,
                    now=time.time(),
                )
                try:
                    async with session.post(
                        msg.info.subscriber_info.recovery,
                        headers={
                            **(
                                {"Authorization": authorization}
                                if authorization is not None
                                else {}
                            ),
                            "X-Topic": base64.b64encode(msg.info.topic).decode("ascii"),
                        },
                    ) as resp:
                        if resp.status >= 400 and resp.status < 500:
                            content_type = resp.headers.get("Content-Type")
                            if content_type is not None and content_type.startswith(
                                "application/json"
                            ):
                                content = await resp.json()
                                if (
                                    isinstance(content, dict)
                                    and content.get("unsubscribe") is True
                                ):
                                    if (
                                        res := await msg.release(new_info=None)
                                    ) != "ok":
                                        return
                                    continue
                        resp.raise_for_status()
                except aiohttp.ClientError:
                    next_attempt = msg.info.attempts + 1
                    next_retry_at = await config.get_delay_for_next_missed_retry(
                        receive_url=msg.info.subscriber_info.url,
                        missed_url=msg.info.subscriber_info.recovery,
                        topic=msg.info.topic,
                        attempts=next_attempt,
                    )
                    if next_retry_at is None:
                        if (res := await msg.release(new_info=None)) != "ok":
                            return
                        continue
                    res = await msg.release(
                        new_info=MutableMissedInfo(
                            attempts=next_attempt,
                            next_retry_at=next_retry_at,
                        )
                    )
                    if res != "ok":
                        return
                    continue

                await msg.release(new_info=None)
            except BaseException:
                await msg.release(
                    new_info=MutableMissedInfo(
                        attempts=msg.info.attempts,
                        next_retry_at=msg.info.next_retry_at,
                    )
                )
                raise


async def sweep_missed_forever(config: Config) -> None:
    """
    Sweeps the unsent missed messages within the database forever, looping
    until asyncio.CancelledError is raised
    """

    while True:
        await sweep_missed_once(config)
        await asyncio.sleep(config.sweep_missed_interval)


@asynccontextmanager
async def sweep_missed(config: Config) -> AsyncGenerator[None, None]:
    """
    Context manager for sweeping missed messages within the database until
    the context is exited without interrupting mid-sweep
    """

    exit_event = asyncio.Event()

    async def _inner() -> None:
        while not exit_event.is_set():
            await sweep_missed_once(config)

            want_exit = asyncio.create_task(exit_event.wait())
            want_loop = asyncio.create_task(asyncio.sleep(config.sweep_missed_interval))
            await asyncio.wait(
                [want_exit, want_loop], return_when=asyncio.FIRST_COMPLETED
            )
            await cancel_and_check(want_exit)
            await cancel_and_check(want_loop)

    inner_task = asyncio.create_task(_inner())
    try:
        yield
    finally:
        exit_event.set()
        await inner_task
