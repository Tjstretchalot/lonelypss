from typing import TYPE_CHECKING, Literal, Optional, Type

from lonelypsp.stateless.make_strong_etag import StrongEtag

from lonelypss.config.set_subscriptions_info import SetSubscriptionsInfo

if TYPE_CHECKING:
    from lonelypss.config.auth_config import (
        IncomingAuthConfig,
        OutgoingAuthConfig,
    )


class IncomingNoneAuth:
    """Allows all incoming requests

    In order for this to be secure it must only be possible for trusted clients
    to connect to the server (e.g., by setting up TLS mutual auth at the binding
    level)
    """

    async def setup_incoming_auth(self) -> None: ...
    async def teardown_incoming_auth(self) -> None: ...

    async def is_subscribe_exact_allowed(
        self,
        /,
        *,
        url: str,
        recovery: Optional[str],
        exact: bytes,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return "ok"

    async def is_subscribe_glob_allowed(
        self,
        /,
        *,
        url: str,
        recovery: Optional[str],
        glob: str,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return "ok"

    async def is_notify_allowed(
        self,
        /,
        *,
        topic: bytes,
        message_sha512: bytes,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return "ok"

    async def is_receive_allowed(
        self,
        /,
        *,
        url: str,
        topic: bytes,
        message_sha512: bytes,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return "ok"

    async def is_missed_allowed(
        self,
        /,
        *,
        recovery: str,
        topic: bytes,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return "ok"

    async def is_check_subscriptions_allowed(
        self, /, *, url: str, now: float, authorization: Optional[str]
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return "ok"

    async def is_set_subscriptions_allowed(
        self,
        /,
        *,
        url: str,
        strong_etag: StrongEtag,
        subscriptions: SetSubscriptionsInfo,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return "ok"


class OutgoingNoneAuth:
    """Doesn't set any authorization header. In order for this to be secure, the
    subscribers must only be able to receive messages from trusted clients.
    """

    async def setup_outgoing_auth(self) -> None: ...
    async def teardown_outgoing_auth(self) -> None: ...

    async def setup_authorization(
        self, /, *, url: str, topic: bytes, message_sha512: bytes, now: float
    ) -> Optional[str]:
        return None

    async def setup_missed(
        self, /, *, recovery: str, topic: bytes, now: float
    ) -> Optional[str]:
        return None


if TYPE_CHECKING:
    _: Type[IncomingAuthConfig] = IncomingNoneAuth
    __: Type[OutgoingAuthConfig] = OutgoingNoneAuth
