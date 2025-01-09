import hmac
from typing import TYPE_CHECKING, Literal, Optional, Type

from lonelypsp.stateful.messages.configure import S2B_Configure
from lonelypsp.stateless.make_strong_etag import StrongEtag

from lonelypss.config.set_subscriptions_info import SetSubscriptionsInfo

if TYPE_CHECKING:
    from lonelypss.config.auth_config import (
        ToBroadcasterAuthConfig,
        ToSubscriberAuthConfig,
    )


class ToBroadcasterTokenAuth:
    """Allows and produces the authorization header to the broadcaster in
    the consistent form `f"Bearer {token}"`

    In order for this to be secure, the headers must be encrypted, typically via
    HTTPS.
    """

    def __init__(self, /, *, token: str) -> None:
        self.expecting = f"Bearer {token}"
        """The exact authorization header the broadcaster receives"""

    async def setup_to_broadcaster_auth(self) -> None: ...
    async def teardown_to_broadcaster_auth(self) -> None: ...

    def _check_header(
        self, authorization: Optional[str]
    ) -> Literal["ok", "unauthorized", "forbidden"]:
        if authorization is None:
            return "unauthorized"
        if not hmac.compare_digest(authorization, self.expecting):
            return "forbidden"
        return "ok"

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
        return self._check_header(authorization)

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
        return self._check_header(authorization)

    async def is_notify_allowed(
        self,
        /,
        *,
        topic: bytes,
        message_sha512: bytes,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return self._check_header(authorization)

    async def is_websocket_configure_allowed(
        self, /, *, message: S2B_Configure, now: float
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return self._check_header(message.authorization)

    async def is_check_subscriptions_allowed(
        self, /, *, url: str, now: float, authorization: Optional[str]
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return self._check_header(authorization)

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
        return self._check_header(authorization)


class ToSubscriberTokenAuth:
    """Allows and produces the authorization header to the subscriber in
    the consistent form `f"Bearer {token}"`

    In order for this to be secure, the clients must verify the header matches
    what they expect and the headers must be encrypted, typically via HTTPS.
    """

    def __init__(self, /, *, token: str) -> None:
        self.expecting = f"Bearer {token}"
        """The authorization header that the subscriber receives"""

    async def setup_to_subscriber_auth(self) -> None: ...
    async def teardown_to_subscriber_auth(self) -> None: ...

    def _check_header(
        self, authorization: Optional[str]
    ) -> Literal["ok", "unauthorized", "forbidden"]:
        if authorization is None:
            return "unauthorized"
        if not hmac.compare_digest(authorization, self.expecting):
            return "forbidden"
        return "ok"

    async def authorize_receive(
        self, /, *, url: str, topic: bytes, message_sha512: bytes, now: float
    ) -> Optional[str]:
        return self.expecting

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
        return self._check_header(authorization)

    async def is_missed_allowed(
        self,
        /,
        *,
        recovery: str,
        topic: bytes,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return self._check_header(authorization)

    async def authorize_missed(
        self, /, *, recovery: str, topic: bytes, now: float
    ) -> Optional[str]:
        return self.expecting

    async def authorize_websocket_confirm_configure(
        self, /, *, broadcaster_nonce: bytes, now: float
    ) -> Optional[str]:
        return self.expecting


if TYPE_CHECKING:
    _: Type[ToBroadcasterAuthConfig] = ToBroadcasterTokenAuth
    __: Type[ToSubscriberAuthConfig] = ToSubscriberTokenAuth
