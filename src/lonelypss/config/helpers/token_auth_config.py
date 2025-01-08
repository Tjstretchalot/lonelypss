import hmac
from typing import TYPE_CHECKING, Literal, Optional, Type

from lonelypsp.stateless.make_strong_etag import StrongEtag

from lonelypss.config.set_subscriptions_info import SetSubscriptionsInfo

if TYPE_CHECKING:
    from lonelypss.config.auth_config import (
        IncomingAuthConfig,
        OutgoingAuthConfig,
    )


class IncomingTokenAuth:
    """Allows subscription management if the Authorization header is of the form
    `f"Bearer {token}"`

    In order for this to be secure, the headers must be encrypted, typically via
    HTTPS.
    """

    def __init__(self, /, *, subscriber_token: str, broadcaster_token: str) -> None:
        self.subscriber_expecting = f"Bearer {subscriber_token}"
        """The exact authorization header we accept from subscribers"""

        self.broadcaster_expecting = f"Bearer {broadcaster_token}"
        """The exact authorization header we accept from broadcasters"""

    async def setup_incoming_auth(self) -> None: ...
    async def teardown_incoming_auth(self) -> None: ...

    def _check_header(
        self, expecting: str, authorization: Optional[str]
    ) -> Literal["ok", "unauthorized", "forbidden"]:
        if authorization is None:
            return "unauthorized"
        if not hmac.compare_digest(authorization, expecting):
            return "forbidden"
        return "ok"

    def _check_subscriber_header(
        self, authorization: Optional[str]
    ) -> Literal["ok", "unauthorized", "forbidden"]:
        return self._check_header(self.subscriber_expecting, authorization)

    def _check_broadcaster_header(
        self, authorization: Optional[str]
    ) -> Literal["ok", "unauthorized", "forbidden"]:
        return self._check_header(self.broadcaster_expecting, authorization)

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
        return self._check_subscriber_header(authorization)

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
        return self._check_subscriber_header(authorization)

    async def is_notify_allowed(
        self,
        /,
        *,
        topic: bytes,
        message_sha512: bytes,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return self._check_subscriber_header(authorization)

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
        return self._check_broadcaster_header(authorization)

    async def is_missed_allowed(
        self,
        /,
        *,
        recovery: str,
        topic: bytes,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return self._check_broadcaster_header(authorization)

    async def is_check_subscriptions_allowed(
        self, /, *, url: str, now: float, authorization: Optional[str]
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return self._check_subscriber_header(authorization)

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
        return self._check_subscriber_header(authorization)


class OutgoingTokenAuth:
    """Sets the authorization header to `f"Bearer {token}"`. In order for this to be
    secure, the clients must verify the header matches what they expect and the headers
    must be encrypted, typically via HTTPS.
    """

    def __init__(self, token: str) -> None:
        self.authorization = f"Bearer {token}"
        """The exact authorization header we set"""

    async def setup_outgoing_auth(self) -> None: ...
    async def teardown_outgoing_auth(self) -> None: ...

    async def setup_authorization(
        self, /, *, url: str, topic: bytes, message_sha512: bytes, now: float
    ) -> Optional[str]:
        return self.authorization

    async def setup_missed(
        self, /, *, recovery: str, topic: bytes, now: float
    ) -> Optional[str]:
        return self.authorization


if TYPE_CHECKING:
    _: Type[IncomingAuthConfig] = IncomingTokenAuth
    __: Type[OutgoingAuthConfig] = OutgoingTokenAuth
