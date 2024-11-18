import hmac
from typing import Literal, Optional, TYPE_CHECKING, Type

if TYPE_CHECKING:
    from httppubsubserver.config.auth_config import (
        IncomingAuthConfig,
        OutgoingAuthConfig,
    )


class IncomingTokenAuth:
    """Allows subscription management if the Authorization header is of the form
    `f"Bearer {token}"`

    In order for this to be secure, the headers must be encrypted, typically via
    HTTPS.
    """

    def __init__(self, token: str) -> None:
        self.expecting = f"Bearer {token}"
        """The exact authorization header we accept"""

    async def setup_incoming_auth(self) -> None: ...
    async def teardown_incoming_auth(self) -> None: ...

    def _check_header(
        self, authorization: Optional[str]
    ) -> Literal["ok", "unauthorized", "forbidden"]:
        if authorization is None:
            return "unauthorized"
        if not hmac.compare_digest(authorization, self.expecting):
            return "forbidden"
        return "ok"

    async def is_subscribe_exact_allowed(
        self, /, *, url: str, exact: bytes, now: float, authorization: Optional[str]
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return self._check_header(authorization)

    async def is_subscribe_glob_allowed(
        self, /, *, url: str, glob: str, now: float, authorization: Optional[str]
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


if TYPE_CHECKING:
    _: Type[IncomingAuthConfig] = IncomingTokenAuth
    __: Type[OutgoingAuthConfig] = OutgoingTokenAuth
