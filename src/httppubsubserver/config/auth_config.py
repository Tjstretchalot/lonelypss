from typing import TYPE_CHECKING, Literal, Optional, Protocol, Type


class IncomingAuthConfig(Protocol):
    async def is_subscribe_exact_allowed(
        self, /, *, url: str, exact: bytes, now: float, authorization: Optional[str]
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        """Determines if the given url can (un)subscribe to the given exact match.

        Args:
            url (str): the url that will receive notifications
            exact (bytes): the exact topic they want to receive messages from
            now (float): the current time in seconds since the epoch, as if from `time.time()`
            authorization (str, None): the authorization header they provided

        Returns:
            `ok`: if the subscription is allowed
            `unauthorized`: if the authorization header is required but not provided
            `forbidden`: if the authorization header is provided but invalid
            `unavailable`: if a service is required to check this isn't available
        """

    async def is_subscribe_glob_allowed(
        self, /, *, url: str, glob: str, now: float, authorization: Optional[str]
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        """Determines if the given url can (un)subscribe to the given glob-style match

        Args:
            url (str): the url that will receive notifications
            glob (str): a glob for the topics that they want to receive notifications from
            now (float): the current time in seconds since the epoch, as if from `time.time()`
            authorization (str, None): the authorization header they provided

        Returns:
            `ok`: if the subscription is allowed
            `unauthorized`: if the authorization header is required but not provided
            `forbidden`: if the authorization header is provided but invalid
            `unavailable`: if a service is required to check this isn't available
        """

    async def is_notify_allowed(
        self,
        /,
        *,
        topic: bytes,
        message_sha512: bytes,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        """Determines if the given message can be published to the given topic. As
        we support very large messages, for authorization only the SHA-512 of
        the message should be used, which will be fully verified before any
        notifications go out.

        Args:
            topic (bytes): the topic that the message is being sent to
            message_sha512 (bytes): the sha512 of the message being sent
            now (float): the current time in seconds since the epoch, as if from `time.time()`
            authorization (str, None): the authorization header they provided

        Returns:
            `ok`: if the message is allowed
            `unauthorized`: if the authorization header is required but not provided
            `forbidden`: if the authorization header is provided but invalid
            `unavailable`: if a service is required to check this isn't available
        """


class OutgoingAuthConfig(Protocol):
    async def setup_authorization(
        self, /, *, url: str, topic: bytes, message_sha512: bytes, now: float
    ) -> Optional[str]:
        """Setups the authorization header that the broadcaster should use when
        contacting the given url about a message with the given sha512 on the
        given topic at approximately the given time.

        Args:
            url (str): the url that will receive the notification
            topic (bytes): the topic that the message is being sent to
            message_sha512 (bytes): the sha512 of the message being sent
            now (float): the current time in seconds since the epoch, as if from `time.time()`

        Returns:
            str, None: the authorization header to use, if any
        """


class AuthConfig(IncomingAuthConfig, OutgoingAuthConfig, Protocol): ...


class AuthConfigFromParts:
    """Convenience class to combine an incoming and outgoing auth config into an
    auth config
    """

    def __init__(self, incoming: IncomingAuthConfig, outgoing: OutgoingAuthConfig):
        self.incoming = incoming
        self.outgoing = outgoing

    async def is_subscribe_exact_allowed(
        self, /, *, url: str, exact: bytes, now: float, authorization: Optional[str]
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return await self.incoming.is_subscribe_exact_allowed(
            url=url, exact=exact, now=now, authorization=authorization
        )

    async def is_subscribe_glob_allowed(
        self, /, *, url: str, glob: str, now: float, authorization: Optional[str]
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return await self.incoming.is_subscribe_glob_allowed(
            url=url, glob=glob, now=now, authorization=authorization
        )

    async def is_notify_allowed(
        self,
        /,
        *,
        topic: bytes,
        message_sha512: bytes,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return await self.incoming.is_notify_allowed(
            topic=topic,
            message_sha512=message_sha512,
            now=now,
            authorization=authorization,
        )

    async def setup_authorization(
        self, /, *, url: str, topic: bytes, message_sha512: bytes, now: float
    ) -> Optional[str]:
        return await self.outgoing.setup_authorization(
            url=url, topic=topic, message_sha512=message_sha512, now=now
        )


if TYPE_CHECKING:
    _: Type[AuthConfig] = AuthConfigFromParts
