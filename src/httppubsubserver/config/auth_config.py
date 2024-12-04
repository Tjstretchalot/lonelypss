from typing import TYPE_CHECKING, Literal, Optional, Protocol, Type


class IncomingAuthConfig(Protocol):
    async def setup_incoming_auth(self) -> None:
        """Prepares this authorization instance for use. If the incoming auth config
        is not re-entrant (i.e., it cannot be used by two clients simultaneously), it
        must detect this and error out.
        """

    async def teardown_incoming_auth(self) -> None:
        """Cleans up this authorization instance after use. This is called when a
        client is done using the auth config, and should release any resources it
        acquired during `setup_incoming_auth`.
        """

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

        Note that in websockets where compression is enabled, the sha512 is
        of the compressed content, as we cannot safely decompress the data (and
        thus compute the decompressed sha512) unless we know it is safe, at which
        point a second check would be redundant.

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
        """Determines if the given message can be received from the given topic. As
        we support very large messages, for authorization only the SHA-512 of
        the message should be used, which will be fully verified. Note that broadcasters
        only need to receive when using websocket connections, and the broadcaster is
        receiving from _other broadcasters_.

        Args:
            url (str): the url the broadcaster used to reach us
            topic (bytes): the topic the message claims to be on
            message_sha512 (bytes): the sha512 of the message being received
            now (float): the current time in seconds since the epoch, as if from `time.time()`
            authorization (str, None): the authorization header they provided

        Returns:
            `ok`: if the message is allowed
            `unauthorized`: if the authorization header is required but not provided
            `forbidden`: if the authorization header is provided but invalid
            `unavailable`: if a service is required to check this isn't available.
              the message will be dropped.
        """


class OutgoingAuthConfig(Protocol):
    async def setup_outgoing_auth(self) -> None:
        """Prepares this authorization instance for use. If the outgoing auth config
        is not re-entrant (i.e., it cannot be used by two clients simultaneously), it
        must detect this and error out.
        """

    async def teardown_outgoing_auth(self) -> None:
        """Cleans up this authorization instance after use. This is called when a
        client is done using the auth config, and should release any resources it
        acquired during `setup_outgoing_auth`.
        """

    async def setup_authorization(
        self, /, *, url: str, topic: bytes, message_sha512: bytes, now: float
    ) -> Optional[str]:
        """Setups the authorization header that the broadcaster should use when
        contacting the given url about a message with the given sha512 on the
        given topic at approximately the given time.

        When using websockets, the url is of the form "websocket:<nonce>:<ctr>",
        where more details are described in the websocket endpoints
        documentation. What's important is that the recipient can either verify
        the url is what they expect or the url is structured such that it is
        unique if _either_ party is acting correctly, meaning replay attacks are
        limited to a single target (i.e., we structurally disallow replaying a
        message sent from Bob to Alice via pretending to be Bob to Charlie, as
        Charlie will be able to tell that message was intended for not-Charlie).

        Note that the reverse is not promised (i.e., broadcasters do not know which
        broadcaster the subscriber meant to contact), but assuming the number of
        broadcasters is much smaller than the number of subscribers, this is less
        of an issue to coordinate.

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

    async def setup_incoming_auth(self) -> None:
        await self.incoming.setup_incoming_auth()

    async def teardown_incoming_auth(self) -> None:
        await self.incoming.teardown_incoming_auth()

    async def setup_outgoing_auth(self) -> None:
        await self.outgoing.setup_outgoing_auth()

    async def teardown_outgoing_auth(self) -> None:
        await self.outgoing.teardown_outgoing_auth()

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
        return await self.incoming.is_receive_allowed(
            url=url,
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
