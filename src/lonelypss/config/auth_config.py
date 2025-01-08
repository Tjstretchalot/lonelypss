from typing import (
    TYPE_CHECKING,
    Literal,
    Optional,
    Protocol,
    Type,
)

from lonelypsp.stateless.make_strong_etag import StrongEtag

from lonelypss.config.set_subscriptions_info import SetSubscriptionsInfo


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
        self,
        /,
        *,
        url: str,
        recovery: Optional[str],
        exact: bytes,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        """Determines if the given url can (un)subscribe to the given exact match.

        Args:
            url (str): the url that will receive notifications
            recovery (str, None): the url that will receive MISSED messages for this
                subscription, if any
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
        self,
        /,
        *,
        url: str,
        recovery: Optional[str],
        glob: str,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        """Determines if the given url can (un)subscribe to the given glob-style match

        Args:
            url (str): the url that will receive notifications
            recovery (str, None): the url that will receive MISSED messages for this
                subscription, if any
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

    async def is_missed_allowed(
        self,
        /,
        *,
        recovery: str,
        topic: bytes,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        """Determines if the indication that the broadcaster missed a message from
        another broadcaster is allowed. This allows the broadcaster to forward this
        information to subscribers to that topic which can trigger recovery.

        Args:
            recovery (str): the url the missed message was sent to
            topic (bytes): the topic the message was on
            now (float): the current time in seconds since the epoch, as if from `time.time()`
            authorization (str, None): the authorization header they provided

        Returns:
            `ok`: if the message is allowed
            `unauthorized`: if the authorization header is required but not provided
            `forbidden`: if the authorization header is provided but invalid
            `unavailable`: if a service is required to check this isn't available
        """

    async def is_check_subscriptions_allowed(
        self, /, *, url: str, now: float, authorization: Optional[str]
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        """Determines if the given authorization is sufficient to retrieve the strong
        etag that identifies the url and its current subscriptions (both exact and glob).

        Args:
            url (str): the url whose subscriptions are being checked
            now (float): the current time in seconds since the epoch, as if from `time.time()`
            authorization (str, None): the authorization header they provided

        Returns:
            `ok`: if the request is allowed
            `unauthorized`: if the authorization header is required but not provided
            `forbidden`: if the authorization header is provided but invalid
            `unavailable`: if a service is required to check this isn't available
        """

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
        """Determines if the given authorization is sufficient to set the
        subscriptions for the given url; this removes any existing subscriptions
        and replaces them with those provided.

        Ideally the authorization would not need to actually iterate the topics
        and globs, but in practice that is too great a restriction, so instead
        the iterable is async, single-use, and can detect if it was unused, allowing
        the implementation the maximum flexibility to make performance optimizations
        while still allowing the obvious desired case of some users can only subscribe
        to certain prefixes

        WARN: when this function returns, `subscriptions` will no longer be usable

        Args:
            url (str): the url whose subscriptions are being set
            strong_etag (StrongEtag): the strong etag that will be verified before
                actually setting subscriptions, but may not have been verified yet.
            subscriptions (SetSubscriptionsInfo): the subscriptions to set
            now (float): the current time in seconds since the epoch, as if from `time.time()`
            authorization (str, None): the authorization header they provided

        Returns:
            `ok`: if the request is allowed
            `unauthorized`: if the authorization header is required but not provided
            `forbidden`: if the authorization header is provided but invalid
            `unavailable`: if a service is required to check this isn't available
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

    async def setup_missed(
        self, /, *, recovery: str, topic: bytes, now: float
    ) -> Optional[str]:
        """Sets up the authorization header that the broadcaster should use when
        contacting the given url about a missed message on the given topic at
        approximately the given time. The contents of the message are not sent
        nor necessarily available; this is just to inform the subscriber that
        they may have missed a message. They may have their own log that they
        can recovery the message with if necessary.

        When sending this over a websocket, the recovery url is of the form
        `websocket:<nonce>:<ctr>`, where more details can be found in the
        stateful documentation in lonelypsp

        Args:
            recovery (str): the url that will receive the missed message
            topic (bytes): the topic that the message was on
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
        self,
        /,
        *,
        url: str,
        recovery: Optional[str],
        exact: bytes,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return await self.incoming.is_subscribe_exact_allowed(
            url=url,
            recovery=recovery,
            exact=exact,
            now=now,
            authorization=authorization,
        )

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
        return await self.incoming.is_subscribe_glob_allowed(
            url=url, recovery=recovery, glob=glob, now=now, authorization=authorization
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

    async def is_missed_allowed(
        self,
        /,
        *,
        recovery: str,
        topic: bytes,
        now: float,
        authorization: Optional[str],
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return await self.incoming.is_missed_allowed(
            recovery=recovery, topic=topic, now=now, authorization=authorization
        )

    async def is_check_subscriptions_allowed(
        self, /, *, url: str, now: float, authorization: Optional[str]
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return await self.incoming.is_check_subscriptions_allowed(
            url=url, now=now, authorization=authorization
        )

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
        return await self.incoming.is_set_subscriptions_allowed(
            url=url,
            strong_etag=strong_etag,
            subscriptions=subscriptions,
            now=now,
            authorization=authorization,
        )

    async def setup_authorization(
        self, /, *, url: str, topic: bytes, message_sha512: bytes, now: float
    ) -> Optional[str]:
        return await self.outgoing.setup_authorization(
            url=url, topic=topic, message_sha512=message_sha512, now=now
        )

    async def setup_missed(
        self, /, *, recovery: str, topic: bytes, now: float
    ) -> Optional[str]:
        return await self.outgoing.setup_missed(recovery=recovery, topic=topic, now=now)


if TYPE_CHECKING:
    _: Type[AuthConfig] = AuthConfigFromParts
