from typing import (
    TYPE_CHECKING,
    Literal,
    Optional,
    Protocol,
    Type,
)

from lonelypsp.stateful.messages.configure import S2B_Configure
from lonelypsp.stateless.make_strong_etag import StrongEtag

from lonelypss.config.set_subscriptions_info import SetSubscriptionsInfo


class ToBroadcasterAuthConfig(Protocol):
    """Handles verifying requests from a subscriber to this broadcaster or
    producing the authorization header when contacting other broadcasters
    """

    async def setup_to_broadcaster_auth(self) -> None:
        """Prepares this authorization instance for use. If the
        to broadcaster auth config is not re-entrant (i.e., it cannot
        be used by two clients simultaneously), it must detect this and error
        out.
        """

    async def teardown_to_broadcaster_auth(self) -> None:
        """Cleans up this authorization instance after use. This is called when a
        client is done using the auth config, and should release any resources
        it acquired during `setup_to_broadcaster_auth`.
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
        """Checks the authorization header posted to the broadcaster to
        (un)subscribe a specific url to a specific topic

        Args:
            url (str): the url that will receive notifications
            recovery (str, None): the url that will receive MISSED messages for this
                subscription, if any. Always None for unsubscribes
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
        """Checks the authorization header posted to the broadcaster to
        (un)subscribe a specific url to a specific glob of topics

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
        """Checks the authorization header posted to the broadcaster to fanout a
        notification on a specific topic.

        As we support very large messages, for authorization only the SHA-512 of
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

    async def is_websocket_configure_allowed(
        self, /, *, message: S2B_Configure, now: float
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        """Checks the authorization header posted to the broadcaster to configure
        a websocket connection with a subscriber.

        Args:
            message (S2B_Configure): the configure message they sent
            now (float): the current time in seconds since the epoch, as if from `time.time()`

        Returns:
            `ok`: if the configure message is allowed
            `unauthorized`: if the authorization header is required but not provided
            `forbidden`: if the authorization header is provided but invalid
            `unavailable`: if a service is required to check this isn't available
        """

    async def is_check_subscriptions_allowed(
        self, /, *, url: str, now: float, authorization: Optional[str]
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        """Checks the authorization header posted to the broadcaster to check
        the subscriptions for a specific url.

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
        """Checks the authorization header posted to the broadcaster to replace
        the subscriptions for a specific url.

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


class ToSubscriberAuthConfig(Protocol):
    """Handles verifying requests from a broadcaster to this subscriber or
    producing the authorization header when contacting subscribers
    """

    async def setup_to_subscriber_auth(self) -> None:
        """Prepares this authorization instance for use. If the to subscriber auth
        config is not re-entrant (i.e., it cannot be used by two clients
        simultaneously), it must detect this and error out.
        """

    async def teardown_to_subscriber_auth(self) -> None:
        """Cleans up this authorization instance after use. This is called when a
        client is done using the auth config, and should release any resources it
        acquired during `setup_to_subscriber_auth`.
        """

    async def authorize_receive(
        self, /, *, url: str, topic: bytes, message_sha512: bytes, now: float
    ) -> Optional[str]:
        """Produces the authorization header to send to the subscriber a message
        with the given sha512 on the given topic at approximately the given
        time.

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
        """Checks the authorization header posted to a subscriber to receive a message
        from a broadcaster on a topic.

        As we support very large messages, for authorization only the SHA-512 of
        the message should be used, which will be fully verified.

        Broadcasters act as subscribers for receiving messages when a subscriber
        is connected via websocket, so it can forward messages sent to other
        broadcasters.

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
        """Checks the authorization header posted to a subscriber via the given
        recovery url to indicate it may have missed a message on the given topic

        Broadcasters act as subscribers for receiving messages when a subscriber
        is connected via websocket, so it can forward messages sent to other
        broadcasters.

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

    async def authorize_missed(
        self, /, *, recovery: str, topic: bytes, now: float
    ) -> Optional[str]:
        """Produces the authorization header to send to the subscriber to indicate
        that it may have missed a message on the given topic. The message is being sent at
        approximately the given time, which is unrelated to when the message they
        missed was sent.

        The contents of the message are not sent nor necessarily available; this
        is just to inform the subscriber that they may have missed a message.
        They may have their own log that they can recovery the message with if
        necessary.

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

    async def authorize_websocket_confirm_configure(
        self, /, *, broadcaster_nonce: bytes, now: float
    ) -> Optional[str]:
        """Produces the authorization header to send to the subscriber to confirm
        the websocket configure message they sent was accepted.

        Args:
            broadcaster_nonce (bytes): the nonce that the broadcaster will send in the
                confirm configure message
            now (float): the current time in seconds since the epoch, as if from `time.time()`

        Returns:
            str, None: the authorization header to use, if any
        """


class AuthConfig(ToBroadcasterAuthConfig, ToSubscriberAuthConfig, Protocol): ...


class AuthConfigFromParts:
    """Convenience class to combine an incoming and outgoing auth config into an
    auth config
    """

    def __init__(
        self,
        to_broadcaster: ToBroadcasterAuthConfig,
        to_subscriber: ToSubscriberAuthConfig,
    ):
        self.to_broadcaster = to_broadcaster
        self.to_subscriber = to_subscriber

    async def setup_to_broadcaster_auth(self) -> None:
        await self.to_broadcaster.setup_to_broadcaster_auth()

    async def teardown_to_broadcaster_auth(self) -> None:
        await self.to_broadcaster.teardown_to_broadcaster_auth()

    async def setup_to_subscriber_auth(self) -> None:
        await self.to_subscriber.setup_to_subscriber_auth()

    async def teardown_to_subscriber_auth(self) -> None:
        await self.to_subscriber.teardown_to_subscriber_auth()

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
        return await self.to_broadcaster.is_subscribe_exact_allowed(
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
        return await self.to_broadcaster.is_subscribe_glob_allowed(
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
        return await self.to_broadcaster.is_notify_allowed(
            topic=topic,
            message_sha512=message_sha512,
            now=now,
            authorization=authorization,
        )

    async def is_websocket_configure_allowed(
        self, /, *, message: S2B_Configure, now: float
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return await self.to_broadcaster.is_websocket_configure_allowed(
            message=message, now=now
        )

    async def is_check_subscriptions_allowed(
        self, /, *, url: str, now: float, authorization: Optional[str]
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return await self.to_broadcaster.is_check_subscriptions_allowed(
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
        return await self.to_broadcaster.is_set_subscriptions_allowed(
            url=url,
            strong_etag=strong_etag,
            subscriptions=subscriptions,
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
        return await self.to_subscriber.is_receive_allowed(
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
        return await self.to_subscriber.is_missed_allowed(
            recovery=recovery, topic=topic, now=now, authorization=authorization
        )

    async def authorize_receive(
        self, /, *, url: str, topic: bytes, message_sha512: bytes, now: float
    ) -> Optional[str]:
        return await self.to_subscriber.authorize_receive(
            url=url, topic=topic, message_sha512=message_sha512, now=now
        )

    async def authorize_missed(
        self, /, *, recovery: str, topic: bytes, now: float
    ) -> Optional[str]:
        return await self.to_subscriber.authorize_missed(
            recovery=recovery, topic=topic, now=now
        )

    async def authorize_websocket_confirm_configure(
        self, /, *, broadcaster_nonce: bytes, now: float
    ) -> Optional[str]:
        return await self.to_subscriber.authorize_websocket_confirm_configure(
            broadcaster_nonce=broadcaster_nonce, now=now
        )


if TYPE_CHECKING:
    _: Type[AuthConfig] = AuthConfigFromParts
