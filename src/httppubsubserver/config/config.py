from typing import AsyncIterable, Literal, Optional, Protocol, Union


class AuthConfig(Protocol):
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


class DBConfig(Protocol):
    async def subscribe_exact(
        self, /, *, url: str, exact: bytes
    ) -> Literal["success", "conflict", "unavailable"]:
        """Subscribes the given URL to the given exact match.

        Args:
            url (str): the url that will receive notifications
            exact (bytes): the exact topic they want to receive messages from

        Returns:
            `success`: if the subscription was added
            `conflict`: if the subscription already exists
            `unavailable`: if a service is required to check this isn't available
        """

    async def unsubscribe_exact(
        self, /, *, url: str, exact: bytes
    ) -> Literal["success", "not_found", "unavailable"]:
        """Unsubscribes the given URL from the given exact match

        Args:
            url (str): the url that will receive notifications
            exact (bytes): the exact topic they want to receive messages from

        Returns:
            `success`: if the subscription was removed
            `not_found`: if the subscription didn't exist
            `unavailable`: if the database for subscriptions is unavailable
        """

    async def subscribe_glob(
        self, /, *, url: str, glob: str
    ) -> Literal["success", "conflict", "unavailable"]:
        """Subscribes the given URL to the given glob-style match

        Args:
            url (str): the url that will receive notifications
            glob (str): a glob for the topics that they want to receive notifications from

        Returns:
            `success`: if the subscription was added
            `conflict`: if the subscription already exists
            `unavailable`: if the database for subscriptions is unavailable
        """

    async def unsubscribe_glob(
        self, /, *, url: str, glob: str
    ) -> Literal["success", "not_found", "unavailable"]:
        """Unsubscribes the given URL from the given glob-style match

        Args:
            url (str): the url that will receive notifications
            glob (str): a glob for the topics that they want to receive notifications from

        Returns:
            `success`: if the subscription was removed
            `not_found`: if the subscription didn't exist
            `unavailable`: if the database for subscriptions is unavailable
        """

    def get_subscribers(
        self, /, *, topic: bytes
    ) -> AsyncIterable[Union[str, Literal[b"unavailable"]]]:
        """Streams back the subscriber urls that match the given topic. We will post messages
        to these urls as they are provided. This should return duplicates if multiple subscriptions
        match with the same url.

        Args:
            topic (bytes): the topic that we are looking for

        Yields:
            str: the urls that match the topic
            b'unavailable': if the database for subscriptions is unavailable
        """


class GenericConfig(Protocol):
    @property
    def message_body_spool_size(self) -> int:
        """If the message body exceeds this size we always switch to a temporary file."""

    @property
    def outgoing_http_timeout_total(self) -> Optional[float]:
        """The total timeout for outgoing http requests in seconds"""

    @property
    def outgoing_http_timeout_connect(self) -> Optional[float]:
        """The timeout for connecting to the server in seconds, which may include multiple
        socket attempts
        """

    @property
    def outgoing_http_timeout_sock_read(self) -> Optional[float]:
        """The timeout for reading from a socket to the server in seconds before the socket is
        considered dead
        """

    @property
    def outgoing_http_timeout_sock_connect(self) -> Optional[float]:
        """The timeout for a single socket connecting to the server before we give up in seconds"""


class Config(AuthConfig, DBConfig, GenericConfig):
    """The injected behavior required for the httppubsubserver to operate. This is
    generally generated for you using one of the templates, see the readme for details
    """
