from typing import (
    AsyncIterable,
    Literal,
    Optional,
    Protocol,
    Type,
    Union,
    TYPE_CHECKING,
)

from httppubsubserver.config.auth_config import AuthConfig


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


class GenericConfigFromValues:
    """Convenience class that allows you to create a GenericConfig protocol
    satisfying object from values"""

    def __init__(
        self,
        message_body_spool_size: int,
        outgoing_http_timeout_total: Optional[float],
        outgoing_http_timeout_connect: Optional[float],
        outgoing_http_timeout_sock_read: Optional[float],
        outgoing_http_timeout_sock_connect: Optional[float],
    ):
        self.message_body_spool_size = message_body_spool_size
        self.outgoing_http_timeout_total = outgoing_http_timeout_total
        self.outgoing_http_timeout_connect = outgoing_http_timeout_connect
        self.outgoing_http_timeout_sock_read = outgoing_http_timeout_sock_read
        self.outgoing_http_timeout_sock_connect = outgoing_http_timeout_sock_connect


class Config(AuthConfig, DBConfig, GenericConfig, Protocol):
    """The injected behavior required for the httppubsubserver to operate. This is
    generally generated for you using one of the templates, see the readme for details
    """


class ConfigFromParts:
    """Convenience class that combines the three parts of the config into a single object."""

    def __init__(self, auth: AuthConfig, db: DBConfig, generic: GenericConfig):
        self.auth = auth
        self.db = db
        self.generic = generic

    async def is_subscribe_exact_allowed(
        self, /, *, url: str, exact: bytes, now: float, authorization: Optional[str]
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return await self.auth.is_subscribe_exact_allowed(
            url=url, exact=exact, now=now, authorization=authorization
        )

    async def is_subscribe_glob_allowed(
        self, /, *, url: str, glob: str, now: float, authorization: Optional[str]
    ) -> Literal["ok", "unauthorized", "forbidden", "unavailable"]:
        return await self.auth.is_subscribe_glob_allowed(
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
        return await self.auth.is_notify_allowed(
            topic=topic,
            message_sha512=message_sha512,
            now=now,
            authorization=authorization,
        )

    async def setup_authorization(
        self, /, *, url: str, topic: bytes, message_sha512: bytes, now: float
    ) -> Optional[str]:
        return await self.auth.setup_authorization(
            url=url, topic=topic, message_sha512=message_sha512, now=now
        )

    async def subscribe_exact(
        self, /, *, url: str, exact: bytes
    ) -> Literal["success", "conflict", "unavailable"]:
        return await self.db.subscribe_exact(url=url, exact=exact)

    async def unsubscribe_exact(
        self, /, *, url: str, exact: bytes
    ) -> Literal["success", "not_found", "unavailable"]:
        return await self.db.unsubscribe_exact(url=url, exact=exact)

    async def subscribe_glob(
        self, /, *, url: str, glob: str
    ) -> Literal["success", "conflict", "unavailable"]:
        return await self.db.subscribe_glob(url=url, glob=glob)

    async def unsubscribe_glob(
        self, /, *, url: str, glob: str
    ) -> Literal["success", "not_found", "unavailable"]:
        return await self.db.unsubscribe_glob(url=url, glob=glob)

    def get_subscribers(
        self, /, *, topic: bytes
    ) -> AsyncIterable[Union[str, Literal[b"unavailable"]]]:
        return self.db.get_subscribers(topic=topic)

    @property
    def message_body_spool_size(self) -> int:
        return self.generic.message_body_spool_size

    @property
    def outgoing_http_timeout_total(self) -> Optional[float]:
        return self.generic.outgoing_http_timeout_total

    @property
    def outgoing_http_timeout_connect(self) -> Optional[float]:
        return self.generic.outgoing_http_timeout_connect

    @property
    def outgoing_http_timeout_sock_read(self) -> Optional[float]:
        return self.generic.outgoing_http_timeout_sock_read

    @property
    def outgoing_http_timeout_sock_connect(self) -> Optional[float]:
        return self.generic.outgoing_http_timeout_sock_connect


if TYPE_CHECKING:
    __: Type[GenericConfig] = GenericConfigFromValues
    ___: Type[Config] = ConfigFromParts
