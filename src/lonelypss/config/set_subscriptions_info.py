from typing import AsyncIterator, Protocol

from lonelypsp.stateless.make_strong_etag import GlobAndRecovery, TopicAndRecovery


class SetSubscriptionsInfo(Protocol):
    """Allows a single, ordered iteration over the topics and urls that a subscriber
    provided in SET_SUBSCRIPTIONS.
    """

    def topics(self) -> AsyncIterator[TopicAndRecovery]:
        """MUST be called before globs() or an error is raised, cannot be called
        multiple times, the corresponding iterable cannot be returned to the start
        """

    def globs(self) -> AsyncIterator[GlobAndRecovery]:
        """MUST be called after topics() or an error is raised, cannot be
        called multiple times, the corresponding iterable cannot be returned to the start
        """
