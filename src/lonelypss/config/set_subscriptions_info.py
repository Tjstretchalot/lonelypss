from typing import AsyncIterator, Protocol


class SetSubscriptionsInfo(Protocol):
    """Allows a single, ordered iteration over the topics and urls that a subscriber
    provided in SET_SUBSCRIPTIONS.
    """

    def topics(self) -> AsyncIterator[bytes]:
        """MUST be called before globs() or an error is raised, cannot be called
        multiple times, the corresponding iterable cannot be returned to the start
        """

    def globs(self) -> AsyncIterator[str]:
        """MUST be called after topics() or an error is raised, cannot be
        called multiple times, the corresponding iterable cannot be returned to the start
        """
