from common.common_domain.events.domain_event import DomainEvent
from common.common_domain.events.domain_event_suscriber import DomainEventSubscriber


class DomainEventPublisher:
    # Using a dictionary to map events to their subscribers
    # _subscribers = {}

    def __init__(self) -> None:
        self._subscribers = {}

    def subscribe(self, event_type: type, *subscribers: DomainEventSubscriber) -> None:
        """
        Subscribe the given subscribers to the given event type.
        :param event_type: The event type to subscribe to.
        :param subscribers: The subscribers to handle this type of event.
        """

        # Ensure the event type key exists
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []

        # Add the subscribers for this event type
        self._subscribers[event_type].extend(subscribers)

    # @classmethod
    def publish(self, *events: DomainEvent) -> None:
        for event in events:
            event_type = type(event)

            # If there are subscribers for this event type, notify them
            for subscriber in self._subscribers.get(event_type, []):
                subscriber.handle_event(event)
