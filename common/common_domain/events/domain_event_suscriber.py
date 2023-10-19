from typing import Protocol

from common.common_domain.events.domain_event import DomainEvent


class DomainEventSubscriber(Protocol):
    def handle_event(self, event: DomainEvent) -> None:
        pass
