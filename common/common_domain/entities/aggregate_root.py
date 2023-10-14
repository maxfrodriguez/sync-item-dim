from common.common_domain.entities.base_entity import BaseEntity
from common.common_domain.value_object.base_id import BaseId


class AggregateRoot(BaseEntity):
    def __init__(self, id: BaseId) -> None:
        super().__init__(id=id)
        self._pending_events = []

    def __str__(self) -> str:
        super().__str__()  # define in the base class

    def __hash__(self):
        return self.id.__hash__()

    def _register_event(self, event):
        event.aggregate_id = self.id
        self._pending_events.append(event)

    def pull_domain_events(self):
        events = self._pending_events[:]
        self._pending_events.clear()
        return events
