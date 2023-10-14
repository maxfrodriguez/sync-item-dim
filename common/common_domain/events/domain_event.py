from abc import abstractmethod
from typing import Any

from common.common_domain.value_object.base_id import BaseId


class DomainEvent:
    def __init__(self, aggregate_id: BaseId) -> None:
        self.aggregate_id = aggregate_id
        self.version = 1

    @abstractmethod
    def publish_event(self, event: Any):
        pass
