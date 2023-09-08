from abc import ABC, abstractmethod
from typing import List

from src.domain.entities.shipment import Shipment


class EventRepositoryABC(ABC):
    @abstractmethod
    def save_and_sync_events(self, list_of_shipments: List[Shipment]) -> None:
        raise NotImplementedError
