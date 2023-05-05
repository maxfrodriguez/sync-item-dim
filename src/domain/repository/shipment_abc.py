from abc import ABC, abstractmethod
from typing import List

from src.domain.entities.shipment import Shipment


class ShipmentRepositoryABC(ABC):
    @abstractmethod
    def retrieve_shipment_list(self, last_modlog: int = None) -> List[Shipment] | None:
        raise NotImplementedError

    # @abstractmethod
    # def save_and_sync_shipment(self, shipment: Shipment) -> Shipment | None:
    #     raise NotImplementedError
