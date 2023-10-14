from abc import ABC, abstractmethod
from typing import List

from src.sync_tmp_events.extract.data.shipment import Shipment


class SyncShipmentRepositoryABC(ABC):
    @abstractmethod
    def find_shipments_to_sync(self, list_shipmets : List[Shipment]) -> List[Shipment]:
        raise NotImplementedError
    
    @abstractmethod
    def load_shipments(self, list_shipments: List[Shipment], custom_fields: List[dict]):
        raise NotImplementedError