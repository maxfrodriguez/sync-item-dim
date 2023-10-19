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
    
    @abstractmethod
    async def save_latest_loader_logs(
        self, lowest_modlog: int, highest_modlog: int, fact_movements_loaded: int
    ):
        raise NotImplementedError