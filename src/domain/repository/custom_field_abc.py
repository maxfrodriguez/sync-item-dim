from abc import ABC, abstractmethod
from typing import List

from src.domain.entities.shipment import Shipment

class CustomFieldABC(ABC):
    
    @abstractmethod
    async def get_custom_fields(self, shipments: List[Shipment]):
        raise NotImplementedError

    @abstractmethod
    async def save_custom_fields(self, custom_field: str):
        raise NotImplementedError