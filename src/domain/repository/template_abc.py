from abc import ABC, abstractmethod
from typing import List

from src.domain.entities.shipment import Shipment

class TemplateABC(ABC):
    @abstractmethod
    async def save_templates(self, shipments: List[Shipment]):
        raise NotImplementedError