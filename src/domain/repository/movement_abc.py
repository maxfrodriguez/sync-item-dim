from abc import ABC, abstractmethod

from src.domain.entities.shipment import Shipment


class MovementRepositoryABC(ABC):
    @abstractmethod
    async def save_movements(self, shipment: Shipment) -> bool:
        raise NotImplementedError
