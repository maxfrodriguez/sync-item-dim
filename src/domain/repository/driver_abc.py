from abc import ABC, abstractmethod

from src.domain.entities.shipment import Shipment


class DriverRepositoryABC(ABC):
    @abstractmethod
    async def save_drivers(self, shipment: Shipment) -> bool:
        raise NotImplementedError