from abc import ABC, abstractmethod

from src.domain.entities.shipment import Shipment


class RecalculateMovementsRepositoryABC(ABC):
    @abstractmethod
    def recalculate_movements(shipment: Shipment):
        raise NotImplementedError