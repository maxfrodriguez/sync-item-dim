from abc import ABC, abstractmethod

class EmptyReturnABC(ABC):
    @abstractmethod
    async def create_empty_return(self, shipment):
        raise NotImplementedError