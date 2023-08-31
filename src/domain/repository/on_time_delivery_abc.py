from abc import ABC, abstractmethod


class OnTimeDeliveryABC(ABC):

    @abstractmethod
    async def create_on_time_delivery(self, shipment_list: list):
        raise NotImplementedError