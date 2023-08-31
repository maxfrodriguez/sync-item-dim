from abc import ABC, abstractmethod


class OnTimeDeliveryABC(ABC):

    @abstractmethod
    async def send_on_time_delivery_sb(self, shipment_list: list):
        raise NotImplementedError