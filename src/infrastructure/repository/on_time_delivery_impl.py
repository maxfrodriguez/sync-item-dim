from typing_extensions import Self
from src.domain.repository.on_time_delivery_abc import OnTimeDeliveryABC
from src.infrastructure.cross_cutting.environment import ENVIRONMENT
from src.infrastructure.cross_cutting.service_bus.service_bus_impl import ServiceBusImpl


class OnTimeDeliveryImpl(OnTimeDeliveryABC):
    def __init__(self, stage) -> None:
        self.__enviroment: ENVIRONMENT = stage
        self._sb_client: ServiceBusImpl = None
        self.__sb_con_string: str= "SB-CONN-STR-ON-TIME-DELIVERY" 
        self.__queue_name: str= "SB-QUEUE-ON-TIME-DELIVERY"

    async def __aenter__(self) -> Self:
        async with ServiceBusImpl(self.__enviroment, self.__sb_con_string, self.__queue_name) as sb_client:
            self._sb_client = sb_client
        return self
        

    async def create_on_time_delivery(self, shipment_list: list):
        pass