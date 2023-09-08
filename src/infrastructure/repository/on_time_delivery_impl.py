import logging
from typing import Optional
from typing_extensions import Self
from src.domain.repository.on_time_delivery_abc import OnTimeDeliveryABC
from src.infrastructure.cross_cutting.environment import ENVIRONMENT
from src.infrastructure.cross_cutting.service_bus.service_bus_impl import ServiceBusImpl


class OnTimeDeliveryImpl(OnTimeDeliveryABC):
    def __init__(self, stage) -> None:
        self.__enviroment: ENVIRONMENT = stage
        self._sb_client: ServiceBusImpl = None
        self.__sb_con_string: str= "SERVICE-BUS-CONN-STRING" 
        self.__queue_name: str= "SB-QUEUE-ON-TIME-DELIVERY"

    async def __aenter__(self) -> Self:
        async with ServiceBusImpl(self.__enviroment, self.__sb_con_string, self.__queue_name) as sb_client:
            self._sb_client = sb_client
        return self
    
    async def __aexit__(self, exc_type: Optional[Exception], value, traceback):
        if exc_type is not None:
            logging.info(f"An Exception has occured {value}")

    async def send_on_time_delivery_sb(self, shipment_list: list):
        try:
            if shipment_list:
                id_list = [shipment.ds_id for shipment in shipment_list]
                await self._sb_client.send_message(id_list)
        except Exception as e:
            logging.error(f"Error in create_on_time_delivery: {e}")