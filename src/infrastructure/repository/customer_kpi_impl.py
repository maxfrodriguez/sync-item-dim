import logging
from typing import List, Optional
from typing_extensions import Self
from src.domain.entities.customer import Customer
from src.domain.repository.customer_kpi_abc import CustomerKpiABC
from src.infrastructure.cross_cutting.environment import ENVIRONMENT
from src.infrastructure.cross_cutting.service_bus.service_bus_impl import ServiceBusImpl


class CustomerKpiImpl(CustomerKpiABC):
    def __init__(self, stage) -> None:
        self.__enviroment: ENVIRONMENT = stage
        self._sb_client: ServiceBusImpl = None
        self.__sb_con_string: str= "SB-CONN-STRING"
        self.__queue_name: str= "SB-QUEUE-CUSTOMER-KPI"

    async def __aenter__(self) -> Self:
        async with ServiceBusImpl(self.__enviroment, self.__sb_con_string, self.__queue_name) as sb_client:
            self._sb_client = sb_client
        return self
    
    async def __aexit__(self, exc_type: Optional[Exception], value, traceback):
        if exc_type is not None:
            logging.info(f"An Exception has occured {value}")
    
    async def send_customer_kpi_sb(self, shipments_customers: List[Customer]):
        try:
            if shipments_customers:
                data = [shipment_customer.to_dict() for shipment_customer in shipments_customers]
                await self._sb_client.send_message(data=data)
        except Exception as e:
            logging.error(f"Error in send_customer_kpi_sb: {e}")