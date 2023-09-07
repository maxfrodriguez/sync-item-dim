import logging
from typing import List, Optional
from typing_extensions import Self
from src.domain.entities.customer import Customer
from src.domain.repository.hubspot_abc import HubspotABC
from src.infrastructure.cross_cutting.environment import ENVIRONMENT
from src.infrastructure.cross_cutting.service_bus.service_bus_impl import ServiceBusImpl


class HubspotImpl(HubspotABC):

    def __init__(self, stage) -> None:
        self.__enviroment: ENVIRONMENT = stage
        self._sb_client: ServiceBusImpl = None
        self.__sb_con_string: str= "SERVICE-BUS-CONN-STRING"
        self.__queue_name: str= "SB-QUEUE-WH-HUBSPOT"

    async def __aenter__(self) -> Self:
        async with ServiceBusImpl(self.__enviroment, self.__sb_con_string, self.__queue_name) as sb_client:
            self._sb_client = sb_client
        return self
    
    async def __aexit__(self, exc_type: Optional[Exception], value, traceback):
        if exc_type is not None:
            logging.info(f"An Exception has occured {value}")
    
    async def send_customer_sb(self, shipments_customers: List[Customer]):
        customers: List[Customer] = []
        customer_hash: set = set()
        try:
            if shipments_customers:
                for customer in shipments_customers:
                    if customer.tmp and customer.customer_id and customer.template_id and customer.customer_id not in customer_hash :
                        customers.append(customer)
                        customer_hash.add(customer.customer_id)
                
                if customer_hash:
                    customer_list = list(customer_hash)
                    await self._sb_client.send_message(data=customer_list)
        except Exception as e:
            logging.error(f"Error in send_customer_sb: {e}")