import logging
from os import getenv
from datetime import datetime
from typing import List, Optional
from typing_extensions import Self
from src.domain.entities.shipment import Shipment
from src.domain.repository.fact_customers_kpi_abc import FactCustomerKPIRepositoryABC
from src.infrastructure.cross_cutting.environment import ENVIRONMENT
from src.infrastructure.cross_cutting.event_grid.event_grid_impl import EventGridImpl


class FactCustomerKPIImpl(FactCustomerKPIRepositoryABC):
    def __init__(self, stage) -> None:
        self.__enviroment: ENVIRONMENT = stage
        self.eg_client: EventGridImpl = None
        self.__credential: str= "EVENT-GRID-CUSTOMER-KD-CREDENTIAL"
        self.__endpoint: str= "EVENT-GRID-CUSTOMER-KD-ENDPOINT"

    async def __aenter__(self) -> Self:
        async with EventGridImpl(self.__enviroment, self.__credential, self.__endpoint) as client:
            self.eg_client = client

        return self

    async def __aexit__(self, exc_type: Optional[Exception], value, traceback):
        if exc_type is not None:
            logging.info(f"An Exception has occured {value}")

    
    async def send_customer_kd_info(self, shipment_list: List[Shipment]):
        try:
            shipments_id = [shipment.ds_id for shipment in shipment_list]
            
            data: dict = {
                "shipments_id": shipments_id
            }
            self.eg_client.send_event(data=data)

        except Exception as e:
            logging.error(f"Error in send_street_turn_information: {e} at {datetime.now()}")