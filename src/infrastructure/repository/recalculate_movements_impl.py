from datetime import datetime
import logging
from typing import Optional
from typing_extensions import Self
from src.domain.entities.shipment import Shipment
from src.domain.repository.recalculate_movements_abc import RecalculateMovementsRepositoryABC
from src.infrastructure.cross_cutting.environment import ENVIRONMENT
from src.infrastructure.cross_cutting.event_grid.event_grid_impl import EventGridImpl


class RecalculateMovementsImpl(RecalculateMovementsRepositoryABC):
    def __init__(self, stage) -> None:
        self.__enviroment: ENVIRONMENT = stage
        self.eg_client: EventGridImpl = None
        self.__credential: str= "EVENT-GRID-ACCESS-KEY-CALC-MOVEMENTS"
        self.__endpoint: str= "EVENT-GRID-ENDPOINT-CALC-MOVEMENTS"

    async def __aenter__(self) -> Self:
        async with EventGridImpl(self.__enviroment, self.__credential, self.__endpoint) as client:
            self.eg_client = client

        return self

    async def __aexit__(self, exc_type: Optional[Exception], value, traceback):
        if exc_type is not None:
            logging.info(f"An Exception has occured {value}")

    
    async def recalculate_movements(self, shipment: Shipment):
        try:
            data: dict = {
                "id": shipment.id,
                "ds_id": shipment.ds_id,
                "has_changed_events": shipment.has_changed_events,
                "has_changed_stops": shipment.has_changed_stops,
            }
            self.eg_client.send_event(data=data)

        except Exception as e:
            logging.error(f"Error in recalculate_movements: {e} at {datetime.now()}")
