import logging
from os import getenv
from datetime import datetime
from typing import List

from azure.core.credentials import AzureKeyCredential
from azure.eventgrid import EventGridEvent, EventGridPublisherClient

from src.domain.entities.shipment import Shipment
from src.infrastructure.cross_cutting.event_grid.event_grid_impl import EventGridImpl
from src.sync_tmp_events.load.notification.notifier_abc import Notifier


class DimChangeStatusChange(Notifier):
    def __init__(self, stage) -> None:
        _credential: str= getenv(f"EVENT_GRID_CHANGE_STATUS_KD_CREDENTIAL_{stage.name}")
        _endpoint: str= getenv(f"EVENT_GRID_CHANGE_STATUS_KD_ENDPOINT_{stage.name}")
        self.__eg_client: EventGridImpl = EventGridPublisherClient(
            _endpoint
            , AzureKeyCredential(_credential)
        )

    
    async def send_information(self, shipment_list: List[Shipment]):
        try:
            shipments_id = [shipment.ds_id for shipment in shipment_list if shipment.ds_status != 'K']
            
            data: dict = {
                "shipments_id": shipments_id
            }

            logging.info(f"Send event {data}")
            recalculate_movements = EventGridEvent(
                subject="recalculate_movements", event_type="recalculate_movements", data=data, data_version="1.0"
            )
            self.__eg_client.send(recalculate_movements)

        except Exception as e:
            logging.error(f"Error in send_street_turn_information: {e} at {datetime.now()}")