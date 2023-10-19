import logging
from datetime import datetime
from os import getenv

from azure.core.credentials import AzureKeyCredential
from azure.eventgrid import EventGridEvent, EventGridPublisherClient

from typing import List
from common.common_infrastructure.cross_cutting.environment import ENVIRONMENT, ConfigurationEnvHelper
from src.sync_tmp_events.extract.data.shipment import Shipment

from src.sync_tmp_events.load.notification.notifier_abc import Notifier

class StreetTurnNotifier(Notifier):
    def __init__(self, stage : ENVIRONMENT = ENVIRONMENT.PRD) -> None:
        self._secret: dict[str, str] = {
            "endpoint": "EventGridChangeStatusKDEndpoint",
            "key": "EventGridChangeStatusKDCredential"
        }
        ConfigurationEnvHelper(stage=stage).get_secrets(self._secret)
        self.__client_eg = EventGridPublisherClient(
            self._secret["endpoint"]
            , AzureKeyCredential(self._secret["key"])
        )

    
    async def send_information(self, shipment_list: List[Shipment]):
        try:
            shipments_id = [shipment.ds_id for shipment in shipment_list]
            data: dict = {
                "shipments_id": shipments_id
            }
            
            logging.info(f"Send event {data}")
            recalculate_movements = EventGridEvent(
                subject="recalculate_movements", event_type="recalculate_movements", data=data, data_version="1.0"
            )
            self.__client_eg.send(recalculate_movements)

        except Exception as e:
            logging.error(f"Error in send_street_turn_information: {e} at {datetime.now()}")