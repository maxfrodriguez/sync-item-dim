import logging
from os import getenv
from orjson import dumps

from azure.servicebus import ServiceBusClient, ServiceBusMessage
from common.common_infrastructure.cross_cutting import ConfigurationEnvHelper

from src.sync_tmp_events.load.notification.notifier_abc import Notifier


class OnTimeDeliveryNotifier(Notifier):
    def __init__(self) -> None:
        self._secret: dict[str, str] = {
            "conn": "ServiceBusConn",
            "queue": "SbQueueOnTimeDelivery"
        }

        ConfigurationEnvHelper().get_secrets(self._secret)

        self._sb_client = ServiceBusClient.from_connection_string(conn_str=self._secret["conn"])

    async def send_information(self, shipment_list: list):
        try:
            
            id_list = [shipment.ds_id for shipment in shipment_list]
            
            sender = self._sb_client.get_queue_sender(queue_name=self._secret["queue"])
            message = ServiceBusMessage(dumps(id_list))
            sender.send_messages(message)

        except Exception as e:
            logging.error(f"Error in create_on_time_delivery: {e}")