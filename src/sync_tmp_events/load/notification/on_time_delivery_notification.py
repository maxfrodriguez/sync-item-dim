import logging
from os import getenv
from orjson import dumps

from azure.servicebus import ServiceBusClient, ServiceBusMessage

from src.infrastructure.cross_cutting.service_bus.service_bus_impl import ServiceBusImpl
from src.sync_tmp_events.load.notification.notifier_abc import Notifier


class OnTimeDeliveryNotifier(Notifier):
    def __init__(self, stage) -> None:
        #self.__enviroment: ENVIRONMENT = stage
        _sb_con_string: str= getenv(f"SERVICE_BUS_CONN_STRING_{stage.name}")
        self._queue_name: str= getenv(f"SB_QUEUE_ON_TIME_DELIVERY_{stage.name}")
        self._sb_client: ServiceBusImpl = ServiceBusClient.from_connection_string(conn_str=_sb_con_string)

    async def send_information(self, shipment_list: list):
        try:
            
            id_list = [shipment.ds_id for shipment in shipment_list]
            
            sender = self._sb_client.get_queue_sender(queue_name=self._queue_name)
            message = ServiceBusMessage(dumps(id_list))
            sender.send_messages(message)

        except Exception as e:
            logging.error(f"Error in create_on_time_delivery: {e}")