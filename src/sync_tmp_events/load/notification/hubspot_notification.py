import logging
from os import getenv
from orjson import dumps
from typing import List
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from src.infrastructure.cross_cutting.service_bus.service_bus_impl import ServiceBusImpl
from src.sync_tmp_events.extract.data.shipment import Shipment
from src.sync_tmp_events.load.data.customer import Customer
from src.sync_tmp_events.load.notification.notifier_abc import Notifier


class HubSpotNotifier(Notifier):

    def __init__(self, stage) -> None:
        _sb_con_string: str= getenv(f"SERVICE_BUS_CONN_STRING_{stage.name}")
        self._queue_name: str= getenv(f"SB_QUEUE_WH_HUBSPOT_{stage.name}")
        self._sb_client: ServiceBusImpl = ServiceBusClient.from_connection_string(conn_str=_sb_con_string)

    async def send_information(self, shipments_customers: List[Shipment]):
        customers: List[Customer] = []
        customer_hash: set = set()
        try:
            for shipment in shipments_customers:
                if shipment.ds_id and shipment.customer_id and shipment.template_id and shipment.customer_id not in customer_hash:
                    customers.append(Customer(
                        tmp= shipment.ds_id
                        , customer_id= shipment.customer_id
                        , template_id= shipment.template_id
                        )
                    )
                    customer_hash.add(shipment.customer_id)
            
            if customer_hash:
                customer_list = list(customer_hash)
                sender = self._sb_client.get_queue_sender(queue_name=self._queue_name)
                message = ServiceBusMessage(dumps(customer_list))
                sender.send_messages(message)
                        
                
        except Exception as e:
            logging.error(f"Error in send_customer_sb: {e}")