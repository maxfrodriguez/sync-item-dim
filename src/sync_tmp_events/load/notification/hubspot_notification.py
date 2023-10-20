import logging
from os import getenv
from orjson import dumps
from typing import List
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from common.common_infrastructure.cross_cutting import ConfigurationEnvHelper
from src.sync_tmp_events.extract.data.shipment import Shipment
from src.sync_tmp_events.load.data.customer import Customer
from src.sync_tmp_events.load.notification.notifier_abc import Notifier


class HubSpotNotifier(Notifier):

    def __init__(self) -> None:
        self._secret: dict[str, str] = {
            "conn": "ServiceBusConn",
            "queue": "SbQueueWhHubspot"
        }
        ConfigurationEnvHelper().get_secrets(self._secret)
        self._sb_client = ServiceBusClient.from_connection_string(conn_str=self._secret["conn"])

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
                sender = self._sb_client.get_queue_sender(queue_name=self._secret["queue"])
                message = ServiceBusMessage(dumps(customer_list))
                sender.send_messages(message)
                        
                
        except Exception as e:
            logging.error(f"Error in send_customer_sb: {e}")