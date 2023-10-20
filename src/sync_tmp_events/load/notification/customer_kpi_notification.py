import logging
from typing import List
from os import getenv
from orjson import dumps

from azure.servicebus import ServiceBusClient, ServiceBusMessage
from common.common_infrastructure.cross_cutting.ConfigurationEnvHelper import ConfigurationEnvHelper

from src.sync_tmp_events.load.data.customer import Customer
from src.sync_tmp_events.load.notification.notifier_abc import Notifier


class TmpChangedNotifier(Notifier):
    def __init__(self) -> None:
        self._secret: dict[str, str] = {
            "conn": "ServiceBusConn",
            "topic": "SbTopicTmpToSync",
            "subscription": "SbSubscriptionTmToSync"
        }
        ConfigurationEnvHelper().get_secrets(self._secret)
        self._sb_client = ServiceBusClient.from_connection_string(conn_str=self._secret["conn"])

    async def send_information(self, shipments_customers: List[Customer]):
        try:
            customers: List[Customer] = []
            for shipment in shipments_customers:
                if shipment.ds_id and shipment.customer_id and shipment.template_id:
                    customers.append(Customer(
                        customer_id = shipment.customer_id,
                        template_id = shipment.template_id,
                        tmp = shipment.ds_id
                    )
                )
                    
            sender = self._sb_client.get_topic_sender(topic_name=self._secret["topic"], subscription_name=self._secret["subscription"])
            message = ServiceBusMessage(dumps(customers))
            sender.send_messages(message)

        except Exception as e:
            logging.error(f"Error in send_customer_kpi_sb: {e}")