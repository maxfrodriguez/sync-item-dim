import logging
from os import getenv

from azure.servicebus import ServiceBusClient, ServiceBusMessage
from orjson import dumps
from typing_extensions import Self
from common.common_infrastructure.cross_cutting import ConfigurationEnvHelper

from common.common_infrastructure.cross_cutting.geotab_client_api.geotab_impl import SingletonMeta
from common.common_infrastructure.cross_cutting.service_bus.service_bus_abc import ServiceBusABC


class ServiceBusImpl(ServiceBusABC, metaclass=SingletonMeta):
    def __init__(self):
        self.client: ServiceBusClient = None

    def __enter__(self) -> Self:
        try:
            if not self.client:
                self.__connect_service_bus()
            return self
        except Exception as e:
            logging.exception(f"Error while creating ServiceBus: {e}")
            raise e

    def __exit__(self, *_) -> None:
        self.client.close()

    def __connect_service_bus(self):
        try:
            service_bus_con_str = ConfigurationEnvHelper().get_secret("ServiceBusConnURL")

            self.client = ServiceBusClient.from_connection_string(conn_str=service_bus_con_str)

        except Exception as e:
            logging.exception(f"Error while creating ServiceBus: {e}")
            raise e

    def send_message(self, data, queue_name):
        sender = self.client.get_queue_sender(queue_name=getenv(queue_name))
        message = ServiceBusMessage(dumps(data))
        sender.send_messages(message)
        print("Sent a single message")
