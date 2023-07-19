import logging
from orjson import dumps
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from src.infrastructure.cross_cutting.key_vault_impl import KeyVaultImpl
from src.infrastructure.cross_cutting.service_bus.service_bus_abc import ServiceBusABC
from src.infrastructure.cross_cutting.environment import ENVIRONMENT

class ServiceBusImpl(ServiceBusABC):
    def __init__(self, stage: ENVIRONMENT = ENVIRONMENT.PRD):
        self.__environment: ENVIRONMENT = stage
        self.client: ServiceBusClient = None
        self.queue_name = None

    async def __aenter__(self) -> 'ServiceBusImpl':
        try:
            if not self.client:
                await self.__connect_service_bus()
            return self
        except Exception as e:
            logging.exception(f"Error while creating ServiceBus: {e}")
            raise e

    async def __aexit__(self, *_) -> None:
        pass

    async def __connect_service_bus(self):
        try:
            async with KeyVaultImpl(self.__environment) as kv:
                service_bus_con_str = await kv.get_secret("SERVICE-BUS-CONN")
                queue_name = await kv.get_secret("QUEUE-NAME")

            self.client = ServiceBusClient.from_connection_string(conn_str=service_bus_con_str)
            self.queue_name = queue_name
            
            del service_bus_con_str, queue_name
        except Exception as e:
            logging.exception(f"Error while creating ServiceBus: {e}")
            raise e

    async def send_message(self, data, queue_name):
        sender = self.client.get_queue_sender(queue_name=queue_name)
        message = ServiceBusMessage(dumps(data))
        await sender.send_messages(message)
        print("Sent a single message")
