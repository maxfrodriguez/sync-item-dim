import logging
from typing_extensions import Self
from orjson import dumps
from azure.core.credentials import AzureKeyCredential
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from src.infrastructure.cross_cutting.key_vault_impl import KeyVaultImpl
from src.infrastructure.cross_cutting.service_bus.service_bus_abc import ServiceBusABC
from src.infrastructure.cross_cutting.environment import ENVIRONMENT

async def connect(enviroment: ENVIRONMENT, service_bus_con_str: str, queue_name: str):
    async with KeyVaultImpl(enviroment) as kv:
        service_bus_con_str = await kv.get_secret(service_bus_con_str)
        __queue_name = await kv.get_secret(queue_name)

    service_bus_con_str = AzureKeyCredential(service_bus_con_str)
    __client = ServiceBusClient.from_connection_string(conn_str=service_bus_con_str)

    return __client, __queue_name

class ServiceBusImpl(ServiceBusABC):
    
    def __init__(self, stage: ENVIRONMENT = ENVIRONMENT.PRD, service_bus_con_str: str = None, queue_name: str = None):
        self.__environment: ENVIRONMENT = stage
        self._client: ServiceBusClient = None
        self.__service_bus_con_str: str = service_bus_con_str
        self._queue_name = queue_name

    async def __aenter__(self) -> Self:
        self._client , self._queue_name = await connect(self.__environment, self.__service_bus_con_str, self._queue_name)
        return self

    async def __aexit__(self, *_) -> None:
        pass

    async def send_message(self, data):
        sender = self._client.get_queue_sender(queue_name=self._queue_name)
        message = ServiceBusMessage(dumps(data))

        await sender.send_messages(message)
