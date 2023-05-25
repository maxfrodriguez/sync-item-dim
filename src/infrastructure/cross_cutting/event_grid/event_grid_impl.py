import logging
from typing_extensions import Self

from azure.core.credentials import AzureKeyCredential
from azure.eventgrid import EventGridEvent, EventGridPublisherClient

from src.infrastructure.cross_cutting.event_grid.event_grid_abc import EventGridABC
from src.infrastructure.cross_cutting.key_vault_impl import KeyVaultImpl
from src.infrastructure.cross_cutting.environment import ENVIRONMENT

async def conect(enviroment: ENVIRONMENT, credential: str, endpoint: str):
    async with KeyVaultImpl(enviroment) as kv:
        credential = await kv.get_secret(credential)
        endpoint = await kv.get_secret(endpoint)

    credential = AzureKeyCredential(credential)
    _client = EventGridPublisherClient(endpoint, credential)

    return _client


class EventGridImpl(EventGridABC):
    def __init__(self, stage: ENVIRONMENT = ENVIRONMENT.PRD, credential:str = None, endpoint: str = None) -> None:
        self.__environment: ENVIRONMENT = stage
        self.__client_eg: EventGridPublisherClient = None
        self.__credential: str = credential
        self.__endpoint: str = endpoint

    async def __aenter__(self) -> Self:
        self.__client_eg = await conect(self.__environment, self.__credential, self.__endpoint)
        return self

    async def __aexit__(self, *_) -> None:
        pass

    def send_event(self, data: dict):
        errors = None
        try:
            logging.info(f"Send event {data}")
            recalculate_movements = EventGridEvent(
                subject="recalculate_movements", event_type="recalculate_movements", data=data, data_version="1.0"
            )
            self.__client_eg.send(recalculate_movements)

        except Exception as e:
            exp: str = str(e)
            logging.info(f"An exception occurred trying to send message: {exp}")
            errors = f"An exception occurred trying to send message: {exp}"
        finally:
            return errors
