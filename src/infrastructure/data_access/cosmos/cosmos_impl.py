from asyncio import gather
from typing import Tuple

from azure.cosmos.aio._container import ContainerProxy
from azure.cosmos.aio._database import DatabaseProxy
from typing_extensions import Self

from src.infrastructure.cross_cutting import ENVIRONMENT, KeyVaultImpl

from . import CosmosHelper
from .cosmos_abc import CosmosABC


async def get_cosmos_resources(stage: ENVIRONMENT, container_name: str) -> Tuple[DatabaseProxy, ContainerProxy]:
    cosmos_url: str | None = None
    cosmos_credential: str | None = None
    cosmos_db_name: str | None = None
    cosmos_container_name: str | None = None

    async with KeyVaultImpl(stage) as kv:
        cosmos_url, cosmos_credential, cosmos_db_name, cosmos_container_name = await gather(
            kv.get_secret("COSMOS-ACCOUNT-URI"),
            kv.get_secret("COSMOS-ACCOUNT-KEY"),
            kv.get_secret("COSMOS-DB-NAME"),
            kv.get_secret(container_name),
        )

    if None in {cosmos_url, cosmos_credential, cosmos_db_name, cosmos_container_name}:
        raise ValueError("None value in credentials")

    database, container = await CosmosHelper.get_db_container(
        cosmos_url=cosmos_url,
        credential=cosmos_credential,
        database_name=cosmos_db_name,
        container_name=cosmos_container_name,
    )

    return database, container


class CosmosImpl(CosmosABC):
    def __init__(self, container_name: str, stage: ENVIRONMENT = ENVIRONMENT.PRD) -> None:
        self.__stage: ENVIRONMENT = stage
        self.__database: DatabaseProxy = None
        self.__container: ContainerProxy = None
        self.__container_name: str = container_name

    async def close_all(self) -> None:
        await self.__container.client_connection.pipeline_client.close()

    async def connect(self) -> None:
        self.__database, self.__container = await get_cosmos_resources(self.__stage, self.__container_name)

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(self, *_) -> None:
        await self.close_all()

    async def recreate_container(self) -> None:
        await self.__database.delete_container(self.__container.id)
        _, self.__container = await get_cosmos_resources(self.__stage, self.__container_name)

    @property
    def container(self) -> ContainerProxy:
        return self.__container
