from asyncio import gather
from typing import Tuple

from azure.data.tables.aio import TableClient, TableServiceClient
from typing_extensions import Self

from src.infrastructure.cross_cutting import ENVIRONMENT, KeyVaultImpl

from . import TableStorageConnection
from .storage_abc import TableStorageABC


async def get_table_storage_resources(stage: ENVIRONMENT) -> Tuple[TableServiceClient, TableClient]:
    conn_str: str | None = None
    table_name: str | None = None

    async with KeyVaultImpl(stage) as kv:
        conn_str, table_name = await gather(kv.get_secret("TS-CONN-STRING"), kv.get_secret("TS-TABLE-NAME"))

    if None in {conn_str, table_name}:
        raise ValueError("None value in credentials")

    connection: TableServiceClient = None
    table_client: TableClient = None
    connection, table_client = await TableStorageConnection.get_service_client(conn_str, table_name)

    return connection, table_client


class TableStorageImpl(TableStorageABC):
    def __init__(self, stage: ENVIRONMENT = ENVIRONMENT.PRD):
        self.__connection: TableServiceClient = None
        self.__table_client: TableClient = None
        self.__stage: ENVIRONMENT = stage

    async def connect(self) -> None:
        self.__connection, self.__table_client = await get_table_storage_resources(self.__stage)

    async def close_all(self) -> None:
        await self.__table_client.close()
        await self.__connection.close()

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(self, *_) -> None:
        await self.close_all()

    @property
    def table_client(self) -> TableClient:
        return self.__table_client
