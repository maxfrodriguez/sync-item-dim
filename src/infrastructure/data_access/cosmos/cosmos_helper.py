from typing import Tuple

from azure.cosmos.aio._container import ContainerProxy
from azure.cosmos.aio._cosmos_client import CosmosClient
from azure.cosmos.aio._database import DatabaseProxy
from azure.cosmos.partition_key import PartitionKey


class CosmosHelper:
    @staticmethod
    async def get_db_container(
        cosmos_url: str, credential: str, database_name: str, container_name: str, partition_key: str = "/id"
    ) -> Tuple[DatabaseProxy, ContainerProxy]:
        client: CosmosClient = CosmosClient(url=cosmos_url, credential=credential)
        database: DatabaseProxy = await client.create_database_if_not_exists(id=database_name)
        container: ContainerProxy = await database.create_container_if_not_exists(
            id=container_name, partition_key=PartitionKey(partition_key)
        )
        return database, container
