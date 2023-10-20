import logging
from datetime import datetime
from typing import Any, Union

from azure.cosmos.container import ContainerProxy
from azure.cosmos.cosmos_client import CosmosClient
from azure.cosmos.database import DatabaseProxy
from azure.cosmos.partition_key import PartitionKey
from typing_extensions import Self

from common.common_infrastructure.cross_cutting import ConfigurationEnvHelper, KeyVaultImpl

from .cosmos_abc import CosmosABC


class CosmosImpl(CosmosABC):
    def __init__(self) -> None:
        self.__database: DatabaseProxy = None

    def __enter__(self) -> Self:
        try:
            if not self.__database:
                self.__connect_db_cosmos()
            return self
        except Exception as e:
            logging.exception(f"Error while creating CosmosClient: {e}")
            raise e

    def __exit__(self, *_) -> None:
        pass

    def __connect_db_cosmos(self) -> CosmosClient:
        try:
            self._secret: dict[str, str] = {
                "uriCosmos": "CosmosEndpoint",
                "keyCosmos": "CosmosKey",
                "database": "CosmosDB"
            }
            ConfigurationEnvHelper().get_secrets(self._secret)

            client: CosmosClient = CosmosClient(
                url=self._secret["uriCosmos"]
                , credential=self._secret["keyCosmos"]
            )
            
            self.__database = client.create_database_if_not_exists(
                id=self._secret["database"])

        except Exception as e:
            logging.exception(f"Error while creating CosmosClient: {e}")
            raise e

    def __get_cosmos_container(self, container: str, partition_key: str = "/id") -> ContainerProxy:
        try:
            container: ContainerProxy = self.__database.create_container_if_not_exists(
                id=container, partition_key=PartitionKey(partition_key)
            )
            return container
        except Exception as e:
            logging.exception(f"Error while creating CosmosContainer: {e}")
            raise e

    def format_date_for_cosmos(self, dt: datetime) -> str:
        return dt.strftime("%Y-%m-%dT%H:%M:%S%z")

    def save(self, payload: dict[str, Any], container: str, partition_key: str = "/id"):
        try:
            result: Union[dict[str, Any], None] = None

            container = self.__get_cosmos_container(container=container, partition_key=partition_key)

            result = container.upsert_item(body=payload)

            return result
        except Exception as e:
            logging.exception(f"Error while saving data: {e}")
            raise e

    def execute_query(
        self, container: str, query: str, partition_key: str = "/id", parameters: list[dict[str, Any]] = []
    ):
        try:
            result: list[dict[str, Any]] = []

            container = self.__get_cosmos_container(container=container, partition_key=partition_key)

            result = [
                item
                for item in container.query_items(
                    query=query,
                    parameters=parameters,
                    enable_cross_partition_query=True,
                )
            ]
            return result
        except Exception as e:
            logging.exception(f"Error while getting data: {e}")
            raise e
