import logging
from typing import Any

from azure.core.exceptions import ResourceNotFoundError
from azure.data.tables import TableClient, TableEntity, TableServiceClient, UpdateMode
from typing_extensions import Self
from common.common_infrastructure.cross_cutting import ConfigurationEnvHelper



class TableStorageImpl:
    def __init__(self) -> None:
        self.__conn: TableServiceClient = None
        self.__tbl: TableClient = None

    def __enter__(self) -> Self:
        try:
            self.connect()
            return self
        except Exception as e:
            logging.exception(f"Error while creating TableStorageImpl: {e}")
            raise e

    def __exit__(self, *_) -> None:
        pass

    def connect(self) -> Self:

        _secrets: dict[str, str] = {
            "ConnSring": "tblConnStr",
            "BillingRreportTable": "tblReportBilling"
        }
        ConfigurationEnvHelper().get_secrets(self._secret)

        self.__conn = TableServiceClient.from_connection_string(_secrets["ConnSring"])
        self.__tbl = self.__conn.create_table_if_not_exists(_secrets["BillingRreportTable"])

    def close_all(self) -> None:
        self.__conn.close()
        self.__tbl.close()

    def upsert_entity(self, entity: dict) -> dict[str, str]:
        result: dict[str, str] = {}

        result = self.__tbl.upsert_entity(mode=UpdateMode.MERGE, entity=entity)
        return result

    def insert_db_entity(self, entity: dict) -> dict | None:
        result: dict | None = None

        result = self.__tbl.create_entity(entity=entity)
        return result

    def query(
        self, query_filter: str, parameters: dict[str, Any] = None, select: list[str] = None
    ) -> list[TableEntity]:
        query_results: list[TableEntity] = []
        try:
            query_results = [
                entity
                for entity in self.__tbl.query_entities(
                    query_filter=query_filter, parameters=parameters, select=select
                )
            ]
        except ResourceNotFoundError as e:
            logging.exception(f"Entity not exists. {str(e.reason)}")
        except Exception as e:
            logging.exception(f"Error searching entity. {str(e)}")
        return query_results
