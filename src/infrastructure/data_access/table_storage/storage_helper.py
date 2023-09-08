from typing import Tuple

from azure.data.tables.aio import TableClient, TableServiceClient


class TableStorageConnection:
    @staticmethod
    async def get_service_client(conn_str: str, table_name: str) -> Tuple[TableServiceClient, TableClient]:
        connection: TableServiceClient = TableServiceClient.from_connection_string(conn_str)
        table_client: TableClient = await connection.create_table_if_not_exists(table_name)
        return connection, table_client
