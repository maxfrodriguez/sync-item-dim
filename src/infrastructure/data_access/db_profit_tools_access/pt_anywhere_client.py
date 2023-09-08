from typing import Dict

from src.infrastructure.data_access.sybase.sql_anywhere_impl import SQLAnywhereBase


class PTSQLAnywhere(SQLAnywhereBase):
    async def connect(self) -> None:
        secrets = {"uid": "PT-UID", "pwd": "PT-PWD", "host": "PT-HOST", "dbn": "PT-DBN", "server": "PT-SERVER"}
        connection_params: Dict[str, str] = await self._get_credentials(secrets)
        await self._get_sybase_resources(**connection_params)
