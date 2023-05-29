from src.infrastructure.cross_cutting.environment import ENVIRONMENT
from src.infrastructure.data_access.alchemy.sa_session_impl import AlchemyBase


class WareHouseDbConnector(AlchemyBase):
    def __init__(self, stage: ENVIRONMENT = ENVIRONMENT.PRD):
        secrets = {
            "user": "SQL-USER",
            "password": "SQL-PASSWORD",
            "host": "SQL-HOST",
            "port": "SQL-PORT",
            "db": "SQL-DB",
            "params": "SQL-PARAMS",
        }
        super().__init__(keyVaults=secrets, passEncrypt=True, stage=stage)

    async def connect(self) -> None:
        await self._get_sqlalchemy_resources(alchemyDriverName="mssql+pyodbc")
