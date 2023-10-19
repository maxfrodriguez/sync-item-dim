
from common.common_infrastructure.cross_cutting.environment import ENVIRONMENT
from src.infrastructure.data_access.alchemy.sa_session_impl import AlchemyBase


class WareHouseDbConnector(AlchemyBase):
    def __init__(self, stage: ENVIRONMENT = ENVIRONMENT.PRD):
        secrets = {
            "user": "SqlUser",
            "password": "SqlPwd",
            "host": "SqlHost",
            "port": "SqlPort",
            "db": "SqlDb",
            "params": "SqlParams",
        }
        super().__init__(keyVaults=secrets, passEncrypt=True, stage=stage)

    async def connect(self) -> None:
        await self._get_sqlalchemy_resources(alchemyDriverName="mssql+pyodbc")
