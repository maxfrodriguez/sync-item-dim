from src.infrastructure.cross_cutting.environment import ENVIRONMENT
from src.infrastructure.data_access.alchemy.sa_session_impl import AlchemyBase


class Tower121DdConnector(AlchemyBase):
    def __init__(self, stage: ENVIRONMENT = ENVIRONMENT.PRD):
        secrets = {
            "user": "PGSQL-USER",
            "password": "PGSQL-PASSWORD",
            "host": "PGSQL-HOST",
            "port": "PGSQL-PORT",
            "db": "PGSQL-DATABASE",
        }
        super().__init__(keyVaults=secrets, stage=stage)

    async def connect(self) -> None:
        await self._get_sqlalchemy_resources(alchemyDriverName="postgresql+psycopg2")
