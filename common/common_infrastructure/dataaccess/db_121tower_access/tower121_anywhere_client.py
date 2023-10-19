import logging

from common.common_infrastructure.cross_cutting.environment import ENVIRONMENT
from common.common_infrastructure.dataaccess.db_context.alchemy.sa_session_impl import AlchemyBase


class Tower121DdConnector(AlchemyBase):
    def __init__(self, stage: ENVIRONMENT = ENVIRONMENT.PRD):
        secrets = {
            "user": "POSTGRES-USER",
            "password": "POSTGRES-PASSWORD",
            "host": "POSTGRES-HOST",
            "port": "POSTGRES-PORT",
            "db": "POSTGRES-DB",
        }
        super().__init__(keyVaults=secrets, stage=stage)

    def connect(self) -> None:
        try:
            self._get_sqlalchemy_resources(alchemyDriverName="postgresql+psycopg2")
        except Exception as e:
            logging.error(f"Error connecting to database 121Tower: {e}")
            raise e
