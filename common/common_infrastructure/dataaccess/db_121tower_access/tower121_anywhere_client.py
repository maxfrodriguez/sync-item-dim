import logging

from common.common_infrastructure.dataaccess.db_context.alchemy.sa_session_impl import AlchemyBase


class Tower121DdConnector(AlchemyBase):
    def __init__(self):
        secrets = {
            "user": "User121Db",
            "password": "Pwd121Db",
            "host": "Host121Db",
            "port": "Port121Db",
            "db": "Bd121Db",
        }
        super().__init__(keyVaults=secrets)

    def connect(self) -> None:
        try:
            self._get_sqlalchemy_resources(alchemyDriverName="postgresql+psycopg2")
        except Exception as e:
            logging.error(f"Error connecting to database 121Tower: {e}")
            raise e
