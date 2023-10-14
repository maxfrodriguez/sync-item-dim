import logging

from common.common_infrastructure.cross_cutting.environment import ENVIRONMENT
from common.common_infrastructure.dataaccess.db_context.sybase.sql_anywhere_impl import SQLAnywhereBase


class PTSQLAnywhere(SQLAnywhereBase):
    def __init__(self, stage: ENVIRONMENT = ENVIRONMENT.PRD):
        secrets = {"uid": "PT-UID", "pwd": "PT-PWD", "host": "PT-HOST", "dbn": "PT-DBN", "server": "PT-SERVER"}
        super().__init__(keyVaults=secrets, stage=stage)

    def connect(self) -> None:
        try:
            self._get_credentials()
            self._get_sybase_resources()
        except Exception as e:
            logging.error(f"Error connecting to database 121Tower: {e}")
            raise e
