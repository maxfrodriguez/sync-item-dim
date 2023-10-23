from src.infrastructure.data_access.alchemy.sa_session_impl import AlchemyBase


class WareHouseDbConnector(AlchemyBase):
    def __init__(self):
        secrets = {
            "user": "SqlUser",
            "password": "SqlPwd",
            "host": "SqlHost",
            "port": "SqlPort",
            "db": "SqlDb",
            #"params": "SqlParams", becareful with this one because gives error in azure functions when using ODBC Driver 17 for SQL Server
        }
        super().__init__(keyVaults=secrets, passEncrypt=True)

    async def connect(self) -> None:
        await self._get_sqlalchemy_resources(alchemyDriverName="mssql+pymssql")
