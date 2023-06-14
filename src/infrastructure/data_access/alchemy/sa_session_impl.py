import logging
from asyncio import gather
from contextlib import asynccontextmanager
from datetime import datetime
from json import loads
from typing import Any, Dict, Generator, List, Optional, Union
from urllib.parse import quote

from async_lru import alru_cache
from sqlalchemy import Select, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.infrastructure.cross_cutting import ENVIRONMENT, KeyVaultImpl
from src.infrastructure.data_access.alchemy.sa_session_helper import SASessionMaker


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class AlchemyBase(metaclass=Singleton):
    _instance = None
    _secrets: Dict[str, str] = None
    _sessionmaker: sessionmaker[Session] = None
    _connector: str = None
    _keyVaultParams: dict[str, str] = None
    _session: Session = None

    def __init__(
        self, keyVaults: dict[str, str] = None, passEncrypt: bool = False, stage: ENVIRONMENT = ENVIRONMENT.PRD
    ):
        self._stage = stage
        self._passEncrypt = passEncrypt
        self._keyVaultParams = keyVaults

    async def _get_credentials(self) -> None:
        if self._secrets is None:
            self._secrets = {}
            async with KeyVaultImpl(self._stage) as kv:
                gathered_secrets = await gather(*(kv.get_secret(secret) for secret in self._keyVaultParams.values()))

            for key, value in zip(self._keyVaultParams.keys(), gathered_secrets):
                if value is None:
                    raise ValueError(f"None value in credentials for {key}")
                self._secrets[key] = value

    def __get_password(self) -> str:
        password = self._secrets["password"]
        if self._passEncrypt:
            password = quote(password)
        return password

    async def _setup_engine(self, alchemyDriverName: str) -> Engine:
        password = self.__get_password()

        # if I recieve the param port I will use it, otherwise I will use the default port
        # if "port" in self._secrets:
        connection_str = (
            f"{alchemyDriverName}://{self._secrets['user']}:{password}"
            f"@{self._secrets['host']}:{self._secrets['port']}/{self._secrets['db']}"
        )

        if "params" in self._secrets:
            params = self.__decode_params(self._secrets["params"])
            connection_str = f"{connection_str}?{params}"

        # if alchemyDriverName == "mssql+pyodbc":
        #     connection_str = "mssql+pyodbc://sa:4vk2R6R1kLktS09Q@127.0.0.1:1433/MovementsCalculatorDB?driver=ODBC+Driver+17+for+SQL+Server"

        return create_engine(url=connection_str, echo=True)

    def __decode_params(self, params: str) -> str:
        params_decoded = loads(params)
        return "?".join([f"{key}={value.replace(' ', '+')}" for key, value in params_decoded.items()])

    async def _get_sqlalchemy_resources(self, alchemyDriverName: str) -> None:
        if self._sessionmaker is None:
            await self._get_credentials()
            engine = await self._setup_engine(alchemyDriverName=alchemyDriverName)
            self._sessionmaker = sessionmaker(bind=engine)

    def execute_select(self, query: Union[str, Select]) -> List[Dict[str, Any]]:
        try:
            with self._sessionmaker() as session:
                if isinstance(query, str):
                    result_proxy = session.execute(text(query))
                if isinstance(query, Select):
                    result_proxy = session.execute(query)
                result = result_proxy.fetchall()
                columns = result_proxy.keys()
                return [dict(zip(columns, row)) for row in result]
        except Exception:
            logging.error(f"Error executing query: {query}")

    async def execute_statement(self, query: Select) -> Any:
        try:
            with self._sessionmaker() as session:
                if isinstance(query, Select):
                    result_proxy = session.execute(query)
                    result = result_proxy.scalars().first()
                    return result
                else:
                    raise ValueError(f"Invalid query type: {type(query)}")
        except Exception:
            logging.error(f"Error executing query: {query}")

    def bulk_copy(self, objects: List[Any]) -> None:
        try:
            # self._session.add_all(objects)
            with self._session.begin():
                self._session.add_all(objects)
        except Exception as e:
            logging.error(f"Error: {e} executing the bulk copy at: {datetime.now()}")
    
    def save_object(self, object: Any) -> None:
        try:
            self._session.add(object)
        except Exception:
            logging.error(f"Error executing the bulk copy at: {datetime.now()}")

    async def execute(self, query: Select) -> Any:
        try:
            with self._sessionmaker() as session:
                if isinstance(query, Select):
                    result_proxy = session.execute(query)
                    result = [{**row} for row in result_proxy.fetchall()]
                    return result
                else:
                    raise ValueError(f"Invalid query type: {type(query)}")
        except Exception:
            logging.error(f"Error executing query: {query}")

    async def __aenter__(self):
        if self._sessionmaker is None:
            await self.connect()
        self._session = self._sessionmaker()
        return self

    async def __aexit__(self, exc_type: Optional[Exception], value, traceback):
        try:
            self._session.commit()
        except Exception as e:
            logging.error(f"Error executing commit: {e}")
            self._session.rollback()
        else:
            self._session.expire_all()
            self._session.close()


def decode_params(params: str) -> str:
    params_decoded: Dict[str, Any] = loads(params)
    return "?".join([f"{key}={value.replace(' ', '+')}" for key, value in params_decoded.items()])


# @alru_cache(maxsize=16)
async def get_connection_str(stage: ENVIRONMENT) -> str:
    # To local use:
    # return "mssql+pyodbc://sa:4vk2R6R1kLktS09Q@127.0.0.1:1433/MovementsCalculatorDB?driver=ODBC+Driver+17+for+SQL+Server"

    user: str | None = None
    password: str | None = None
    host: str | None = None
    port: str | None = None
    db: str | None = None
    params: str | None = None

    async with KeyVaultImpl(stage) as kv:
        user, password, host, port, db, params = await gather(
            kv.get_secret("SQL-USER"),
            kv.get_secret("SQL-PASSWORD"),
            kv.get_secret("SQL-HOST"),
            kv.get_secret("SQL-PORT"),
            kv.get_secret("SQL-DB"),
            kv.get_secret("SQL-PARAMS"),
        )

    if None in {user, password, host, port, db}:
        raise ValueError("None value in credentials")

    password = quote(password)
    connection_str: str = f"mssql+pyodbc://{user}:{password}@{host}:{port}/{db}"
    if params:
        params = decode_params(params=params)
        connection_str = f"{connection_str}?{params}"

    return connection_str


async def get_sa_sessionmaker(connection_str: str) -> sessionmaker[Session]:
    session: sessionmaker[Session] = SASessionMaker.get_sync_session_maker(connection_string=connection_str)
    return session


@asynccontextmanager
async def get_sa_session(stage: ENVIRONMENT = ENVIRONMENT.PRD) -> Generator[Session, None, None]:
    connection_str: str = await get_connection_str(stage=stage)
    session_inst: sessionmaker[Session] = await get_sa_sessionmaker(connection_str=connection_str)
    with session_inst.begin() as session:
        yield session
