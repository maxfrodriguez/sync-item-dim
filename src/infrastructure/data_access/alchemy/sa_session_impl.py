import logging
from asyncio import gather
from contextlib import asynccontextmanager
from datetime import datetime
from json import loads
from typing import Any, Dict, Generator, List, Optional, Type, Union
from urllib.parse import quote
from os import getenv

from async_lru import alru_cache
from sqlalchemy import Select, create_engine, delete, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from common.common_infrastructure.cross_cutting.ConfigurationEnvHelper import ConfigurationEnvHelper


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
        self, keyVaults: dict[str, str] = None, passEncrypt: bool = False):
        self._passEncrypt = passEncrypt
        self._keyVaultParams = keyVaults

    async def _get_credentials(self) -> None:
        if self._secrets is None:
            self._secrets = {}
            
        self._secrets = ConfigurationEnvHelper().get_secrets(self._keyVaultParams)

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
        except Exception as e:
            logging.error(f"Error executing query: {query}, with error: {e}")

    async def execute_statement(self, statement: Select) -> Any:
        try:
            if isinstance(statement, Select):
                result_proxy = self._session.execute(statement)
                result = result_proxy.scalars().first()
                return result
            elif isinstance(statement, str):
                result_proxy = self._session.execute(text(statement))
            else:
                result_proxy = self._session.execute(statement)
        except Exception as e:
            logging.error(f"Error executing query: {statement}")
            raise e

    def bulk_copy(self, objects: List[Any]) -> None:
        try:
            with self._session.begin():
                self._session.add_all(objects)
                self._session.flush()
                self._session.commit()

        except Exception as e:
            logging.error(f"Error: {e} executing the bulk copy at: {datetime.now()}")
            raise e
            
    
    def save_object(self, object: Any) -> None:
        try:
            self._session.add(object)
            self._session.flush()
            self._session.commit()

        except Exception as e:
            logging.error(f"Error executing the bulk copy at: {datetime.now()}")
            raise e
            

    def upsert_data(self, model_instances: List[Any]) -> None:
        try:
            for object in model_instances:
                self._session.merge(object)

            self._session.flush()
            self._session.commit()


        except Exception as e:
            self._session.rollback()
            logging.error(f"upsert_data error: {e}")
            raise e

    def upsert_bulk_data(self, model_instances: List[Any]) -> None:
        try:
            for object in model_instances:
                self._session.execute(delete(type(object)).where(type(object).ds_id == object.ds_id))
            
            for object in model_instances:
                self._session.merge(object)

            self._session.flush()
            self._session.commit()


        except Exception as e:
            self._session.rollback()
            logging.error(f"upsert_bulk_data error: {e}")

    
    def upsert_bulk_events(self, model_instances: List[Any]) -> None:
        try:
            for object in model_instances:
                self._session.execute(delete(type(object)).where(type(object).shipment_id == object.shipment_id))
            
            for object in model_instances:
                self._session.merge(object)

            self._session.flush()
            self._session.commit()


        except Exception as e:
            self._session.rollback()
            logging.error(f"upsert_bulk_data error: {e}")


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
