import logging
from datetime import datetime
from json import loads
from typing import Any, Union
from urllib.parse import quote

from sqlalchemy import Insert, Select, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from common.common_infrastructure.cross_cutting import ENVIRONMENT, ConfigurationEnvHelper, KeyVaultImpl


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class AlchemyBase(metaclass=Singleton):
    _instance = None
    _secrets: dict[str, str] = None
    _sessionmaker: sessionmaker[Session] = None
    _connector: str = None
    _keyVaultParams: dict[str, str] = None
    _session: Session = None

    def __init__(
        self, keyVaults: dict[str, str] = None, passEncrypt: bool = False):
        self._passEncrypt = passEncrypt
        self._keyVaultParams = keyVaults

    def _get_credentials(self) -> None:
        if self._secrets is None:
            self._secrets = {}

            self._secrets = ConfigurationEnvHelper().get_secrets(self._keyVaultParams)

    def __get_password(self) -> str:
        password = self._secrets["password"]
        if self._passEncrypt:
            password = quote(password)
        return password

    def _setup_engine(self, alchemyDriverName: str) -> Engine:
        password = self.__get_password()

        # create connection from keyvault attributes
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

    def _get_sqlalchemy_resources(self, alchemyDriverName: str) -> None:
        if self._sessionmaker is None:
            self._get_credentials()
            engine = self._setup_engine(alchemyDriverName=alchemyDriverName)
            self._sessionmaker = sessionmaker(bind=engine)

    def execute_select(self, query: Union[str, Select]) -> list[dict[str, Any]]:
        try:
            if isinstance(query, str):
                result_proxy = self._session.execute(text(query))
            if isinstance(query, Select):
                result_proxy = self._session.execute(query)
            result = result_proxy.fetchall()
            columns = result_proxy.keys()
            return [dict(zip(columns, row)) for row in result]
        except Exception as e:
            logging.error(f"Error executing query: {query}")
            raise e

    def execute_statement(self, query: Select) -> Any:
        try:
            if isinstance(query, Select):
                result_proxy = self._session.execute(query)
                result = result_proxy.scalars().first()
                return result
            else:
                raise ValueError(f"Invalid query type: {type(query)}")
        except Exception as e:
            logging.error(f"Error executing query: {query}")
            raise e

    def bulk_copy(self, objects: list[Any]) -> None:
        try:
            for obj in objects:
                if obj.id == -1:
                    self._session.add(obj)
                else:
                    self._session.merge(obj)
            self._session.flush()
        except Exception as e:
            logging.error(f"Error executing the bulk copy at: {datetime.now()}, with error: {e}")
            raise e

    def save_object(self, object: Any) -> None:
        try:
            result = self._session.add(object)
            self._session.flush()
            return result.lastrowid
        except Exception:
            logging.error(f"Error executing the bulk copy at: {datetime.now()}")

    def execute(self, query, args: dict = None) -> Any:
        try:
            if isinstance(query, Select):
                result_proxy = self._session.execute(query)
                result = [{**row} for row in result_proxy.fetchall()]
                return result
            elif isinstance(query, Insert):
                result = self._session.execute(query, args)
                return result.lastrowid
            else:
                raise ValueError(f"Invalid query type: {type(query)}")
        except Exception:
            logging.error(f"Error executing query: {query}")

    def __enter__(self):
        try:
            if self._sessionmaker is None:
                self.connect()
            self._session = self._sessionmaker()
            return self
        except Exception as e:
            logging.error(f"Error executing connection: {e}")
            raise e

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if exc_value is not None:
            self._session.rollback()
            raise exc_value
        try:
            self._session.commit()
        except Exception as e:
            logging.error(f"Error executing commit: {e}")
            self._session.rollback()
            raise e
        else:
            self._session.expire_all()
            self._session.close()
