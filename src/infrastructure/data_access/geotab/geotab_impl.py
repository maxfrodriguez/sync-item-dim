import logging
import time as timer
from asyncio import gather
from typing import Any, Dict, List, Union

from mygeotab import API as GeoTabAPI
from mygeotab import MyGeotabException, TimeoutException
from typing_extensions import Final, Self

from src.infrastructure.cross_cutting import ENVIRONMENT, KeyVaultImpl

from . import GeotabHelper
from .geotab_abc import GeotabABC

NUM_ALLOWED_FAILED_QUERIES: Final = 5
ALLOWED_ERROR_MESSAGES: List[str] = ["API calls quota exceeded. Maximum admitted 10 per 1m"]


async def get_geotab_client(stage: ENVIRONMENT) -> GeoTabAPI:
    username: str | None = None
    password: str | None = None
    database: str | None = None

    async with KeyVaultImpl(stage) as kv:
        username, password, database = await gather(
            kv.get_secret("GEOTAB-USER"), kv.get_secret("GEOTAB-PASSWORD"), kv.get_secret("GEOTAB-DATABASE")
        )

    if None in {username, password, database}:
        raise ValueError("None value in credentials")

    client: GeoTabAPI = GeotabHelper.autenticate_geotab(username=username, password=password, database=database)

    return client


class GeotabImpl(GeotabABC):
    def __init__(self, stage: ENVIRONMENT = ENVIRONMENT.PRD) -> None:
        self.__environment: ENVIRONMENT = stage
        self.__client: GeoTabAPI = None

    async def auth(self) -> None:
        await get_geotab_client(self.__environment)

    async def make_async_request(self, entity_type: str, parameters: Dict[str, Any], method: str = "Get"):
        exception: Union[Exception, None] = None
        for _ in range(NUM_ALLOWED_FAILED_QUERIES):
            try:
                if parameters:
                    results_limit: Union[int, None] = parameters.get("resultsLimit", None)
                    if results_limit is not None:
                        del parameters["resultsLimit"]
                    parameters = {"search": parameters["search"], "resultsLimit": results_limit}
                return await self.__client.call_async(method, type_name=entity_type, **parameters)
            except (TimeoutException, MyGeotabException) as e:
                exc_str: str = str(e)
                for allowed_msg in ALLOWED_ERROR_MESSAGES:
                    if allowed_msg not in exc_str:
                        raise
                logging.warning(f"Failed Geotab query, waiting 60 seconds to try again. Exception: {exc_str}")
                timer.sleep(60)
                exception = e
            except Exception as e:
                exc_str: str = str(e)
                logging.warning(f"Raise an Exception in make_async_request method: {exc_str}")
                raise
        logging.warning(f"Geotab query failed {NUM_ALLOWED_FAILED_QUERIES} in a row. Giving up...")
        if exception:
            raise exception

    async def __aenter__(self) -> Self:
        self.__client = await self.auth()
        return self

    async def __aexit__(self, *_) -> None:
        pass
