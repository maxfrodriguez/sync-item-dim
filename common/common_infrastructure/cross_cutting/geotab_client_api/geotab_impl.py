import logging
import time as timer
from collections import defaultdict
from datetime import datetime
from typing import Any, Union

from mygeotab import API, AuthenticationException, MyGeotabException, TimeoutException
from typing_extensions import Final, Self
from common.common_infrastructure.cross_cutting import ConfigurationEnvHelper
from common.common_infrastructure.cross_cutting.geotab_client_api.geotab_abc import GeotabABC

NUM_ALLOWED_FAILED_QUERIES: Final = 5
ALLOWED_ERROR_MESSAGES: list[str] = ["API calls quota exceeded. Maximum admitted 10 per 1m"]


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class GeotabImpl(GeotabABC, metaclass=SingletonMeta):
    def __init__(self) -> None:
        self.__client: API = None
        self.devices_by_geotab_trips: dict[str, dict] | None = defaultdict(list)
        self.devices_by_geotab_waypoints: dict[str, dict] | None = defaultdict(list)

    def make_request(self, entity_type: str, parameters: dict[str, Any], method: str = "Get"):
        exception: Union[Exception, None] = None
        for _ in range(NUM_ALLOWED_FAILED_QUERIES):
            try:
                if parameters:
                    results_limit: Union[int, None] = parameters.get("resultsLimit", None)
                    if results_limit is not None:
                        del parameters["resultsLimit"]
                    parameters = {"search": parameters["search"], "resultsLimit": results_limit}
                return self.__client.call(method, type_name=entity_type, **parameters)
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
                logging.warning(
                    f"Raise an Exception in __make_request method: {exc_str}, with parameters {parameters}"
                )

                try:
                    logging.info("Try Again to make request to GeoTab")
                    self.__verify_session()
                    return self.make_request(entity_type=entity_type, parameters=parameters, method=method)
                except Exception as e:
                    logging.exception(f"Raise an Exception in __make_request method: {str(e)}")
                    raise e
        logging.warning(f"Geotab query failed {NUM_ALLOWED_FAILED_QUERIES} in a row. Giving up...")
        if exception:
            raise exception

    def __get_credentials(self) -> None:
        try:
            self._secret: dict[str, str] = {
                "username": "GeotabUser",
                "password": "GeotabPassword",
                "database": "GeotabDatabase"
            }
            ConfigurationEnvHelper().get_secrets(self._secret)

            self.__client: API = API(
                username=self._secret["username"]
                , password=self._secret["password"]
                , database=self._secret["database"])
            
        except AuthenticationException:
            logging.exception("Unsuccessful authentication with the server.")
            raise
        except Exception as e:
            logging.exception(f"Cannot connect to GeoTab with Key Vault. {str(e)}")
            raise

    def __connect(self) -> None:
        try:
            self.__client.authenticate()
        except AuthenticationException as e:
            logging.exception(f"Failed Authentication {str(e.message)}")
        except MyGeotabException as e:
            logging.exception(f"Failed GeoTab {str(e.message)}")
        except TimeoutException as e:
            logging.exception(f"Time out in autenticate {str(e.message)}")

    def __verify_session(self) -> Self:
        self.__connect()
        return self

    def __enter__(self) -> Self:
        if not self.__client:
            self.__get_credentials()
        if self.__client.credentials.session_id is None:
            self.__connect()
        logging.info(f"GeoTab session id: {self.__client.credentials.session_id}")
        return self

    def __exit__(self, *_) -> None:
        # self.__client.close() TODO -> validate if this is necessary or not
        pass

    def get_trips(self, start_date: datetime, end_date: datetime):
        try:
            # trips = geotab.get_trips(start_date, end_date)
            return self.make_request(
                entity_type="Trip",
                method="Get",
                parameters={
                    "search": {
                        "fromDate": start_date,
                        "toDate": end_date,
                    }
                },
            )
        except Exception as e:
            logging.exception(f"Failed to get trips: {start_date} from GeoTab. {str(e)}")

    def get_waypoints(self, start_date: datetime, end_date: datetime, device_id: str = None):
        if device_id:
            parameters = {
                "search": {"fromDate": start_date, "toDate": end_date, "deviceSearch": {"id": device_id}},
            }
        else:
            parameters = {"search": {"fromDate": start_date, "toDate": end_date}}

        return self.make_request(
            entity_type="LogRecord",
            method="Get",
            parameters=parameters,
        )

    def is_device_tractor(self, device_id: str):
        try:
            #### verify if the trip is for a tractor or a chassis
            geotab_device = self.__get_device(device_id=device_id)

            # TODO: add the set of devices reviewed to the cache and

            if geotab_device and "engineVehicleIdentificationNumber" in geotab_device[0]:
                if geotab_device[0]["engineVehicleIdentificationNumber"] == "?":
                    return False

            # if trip["driver"] == "UnknownDriverId":
            #     print(f"The driver who is in truck {geotab_device[0]['name']} is not logged in.")
            #     logging.error(f"The driver who is in truck {geotab_device[0]['name']} is not logged in.")

            return True
        except Exception as e:
            logging.exception(f"Failed to get device: {device_id} from GeoTab. {str(e)}")
            raise e

    def __get_device(self, device_id: str):
        try:
            if device_id:
                parameters = {
                    "search": {"id": device_id},
                }
            else:
                parameters = None

            return self.make_request(
                entity_type="Device",
                method="Get",
                parameters=parameters,
            )
        except Exception as e:
            logging.exception(f"Failed to get device: {device_id} from GeoTab. {str(e)}")
            raise e
