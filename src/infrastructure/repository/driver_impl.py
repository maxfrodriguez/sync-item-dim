

from datetime import datetime
import logging
from typing import Generator, List
from src.domain.entities.driver import Driver
from src.domain.entities.shipment import Shipment
from src.domain.repository.driver_abc import DriverRepositoryABC
from src.infrastructure.cross_cutting.environment import ENVIRONMENT
from src.infrastructure.data_access.db_121tower_access.tower121_anywhere_client import Tower121DdConnector
from src.infrastructure.data_access.db_profit_tools_access.pt_anywhere_client import PTSQLAnywhere
from src.infrastructure.data_access.db_profit_tools_access.queries.queries import NEXT_ID_WH, PT_DRIVERS_QUERY, STOPS_QUERY, WH_DRIVERS_QUERY
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SADrivers
from src.infrastructure.data_access.db_ware_house_access.whdb_anywhere_client import WareHouseDbConnector
from src.infrastructure.data_access.sybase.sql_anywhere_impl import Record


class DriverImpl(DriverRepositoryABC):
    async def get_driver_pt(self, pt_driver_id: int) -> Driver:
        try:
            async with PTSQLAnywhere(stage=ENVIRONMENT.PRD) as sybase_client:
                result: Generator[Record, None, None] = sybase_client.SELECT(
                    PT_DRIVERS_QUERY.format(pt_driver_id)
                )

                if result:
                    new_driver =  Driver(**result)

                return new_driver
            
        except Exception as e:
            logging.error(f"Error in get_driver_pt: {e} at {datetime.now()}")


    async def save_driver(self, pt_driver_id: int) -> None:
        try:
            async with WareHouseDbConnector(stage=ENVIRONMENT.PRD) as wh_client:
                result = wh_client.execute_select(WH_DRIVERS_QUERY.format(pt_driver_id))

                if not result:
                    driver: Driver = await self.get_driver_pt(pt_driver_id=pt_driver_id)
                    if driver:
                        new_sadriver = SADrivers(
                            di_id = driver.di_id,
                            name = driver.name,
                            status = driver.status,
                            fleet = driver.fleet,
                        )

                
            wh_client.save_object(new_sadriver)

        except Exception as e:
            logging.error(f"Error in save_driver: {e} at {datetime.now()}")


    async def save_drivers(self, list_of_shipments: List[Shipment]):
        ids = ", ".join(f"'{shipment.ds_id}'" for shipment in list_of_shipments)

        async with Tower121DdConnector(stage=ENVIRONMENT.PRD) as tower_121_client:
            rows = tower_121_client.execute_select(STOPS_QUERY.format(ids))

        # if rows is empty, raise the exeption to close the process because we need to loggin just in application layer
        assert rows, f"didn't find drivers to sync at {datetime.now()}"

        # declare a set list to store the RateConfShipment objects
        unique_event = set()
        # create a list of rateconf_shipment objects
        # bulk_shipments : List[SAShipment] = []
        bulk_drivers: List[SADrivers] = []

        async with WareHouseDbConnector(stage=ENVIRONMENT.PRD) as wh_client:
            row_next_id = wh_client.execute_select(NEXT_ID_WH.format("drivers"))

        assert row_next_id, f"Did't not found next Id for ''Drivers WH'' at {datetime.now()}"

        next_id = row_next_id[0]["NextId"]
        if next_id is None:
            next_id = 0

        # read shipments_query one by one
        for row_query in rows:
            # create a KeyRateConfShipment object to store the data from the shipment
            unique_key_event = int(row_query["pt_event_id"])
            if not row_query.get("stop_id"):
                continue
            
            unique_key_stopid = int(row_query["stop_id"]) 

            # # validate if the unique_rateconf_key is not in the set list to avoid duplicates of the same RateConfShipment
            # if unique_key_shipment not in unique_shipment:
            if unique_key_event not in unique_event:
                # add the shipment_obj to the set list
                unique_event.add(unique_key_event)

                current_event = next(
                    (
                        event
                        for shipment in list_of_shipments
                        for event in shipment.events
                        if event.de_id == unique_key_event
                    ),
                    None,
                )

                if current_event:
                    # duplicate_stop_counter = Counter(stop["stop_id"] for stop in rows if stop["stop_id"] == unique_key_stopid)

                    # is_duplicate = True if duplicate_stop_counter.get(unique_key_stopid) > 1 else False
                       
                    new_driver: SADrivers = SADrivers(**row_query)
                    new_driver.event_id = current_event.id
                    new_driver.id = next_id
                    current_stop = Driver(**row_query)
                    current_stop.id = next_id
                    current_event.stop = current_stop

                    bulk_drivers.append(new_driver)
                    next_id += 1
                else:
                    logging.warning(f"Did't find the shipment: {unique_key_event}")

        # bulk copy de bulk_shipments
        async with WareHouseDbConnector(stage=ENVIRONMENT.PRD) as wh_client:
            wh_client.bulk_copy(bulk_drivers)