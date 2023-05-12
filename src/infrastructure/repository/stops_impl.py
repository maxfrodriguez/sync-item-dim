import logging
from datetime import datetime
from typing import Any, Dict, List

from orjson import dumps, loads

from src.domain.entities.shipment import Shipment
from src.domain.entities.stop import Stop
from src.domain.repository.stops_abc import StopsRepositoryABC
from src.infrastructure.cross_cutting.environment import ENVIRONMENT
from src.infrastructure.cross_cutting.hasher import deep_hash
from src.infrastructure.data_access.db_121tower_access.tower121_anywhere_client import Tower121DdConnector
from src.infrastructure.data_access.db_profit_tools_access.queries.queries import NEXT_ID_WH, STOPS_QUERY, WAREHOUSE_STOPS
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SAStops
from src.infrastructure.data_access.db_ware_house_access.whdb_anywhere_client import WareHouseDbConnector


class StopsImpl(StopsRepositoryABC):
    async def get_stop_by_id(self, events: List[int]):
        ids = ", ".join(f"'{event_id}'" for event_id in events)
        async with Tower121DdConnector(stage=ENVIRONMENT.UAT) as tower_121_client:
            result = await tower_121_client.execute_select(STOPS_QUERY.format(ids))
            if result:
                stops: List[Stop] = Stop.create_stops(result)
                return stops

    async def save_stops(self, stops: List[Stop], session) -> None:
        for stop in stops:
            try:
                raw_stop: Dict[str, Any] = loads(dumps(stop))
                new_stop: SAStops = SAStops(**raw_stop)
                new_stop.save(db_session=session)
            except Exception as e:
                logging.error(f"Error in save_stops: {e}")

    async def save_and_sync_stops(self, list_of_shipments: List[Shipment]):
        ids = ", ".join(f"'{shipment.ds_id}'" for shipment in list_of_shipments)

        async with Tower121DdConnector(stage=ENVIRONMENT.UAT) as tower_121_client:
            rows = tower_121_client.execute_select(STOPS_QUERY.format(ids))

        # if rows is empty, raise the exeption to close the process because we need to loggin just in application layer
        assert rows, f"did't not found stops to sync at {datetime.now()}"

        # declare a set list to store the RateConfShipment objects
        unique_event = set()
        # create a list of rateconf_shipment objects
        # bulk_shipments : List[SAShipment] = []
        bulk_stops: List[SAStops] = []
        event_ids = ", ".join(f"'{stop['pt_event_id']}'" for stop in rows)

        async with WareHouseDbConnector(stage=ENVIRONMENT.UAT) as wh_client:
            row_next_id = wh_client.execute_select(NEXT_ID_WH.format("stops"))
            wh_stops: List[Dict[str, Any]] = wh_client.execute_select(WAREHOUSE_STOPS.format(event_ids))

        stops_hash_list = {wh_stop['pt_event_id']: wh_stop['hash'] for wh_stop in wh_stops}

        assert row_next_id, f"Did't not found next Id for ''Stops WH'' at {datetime.now()}"

        next_id = row_next_id[0]["NextId"]
        if next_id is None:
            next_id = 0

        # read shipments_query one by one
        for row_query in rows:
            # create a KeyRateConfShipment object to store the data from the shipment
            # unique_key_shipment = row_query["tmp"]
            stop_hash = deep_hash(row_query)
            unique_key_event = int(row_query["pt_event_id"])

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
                    # Validate stop hash
                    if (stop_hash and unique_key_event in stops_hash_list and stops_hash_list[unique_key_event]) and str(stop_hash) == stops_hash_list[unique_key_event]:
                        continue

                    # row_query.pop("ds_id", None)
                    new_stop: SAStops = SAStops(**row_query)
                    new_stop.event_id = current_event.id
                    new_stop.id = next_id
                    new_stop.hash = stop_hash
                    new_stop.created_at = datetime.utcnow().replace(second=0, microsecond=0)
                    current_stop = Stop(**row_query)
                    current_stop.id = next_id
                    current_event.stop = current_stop
                    
                    bulk_stops.append(new_stop)
                    next_id += 1
                else:
                    logging.warning(f"Did't find the shipment: {unique_key_event}")

        # Bulk insert Stops.
        async with WareHouseDbConnector(stage=ENVIRONMENT.UAT) as wh_client:
            wh_client.bulk_copy(bulk_stops)
