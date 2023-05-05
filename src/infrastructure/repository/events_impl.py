import logging
from datetime import datetime
from typing import Generator, List

from src.domain.entities.event import Event
from src.domain.entities.shipment import Shipment
from src.domain.repository.events_abc import EventRepositoryABC
from src.infrastructure.cross_cutting.environment import ENVIRONMENT
from src.infrastructure.data_access.db_profit_tools_access.pt_anywhere_client import PTSQLAnywhere
from src.infrastructure.data_access.db_profit_tools_access.queries.queries import COMPLETE_EVENT_QUERY, NEXT_ID_WH
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SAEvent
from src.infrastructure.data_access.db_ware_house_access.whdb_anywhere_client import WareHouseDbConnector
from src.infrastructure.data_access.sybase.sql_anywhere_impl import Record


class EventImpl(EventRepositoryABC):
    async def save_and_sync_events(self, list_of_shipments: List[Shipment]):
        ids = ", ".join(f"'{shipment.ds_id}'" for shipment in list_of_shipments)

        async with PTSQLAnywhere(stage=ENVIRONMENT.UAT) as sybase_client:
            rows: Generator[Record, None, None] = sybase_client.SELECT(
                COMPLETE_EVENT_QUERY.format(ids), result_type=dict
            )

        # if rows is empty, raise the exeption to close the process because we need to loggin just in application layer
        assert rows, f"don't not found shipments to sync at {datetime.now()}"

        # declare a set list to store the RateConfShipment objects
        unique_event = set()
        # create a list of rateconf_shipment objects
        # bulk_shipments : List[SAShipment] = []
        bulk_events: List[SAEvent] = []

        async with WareHouseDbConnector(stage=ENVIRONMENT.UAT) as wh_client:
            row_next_id = wh_client.execute_select(NEXT_ID_WH.format("events"))

        assert row_next_id, f"Don't not found next Id for ''Events WH'' at {datetime.now()}"

        next_id = row_next_id[0]["NextId"]

        # read shipments_query one by one
        for row_query in rows:
            # create a KeyRateConfShipment object to store the data from the shipment
            unique_key_shipment = row_query["ds_id"]
            unique_key_event = row_query["de_id"]

            # # validate if the unique_rateconf_key is not in the set list to avoid duplicates of the same RateConfShipment
            # if unique_key_shipment not in unique_shipment:
            if unique_key_event not in unique_event:
                # add the shipment_obj to the set list
                unique_event.add(unique_key_event)

                current_shipment = [
                    shipment for shipment in list_of_shipments if shipment.ds_id == unique_key_shipment
                ][0]

                if current_shipment:
                    row_query.pop("ds_id", None)
                    new_event: SAEvent = SAEvent(**row_query)
                    new_event.shipment_id = current_shipment.id
                    new_event.id = next_id
                    current_event = Event(**row_query)
                    current_event.id = next_id
                    current_shipment.events.append(current_event)

                    bulk_events.append(new_event)
                    next_id += 1
                else:
                    logging.warning(f"Don't find the shipment: {unique_key_event}")

        # bulk copy de bulk_shipments
        async with WareHouseDbConnector(stage=ENVIRONMENT.UAT) as wh_client:
            wh_client.bulk_copy(bulk_events)
