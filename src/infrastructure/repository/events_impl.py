import logging
from datetime import datetime
from typing import Any, Dict, Generator, List

from src.domain.entities.event import Event
from src.domain.entities.shipment import Shipment
from src.domain.repository.events_abc import EventRepositoryABC
from src.infrastructure.cross_cutting.environment import ENVIRONMENT
from src.infrastructure.cross_cutting.hasher import deep_hash
from src.infrastructure.data_access.db_profit_tools_access.pt_anywhere_client import PTSQLAnywhere
from src.infrastructure.data_access.db_profit_tools_access.queries.queries import COMPLETE_EVENT_QUERY, NEXT_ID_WH, WAREHOUSE_EVENTS
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SAEvent
from src.infrastructure.data_access.db_ware_house_access.whdb_anywhere_client import WareHouseDbConnector
from src.infrastructure.data_access.sybase.sql_anywhere_impl import Record
from src.infrastructure.repository.recalculate_movements_impl import RecalculateMovementsImpl


class EventImpl(EventRepositoryABC):
    async def save_and_sync_events(self, list_of_shipments: List[Shipment], recalculate_movements_repository: RecalculateMovementsImpl):
        events_hash_list = {}
        ids = ", ".join(f"'{shipment.ds_id}'" for shipment in list_of_shipments)

        async with PTSQLAnywhere(stage=ENVIRONMENT.PRD) as sybase_client:
            rows: Generator[Record, None, None] = sybase_client.SELECT(
                COMPLETE_EVENT_QUERY.format(ids), result_type=dict
            )

        # if rows is empty, raise the exeption to close the process because we need to loggin just in application layer
        assert rows, f"don't not found shipments to sync at {datetime.now()}"

        # declare a set list to store the RateConfShipment objects
        unique_events_ids = set()
        # create a list of rateconf_shipment objects
        # bulk_shipments : List[SAShipment] = []
        bulk_events: List[SAEvent] = []

        event_ids = ", ".join(f"'{event['de_id']}'" for event in rows)

        async with WareHouseDbConnector(stage=ENVIRONMENT.PRD) as wh_client:
            row_next_id = wh_client.execute_select(NEXT_ID_WH.format("events"))
            wh_events: List[Dict[str, Any]] = wh_client.execute_select(WAREHOUSE_EVENTS.format(event_ids))
        
        if wh_events:
            events_hash_list = {wh_event['de_id']: wh_event['hash'] for wh_event in wh_events}
            event_id_list = {wh_event['de_id']: wh_event['id'] for wh_event in wh_events}

        assert row_next_id, f"Did't not found next Id for ''Events WH'' at {datetime.now()}"

        next_id = row_next_id[0]["NextId"] if row_next_id[0]["NextId"] is not None else 0

        # read shipments_query one by one
        for row_query in rows:
            # create a KeyRateConfShipment object to store the data from the shipment
            event_hash = deep_hash(row_query)
            shipment_id = row_query["ds_id"]
            event_id = row_query["de_id"]

            # # validate if the unique_rateconf_key is not in the set list to avoid duplicates of the same RateConfShipment
            # if shipment_id not in unique_shipment:
            if event_id not in unique_events_ids:
                # add the shipment_obj to the set list
                unique_events_ids.add(event_id)

                current_shipment = [
                    shipment for shipment in list_of_shipments if shipment.ds_id == shipment_id
                ][0]

                
                if current_shipment:
                    # Validate event hash
                    if (event_hash and event_id in events_hash_list and events_hash_list[event_id]) and str(event_hash) == events_hash_list[event_id]:
                        row_query.pop("ds_id", None)
                        current_event = Event(**row_query)
                        current_event.id = int(event_id_list[event_id])
                        current_shipment.events.append(current_event)
                        continue


                    row_query.pop("ds_id", None)
                    new_event: SAEvent = SAEvent(**row_query)
                    new_event.shipment_id = current_shipment.id
                    new_event.id = next_id
                    new_event.hash = event_hash
                    new_event.created_at = datetime.utcnow().replace(second=0, microsecond=0)
                    current_event = Event(**row_query)
                    current_event.id = next_id
                    current_shipment.events.append(current_event)

                    # Sends shipment information to recalculate movements
                    current_shipment.has_changed_events = True
                    recalculate_movements_repository.recalculate_movements(shipment=current_shipment)

                    bulk_events.append(new_event)
                    next_id += 1
                else:
                    logging.warning(f"Did't find the shipment: {event_id}")

        # bulk copy de bulk_shipments
            
        async with WareHouseDbConnector(stage=ENVIRONMENT.PRD) as wh_client:
            wh_client.bulk_copy(bulk_events)
