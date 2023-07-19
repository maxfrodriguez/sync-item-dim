import logging
from datetime import datetime
from typing import Any, Dict, Generator, List

from src.domain.entities.event import Event
from src.domain.entities.shipment import Shipment
from src.domain.repository.events_abc import EventRepositoryABC
from src.infrastructure.cross_cutting.environment import ENVIRONMENT
from src.infrastructure.cross_cutting.hasher import deep_hash
from src.infrastructure.cross_cutting.service_bus.service_bus_impl import ServiceBusImpl
from src.infrastructure.data_access.db_profit_tools_access.pt_anywhere_client import PTSQLAnywhere
from src.infrastructure.data_access.db_profit_tools_access.queries.queries import COMPLETE_EVENT_QUERY, NEXT_ID_WH, WAREHOUSE_EVENTS
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SAEvent, SAShipmentEvent
from src.infrastructure.data_access.db_ware_house_access.whdb_anywhere_client import WareHouseDbConnector
from src.infrastructure.data_access.sybase.sql_anywhere_impl import Record
from src.infrastructure.repository.recalculate_movements_impl import RecalculateMovementsImpl


class EventImpl(EventRepositoryABC):
    async def send_list_sb(self, id_list: List[int]) -> None:
        id_set = set(id_list)
        for unique_id in id_set:
            await self.send_sb_message(unique_id)

    async def send_sb_message(self, id: int, stage: ENVIRONMENT = ENVIRONMENT.PRD) -> None:
        async with ServiceBusImpl(stage=stage) as servicebus_client:
            try:
                await servicebus_client.send_message(data=id, queue_name=servicebus_client.queue_name)
            except Exception as e:
                logging.exception(f"An exception occurred while sending the message: {str(e)}")

    async def validate_event_dates(self, rows: Generator) -> Generator[Dict | Record, None, None]:
        for row in rows:
            for field in ["appointment", "arrival", "departure", "earliest", "latest"]:
                dt = row[f"de_{field}_dt"]
                tm = row[f"de_{field}_tm"]
                
                if dt and tm:
                    row[f'de_{field}'] = dt + ' ' + tm
                else:
                    row[f'de_{field}'] = dt + ' 00:00:00.000' if dt else None
                
                del row[f"de_{field}_dt"]
                del row[f"de_{field}_tm"]

        return rows 

    async def bulk_save_events(self, bulk_of_events: List[SAEvent]) -> None:
        async with WareHouseDbConnector(stage=ENVIRONMENT.PRD) as wh_client:
            wh_client.bulk_copy(bulk_of_events)
    
    async def bulk_save_event_shipments(self, bulk_of_event_ships: List[SAShipmentEvent]) -> None:
        async with WareHouseDbConnector(stage=ENVIRONMENT.PRD) as wh_client:
            wh_client.bulk_copy(bulk_of_event_ships)

    async def save_and_sync_events(self, list_of_shipments: List[Shipment]):
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
        bulk_events: List[SAEvent] = []
        bulk_of_ship_events : List[SAEvent] = []
        shipments_id_list = []

        event_ids = ", ".join(f"'{event['de_id']}'" for event in rows)

        # New logic to CAST Dates in Events
        rows = await self.validate_event_dates(rows=rows)

        async with WareHouseDbConnector(stage=ENVIRONMENT.PRD) as wh_client:
            row_next_id = wh_client.execute_select(NEXT_ID_WH.format("events"))
            row_event_ship_id = wh_client.execute_select(NEXT_ID_WH.format("shipments_events"))
            wh_events: List[Dict[str, Any]] = wh_client.execute_select(WAREHOUSE_EVENTS.format(event_ids))
        
        if wh_events:
            events_hash_list = {wh_event['de_id']: wh_event['hash'] for wh_event in wh_events}
            event_id_list = {wh_event['de_id']: wh_event['id'] for wh_event in wh_events}

        assert row_next_id, f"Did't not found next Id for ''Events WH'' at {datetime.now()}"
        assert row_event_ship_id, f"Did't not found next Id for ''Shipments_Events WH'' at {datetime.now()}"

        next_id = row_next_id[0]["NextId"] if row_next_id[0]["NextId"] is not None else 0
        event_ship_next_id = row_event_ship_id[0]["NextId"] if row_event_ship_id[0]["NextId"] is not None else 0


        # read shipments_query one by one
        for row_query in rows:
           
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
                        # WH existing event, adds logic to create relation in Shipment_Event
                        sa_event_ship: SAShipmentEvent = SAShipmentEvent(
                            sk_shipment_id = current_shipment.id,
                            sk_event_id = current_event.id,
                            sequence_event_id = current_event.de_ship_seq,
                            created_at = datetime.utcnow().replace(second=0, microsecond=0),
                            id = event_ship_next_id
                        )
                        bulk_of_ship_events.append(sa_event_ship)
                        shipments_id_list.append(current_shipment.ds_id)
                        event_ship_next_id += 1
                        continue

                    row_query.pop("ds_id", None)
                    # New logic for appointment/arrival date
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

                    bulk_events.append(new_event)
                    shipments_id_list.append(current_shipment.ds_id)
                    next_id += 1

                    # WH non existing Event
                    sa_event_ship: SAShipmentEvent = SAShipmentEvent(
                         sk_shipment_id = current_shipment.id,
                        sk_event_id = new_event.id,
                        sequence_event_id = new_event.de_ship_seq,
                        created_at = datetime.utcnow().replace(second=0, microsecond=0),
                        id = event_ship_next_id
                    )
                    bulk_of_ship_events.append(sa_event_ship)
                    event_ship_next_id += 1
                else:
                    logging.warning(f"Did't find the shipment: {event_id}")
            
        await self.bulk_save_events(bulk_events)
        await self.bulk_save_event_shipments(bulk_of_ship_events)
        await self.send_list_sb(shipments_id_list)
