from dataclasses import asdict
import logging
from typing import Any, Dict, List
from common.common_infrastructure.cross_cutting.hasher import deep_hash


from src.infrastructure.data_access.db_ware_house_access.whdb_anywhere_client import WareHouseDbConnector
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SAEvent, SAFactEvent
from src.sync_tmp_events.extract.data.EventPTDTO import EventPTDTO
from src.sync_tmp_events.extract.queries.query_event_pt import WAREHOUSE_EVENTS
from src.sync_tmp_events.load.data.dim_event_adapter import DimEventAdapter
from src.sync_tmp_events.ETL.facade_transform_abc import TransformFacadeABC


class TransformFacade(TransformFacadeABC):
    def __init__(self) -> None:
        self.wh_repository = WareHouseDbConnector()
        self.event_to_sync: List[EventPTDTO] = []
        self.event_to_delete: List[Dict[str, Any]] = []


    async def transform_events_to_sync(self, list_events: List[EventPTDTO]) -> None:
        de_ids = ", ".join(f"'{shipment.de_id}'" for shipment in list_events)

        async with self.wh_repository:
            wh_events: List[Dict[str, Any]] = self.wh_repository.execute_select(
                WAREHOUSE_EVENTS.format(de_ids)
            )

        #create a list of events to sync by shipment ds_id like a key
        shipment_events: Dict[int, List[EventPTDTO]] = {}
        for event in list_events:
            if event.ds_id not in shipment_events:
                shipment_events[event.ds_id] = []
            shipment_events[event.ds_id].append(event)

        for ds_id in shipment_events.keys():
            wh_events_by_shipment_rows = [
                eq_row for eq_row in wh_events if eq_row["ds_id"] == ds_id
            ]

            #add all the events if the shipment is new
            if len(wh_events_by_shipment_rows) == 0:
                for event in shipment_events[ds_id]:
                    event.calculate_datetime_fields()
                    self.event_to_sync.append(event)

            else:
                for event in shipment_events[ds_id]:
                    wh_event = next((ev for ev in wh_events_by_shipment_rows if ev["de_id"] == event.de_id), None)

                    event.hash = deep_hash(event)
                    if wh_event and wh_event["hash"] and int(wh_event["hash"]) == event.hash:
                        wh_events_by_shipment_rows.remove(wh_event)
                        continue
                    try:
                        event.calculate_datetime_fields()
                        self.event_to_sync.append(event)
                        wh_events_by_shipment_rows.remove(wh_event)
                    except Exception as e:
                        logging.error(f"Error sincronizing shipment {event.ds_id}: {e}")

                if wh_events_by_shipment_rows:
                    self.event_to_delete.extend(wh_events_by_shipment_rows)