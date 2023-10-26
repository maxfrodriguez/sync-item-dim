from dataclasses import asdict
from datetime import datetime
import logging
from typing import Any, Dict, List


from src.infrastructure.data_access.db_ware_house_access.whdb_anywhere_client import WareHouseDbConnector
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SAEvent, SAFactEvent
from src.sync_tmp_events.ETL.facade_load_abc import LoadFacadeABC
from src.sync_tmp_events.extract.data.EventPTDTO import EventPTDTO
from src.sync_tmp_events.load.data.dim_event_adapter import DimEventAdapter
from src.sync_tmp_events.load.data.dim_fact_event_adapter import DimFactEventAdapter
from src.sync_tmp_events.load.query.query_event_wh import WAREHOUSE_DELETE_EVENTS


class LoadFacade(LoadFacadeABC):
    def __init__(self) -> None:
        self.wh_repository = WareHouseDbConnector()

    async def load_events(self, list_events: List[EventPTDTO]):
        bulk_events: List[SAEvent] = []
        bulk_fact_events: List[SAFactEvent] = []
        list_events_to_notify: List[EventPTDTO] = []
         

        async with self.wh_repository:
            for event in list_events:
                try:
                    bulk_events.append(DimEventAdapter(
                        **asdict(event)
                    ))

                    bulk_fact_events.append(DimFactEventAdapter(
                        **asdict(event)
                    ))
 
                    list_events_to_notify.append(event)
                except Exception as e:
                    logging.error(f"Error sincronizing shipment {event.ds_id}: {e}")

            try:
                self.wh_repository.bulk_copy(bulk_events)
                self.wh_repository.upsert_data(model_instances=bulk_fact_events)
            except Exception as e:
                ids = ", ".join(f"'{shipment.ds_id}'" for shipment in list_events)
                logging.error(f"Error in bulk_copy and sync to warehouse the shipments: {ids}, with error: {e}")

        return list_events_to_notify
    
    async def delete_events(self, event_to_delete: List[Dict[str, Any]]) -> None:
        try:
            if len(event_to_delete):
                de_ids = ", ".join(f"{shipment['de_id']}" for shipment in event_to_delete)
                async with self.wh_repository:
                    results = await self.wh_repository.execute_statement(
                        WAREHOUSE_DELETE_EVENTS.format(de_ids)
                    )
            
        except Exception as e:
            logging.error(f"error in save_latest_loader_logs:{e} at {datetime.now()}")