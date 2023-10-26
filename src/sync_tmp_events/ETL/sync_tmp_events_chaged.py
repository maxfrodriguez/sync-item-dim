import logging
from typing import Any, Dict, List
from src.sync_tmp_events.ETL.facade_load_abc import LoadFacadeABC
from src.sync_tmp_events.extract.data.shipments_changed import ShipmentsChanged


from src.sync_tmp_events.ETL.facade_extract_abc import ExtractFacadeABC
from src.sync_tmp_events.ETL.facade_transform_abc import TransformFacadeABC


class SyncronizerEventsChaged:
    def __init__(self
                 , extract_service: ExtractFacadeABC
                 , transform_repository: TransformFacadeABC
                 , load_facade : LoadFacadeABC) -> None:
        self.extract_facade = extract_service
        self.transform_facade = transform_repository
        self.load_facade = load_facade

    async def syncronize(self, tmps_changed : List[Dict[str, Any]]) -> None:
        
        # convert the tmps_changed to shipment_changed
        self.shipments_changed = [ShipmentsChanged(**tmp) for tmp in tmps_changed]

        await self.extract_facade.get_events_tmp_changed(tmps_changed)
    
        # sincronize all if has less than 200 events to sync
        for events_to_sync in self.extract_facade.next_events():
            try:
                await self.transform_facade.transform_events_to_sync(events_to_sync)
                
                await self.load_facade.load_events(self.transform_facade.event_to_sync)
                await self.load_facade.delete_events(self.transform_facade.event_to_delete)

            except Exception as e:
                ids = ", ".join(f"'{shipment.ds_id}'" for shipment in events_to_sync)
                logging.error(f"Error in syncronize shipments: {ids} with error: {e}")