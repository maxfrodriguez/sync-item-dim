from typing import Generator, List
from common.common_infrastructure.cross_cutting.ConfigurationEnvHelper import ConfigurationEnvHelper

from common.common_infrastructure.dataaccess.db_context.sybase.sql_anywhere_impl import Record
from common.common_infrastructure.dataaccess.db_profit_tools_access.pt_anywhere_client import PTSQLAnywhere

from src.infrastructure.data_access.db_ware_house_access.whdb_anywhere_client import WareHouseDbConnector
from src.sync_tmp_events.extract.data.EventPTDTO import EventPTDTO

from src.sync_tmp_events.extract.data.shipments_changed import ShipmentsChanged
from src.sync_tmp_events.extract.queries.query_event_pt import COMPLETE_EVENT_QUERY
from src.sync_tmp_events.ETL.facade_extract_abc import ExtractFacadeABC



class ExtractFacade(ExtractFacadeABC):

    def __init__(self) -> None:
        self.wh_repository = WareHouseDbConnector()
        self.pt_repository = PTSQLAnywhere()
        self.tmps_to_sync: List[EventPTDTO] = []

    def next_events(self):
        for i in range(0, len(self.events_tmp_changed), self.pack_size):
            yield self.events_tmp_changed[i:i + self.pack_size]

    async def get_events_tmp_changed(self, tmps_changed : List[ShipmentsChanged]) -> None:
        
        shipment_ids = set(shipment['tmp'] for shipment in tmps_changed)
        ids = ", ".join(f"'{shipment}'" for shipment in shipment_ids)

        with self.pt_repository:
            raw_data: Generator[Record, None, None] = self.pt_repository.SELECT(
                COMPLETE_EVENT_QUERY.format(ids), result_type=dict
            )
        self.events_tmp_changed = [EventPTDTO(**event) for event in raw_data]

        self.pack_size=len(self.events_tmp_changed)        
        if len(self.events_tmp_changed) > 200:
            vault_pack_size=ConfigurationEnvHelper().get_secret("PackageSizeToSync")
            self.pack_size = int(vault_pack_size) if vault_pack_size else 25
