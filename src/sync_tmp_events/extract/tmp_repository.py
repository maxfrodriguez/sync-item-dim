from datetime import datetime, timedelta
import logging
from typing import Generator, List

from common.common_infrastructure.cross_cutting.environment import ENVIRONMENT
from common.common_infrastructure.dataaccess.db_profit_tools_access.pt_anywhere_client import PTSQLAnywhere

from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SALoaderLog
from src.infrastructure.data_access.db_ware_house_access.whdb_anywhere_client import WareHouseDbConnector
from src.infrastructure.data_access.sybase.sql_anywhere_impl import Record

from src.sync_tmp_events.extract.data.modlog import ModLog
from src.sync_tmp_events.extract.data.shipment import Shipment
from src.sync_tmp_events.extract.tmp_repository_abc import TmpRepositoryABC
from src.sync_tmp_events.extract.queries.query_tmp_pt import MODLOG_QUERY, SHIPMENT_EQUIPMENT_SPLITTED_QUERY, SHIPMENT_SPLITTED_QUERY, SHIPMENTS_CUSTOM_FIELDS_QUERY


class TmpRepository(TmpRepositoryABC):

    def __init__(self, stage: ENVIRONMENT = ENVIRONMENT.PRD) -> None:
        self.wh_repository = WareHouseDbConnector(stage=stage)
        self.pt_repository = PTSQLAnywhere(stage=stage)
        self.tmps_to_sync: List[Shipment] = []
        self.mod_datetime: datetime = datetime.utcnow() - timedelta(days=15)
        self.mod_datetime = self.mod_datetime.replace(hour=0, minute=0, second=0, microsecond=0)

    async def __aenter__(self):
        self.pt_repository.__enter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.pt_repository.__exit__(exc_type, exc_val, exc_tb)

    def next_shipments(self, pack_size=25):
        if not pack_size:
            pack_size = 25

        pack_size = int(pack_size) if pack_size else 25

        for i in range(0, len(self.tmps_to_sync), pack_size):
            yield self.tmps_to_sync[i:i + pack_size]

    async def get_tmp_changed(self) -> None:
        result = await self._get_tmp_to_sync()

        if not result:
            return
        
        shipment_ids = set(shipment['ds_id'] for shipment in result)
        ids = ", ".join(f"'{shipment}'" for shipment in shipment_ids)

        raw_data: Generator[Record, None, None] = self.pt_repository.SELECT(
            SHIPMENT_SPLITTED_QUERY.format(ids), result_type=dict
        )
        
        #I want to find the shipments ds_id dulpicated in the result to loggin in and review in the future.
        set_shipment_ids = set()
        for raw_row_shipment in raw_data:
            try:
                if raw_row_shipment['ds_id'] in set_shipment_ids:
                    logging.error(f"shipment duplicated in the result: {raw_row_shipment}")
                else:
                    set_shipment_ids.add(raw_row_shipment['ds_id'])
                    tmp_to_sync = Shipment(**raw_row_shipment)
                    mod_log_match = [eq_row for eq_row in result if eq_row["ds_id"] == raw_row_shipment['ds_id']]
                    if mod_log_match:
                        tmp_to_sync.id = mod_log_match[0]['ship_mod_id']
                    self.tmps_to_sync.append(tmp_to_sync)        
            except Exception as e:
                logging.error(f"Error loading this tmp: {raw_row_shipment['ds_id']} to sync: {e}")


        #order by asc by id the tmps_to_sync
        self.tmps_to_sync.sort(key=lambda x: x.id)

    async def _get_tmp_to_sync(self):
        async with self.wh_repository:
            latest_mod_log = await SALoaderLog.get_highest_version(self.wh_repository)
            mod_log = ModLog(
                mod_lowest_version=latest_mod_log.mod_lowest_version,
                mod_highest_version=latest_mod_log.mod_highest_version,
                num_fact_movements_loaded=latest_mod_log.num_fact_movements_loaded,
                created_at=latest_mod_log.created_at,
            )

        if not mod_log:
            return
        result: Generator[Record, None, None] = self.pt_repository.SELECT(
            MODLOG_QUERY.format(
                mod_log.mod_highest_version
                #, self.mod_datetime
            )
        )
        
        return result

        

    def complement_with_equipment_info(self, list_shipments: List[Shipment]) -> None:

        ids = ", ".join(f"'{shipment.ds_id}'" for shipment in list_shipments)
        raws_equipment: Generator[Record, None, None] = self.pt_repository.SELECT(
                SHIPMENT_EQUIPMENT_SPLITTED_QUERY.format(ids), result_type=dict
            )
        
        for tmp_to_complement in list_shipments:
            matching_rows = [
                    eq_row for eq_row in raws_equipment if eq_row["ds_id"] == tmp_to_complement.ds_id
                ]
            for equipment in matching_rows:
                if equipment.get('eq_type') == 'C' and equipment.get('Line') is not None and equipment.get('Type') is not None:
                    tmp_to_complement.eq_c_info_eq_type = equipment.get('eq_type')    
                    tmp_to_complement.eq_c_info_line = equipment.get('Line')          
                    tmp_to_complement.eq_c_info_type = equipment['Type']              
                    tmp_to_complement.container_id = equipment['eq_ref']              
                    
                elif equipment.get('eq_type') == 'H':
                        #result_dict[ds_id]['ds_id'] = ds_id
                    tmp_to_complement.eq_h_info_eq_type = equipment.get('eq_type')
                    tmp_to_complement.eq_h_info_line = equipment.get('Line')
                    tmp_to_complement.eq_h_info_type = equipment['Type']
                    tmp_to_complement.chassis_id = equipment['eq_ref']
        
    async def get_custom_fields(self, list_shipments: List[Shipment]) -> None:
        ids = ", ".join(f"'{shipment.ds_id}'" for shipment in list_shipments)
        raws_custom_fields: Generator[Record, None, None] = self.pt_repository.SELECT(
                SHIPMENTS_CUSTOM_FIELDS_QUERY.format(ids), result_type=dict
            )
        
        custom_fields = []
        for custom_field in raws_custom_fields:
            tmp = [shipment for shipment in self.tmps_to_sync if shipment.ds_id == custom_field['ds_id']]
            if tmp:
                custom_field['sk_id_shipment_fk'] = tmp[0].id
                custom_fields.append(custom_field)

        return custom_fields
