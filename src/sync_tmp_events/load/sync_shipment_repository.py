from dataclasses import asdict
from datetime import datetime
import logging
from typing import Any, Dict, List
from common.common_infrastructure.cross_cutting.environment import ENVIRONMENT
from src.infrastructure.cross_cutting.hasher import deep_hash


from src.infrastructure.data_access.db_ware_house_access.whdb_anywhere_client import WareHouseDbConnector
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SACustomFields, SAFactShipment, SALoaderLog, SAShipment, SATemplate

from src.sync_tmp_events.extract.data.shipment import Shipment
from src.sync_tmp_events.load.data.dim_shipment_adapter import DimShipmentAdapter
from src.sync_tmp_events.load.data.dim_template_adapter import DimTemplateAdapter
from src.sync_tmp_events.load.data.fact_shipment_adapter import FactShipmentAdapter
from src.sync_tmp_events.load.sync_shipment_repository_abc import SyncShipmentRepositoryABC
from src.sync_tmp_events.load.queries.queries import WAREHOUSE_SHIPMENTS


class SyncShipmentRepository(SyncShipmentRepositoryABC):
    def __init__(self, stage: ENVIRONMENT = ENVIRONMENT.PRD) -> None:
        #self.__stage = stage
        self.wh_repository = WareHouseDbConnector(stage=stage)

    async def find_shipments_to_sync(self, list_shipmets: List[Shipment]) -> List[Shipment]:
        shipment_to_sync : List[Shipment] = []
        ids = ", ".join(f"'{shipment.ds_id}'" for shipment in list_shipmets)

        async with self.wh_repository:
            wh_shipments: List[Dict[str, Any]] = self.wh_repository.execute_select(
                WAREHOUSE_SHIPMENTS.format(ids)
            )

            for shipment in list_shipmets:
                matching_rows = [
                    eq_row for eq_row in wh_shipments if eq_row["ds_id"] == shipment.ds_id
                ]

                shipment.hash = deep_hash(shipment)
                if matching_rows and matching_rows[0]["hash"] == shipment.hash:
                    continue
                
                try:
                    shipment_to_sync.append(shipment)
                except Exception as e:
                    logging.error(f"Error sincronizing shipment {shipment.ds_id}: {e}")

        return shipment_to_sync
    
    async def load_shipments(self, list_shipments: List[Shipment], custom_fields: List[dict]):
        bulk_shipments: List[SAShipment] = []
        bulk_templates: List[SATemplate] = []
        bulk_fact_shipments: List[SAFactShipment] = []
        bulk_custom_fields: List[SACustomFields] = []
        list_shipments_to_notify: List[Shipment] = [] 

        async with self.wh_repository:
            for shipment in list_shipments:
                try:
                    if shipment.ds_status == "A":
                        bulk_templates.append(DimTemplateAdapter(**asdict(shipment)))

                    else:
                        bulk_shipments.append(DimShipmentAdapter(
                            **asdict(shipment)
                        ))

                        bulk_fact_shipments.append(FactShipmentAdapter(
                            sk_last_shipment_id=shipment.id
                            , **asdict(shipment)
                        ))
 
                        list_shipments_to_notify.append(shipment)
                except Exception as e:
                    logging.error(f"Error sincronizing shipment {shipment.ds_id}: {e}")

            for custom_field in custom_fields:
                try:
                    bulk_custom_fields.append(SACustomFields(**custom_field))
                except Exception as e:
                    logging.error(f"Error sincronizing custom field {custom_field}: {e}")

            #saving all the data about shipments
            try:
                self.wh_repository.bulk_copy(bulk_shipments)
                self.wh_repository.upsert_data(model_instances=bulk_fact_shipments)
                self.wh_repository.bulk_copy(bulk_custom_fields)
            except Exception as e:
                ids = ", ".join(f"'{shipment.ds_id}'" for shipment in list_shipments)
                logging.error(f"Error in bulk_copy and sync to warehouse the shipments: {ids}, with error: {e}")

            try:
                self.wh_repository.upsert_data(model_instances=bulk_templates)
            except Exception as e:
                ids = ", ".join(f"'{shipment.ds_id}'" for shipment in bulk_templates)
                logging.error(f"Error in bulk_copy and sync to warehouse the templates: {ids}, with error: {e}")


        return list_shipments_to_notify
    
    async def save_latest_loader_logs(
        self, lowest_modlog: int, highest_modlog: int, fact_movements_loaded: int
    ) -> None:
        try:
            new_modlog: SALoaderLog = SALoaderLog(
                mod_lowest_version=lowest_modlog,
                mod_highest_version=highest_modlog,
                num_fact_movements_loaded=fact_movements_loaded,
                created_at= datetime.utcnow().replace(second=0, microsecond=0)
            )

            async with self.wh_repository:
                self.wh_repository.save_object(new_modlog)
            
        except Exception as e:
            logging.error(f"error in save_latest_loader_logs:{e} at {datetime.now()}")