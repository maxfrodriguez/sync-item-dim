from datetime import datetime
from typing import Any, Dict, Generator, List, Literal, List
from src.domain.entities.shipment import Shipment
from src.domain.repository.custom_field_abc import CustomFieldABC
from src.infrastructure.cross_cutting.environment import ENVIRONMENT
from src.infrastructure.data_access.db_profit_tools_access.queries.queries import SHIPMENTS_CUSTOM_FIELDS_QUERY
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SACustomFields
from src.infrastructure.data_access.db_ware_house_access.whdb_anywhere_client import WareHouseDbConnector
from src.infrastructure.data_access.sybase.sql_anywhere_impl import Record
from src.infrastructure.data_access.db_profit_tools_access.pt_anywhere_client import PTSQLAnywhere


class CustomFieldImpl(CustomFieldABC):
    async def get_custom_fields(self, shipments: List[Shipment]):

        ids = ", ".join(f"'{shipment.ds_id}'" for shipment in shipments)

        async with PTSQLAnywhere(stage=ENVIRONMENT.PRD) as sybase_client:
            raw: Generator[Record, None, None] = sybase_client.SELECT(
                SHIPMENTS_CUSTOM_FIELDS_QUERY.format(ids), result_type=dict
            )
        return raw

    async def save_custom_fields(self, custom_field_sa_list: List[SACustomFields]):
        async with WareHouseDbConnector(stage=ENVIRONMENT.PRD) as wh_client:
            wh_client.bulk_copy(custom_field_sa_list)