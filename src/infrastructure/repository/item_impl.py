from datetime import datetime
from typing import Any, Dict, Generator, List, Literal, List
from src.domain.entities.shipment import Shipment
from src.domain.repository.item_abc import ItemABC
from src.infrastructure.cross_cutting.environment import ENVIRONMENT
from src.infrastructure.data_access.db_profit_tools_access.queries.queries import ITEMS_QUERY
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SAItems
from src.infrastructure.data_access.db_ware_house_access.whdb_anywhere_client import WareHouseDbConnector
from src.infrastructure.data_access.sybase.sql_anywhere_impl import Record
from src.infrastructure.data_access.db_profit_tools_access.pt_anywhere_client import PTSQLAnywhere


class ItemImpl(ItemABC):
    async def get_items(self, shipments: List[Shipment]):
        ids = ", ".join(f"'{shipment.ds_id}'" for shipment in shipments)

        async with PTSQLAnywhere(stage=ENVIRONMENT.PRD) as sybase_client:
            raw: Generator[Record, None, None] = sybase_client.SELECT(
                ITEMS_QUERY.format(ids), result_type=dict
            )
        assert raw, f"Did not find items to sync at {datetime.now()}"

        return raw


    async def save_items(self, item_sa_list: List[SAItems]):
        async with WareHouseDbConnector(stage=ENVIRONMENT.PRD) as wh_client:
            wh_client.bulk_copy(item_sa_list)