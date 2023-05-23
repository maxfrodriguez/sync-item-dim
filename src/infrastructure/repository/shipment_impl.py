import re
import logging
from datetime import datetime
from typing import Any, Dict, Generator, List

from src.domain.entities.shipment import Event, Shipment
from src.domain.repository.shipment_abc import ShipmentRepositoryABC
from src.infrastructure.cross_cutting.environment import ENVIRONMENT
from src.infrastructure.cross_cutting.hasher import deep_hash
from src.infrastructure.data_access.alchemy.sa_session_impl import get_sa_session
from src.infrastructure.data_access.db_profit_tools_access.pt_anywhere_client import PTSQLAnywhere
from src.infrastructure.data_access.db_profit_tools_access.queries.queries import (
    COMPLETE_EVENT_QUERY,
    COMPLETE_SHIPMENT_QUERY,
    MODLOG_QUERY,
    NEXT_ID_WH,
    WAREHOUSE_SHIPMENTS,
)
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SALoaderLog, SAShipment, SATemplate
from src.infrastructure.data_access.db_ware_house_access.whdb_anywhere_client import WareHouseDbConnector
from src.infrastructure.data_access.sybase.sql_anywhere_impl import Record


class ShipmentImpl(ShipmentRepositoryABC):

    async def get_shipment_by_id(self, shipment: Shipment) -> Shipment:
        async with PTSQLAnywhere(stage=ENVIRONMENT.PRD) as sybase_client:
            result: Generator[Record, None, None] = sybase_client.SELECT(
                COMPLETE_EVENT_QUERY.format(shipment.ds_id), result_type=dict
            )
            if result:
                shipment.events = [Event(**event) for event in result]
            return shipment

    async def retrieve_shipment_list(
        self, last_modlog: int = None, query_to_execute: str = MODLOG_QUERY
    ) -> List[Shipment] | None:
        shipments: List[Shipment] = []
        latest_sa_mod_log: SALoaderLog | None = None
        if last_modlog is None:
            async with get_sa_session() as session:
                latest_sa_mod_log = SALoaderLog.get_highest_version(db_session=session)
            if not latest_sa_mod_log:
                return

        async with PTSQLAnywhere(stage=ENVIRONMENT.PRD) as sybase_client:
            result: Generator[Record, None, None] = sybase_client.SELECT(
                query_to_execute.format(latest_sa_mod_log.mod_highest_version)
            )

            if result:
                try:
                    shipments = [
                        Shipment(ds_id=shipment["ds_id"], modlog=shipment["r_mod_id"], ds_status=shipment["ds_status"])
                        for shipment in result
                    ]
                except Exception as e:
                    logging.error(f"Error in retrieve_shipment_list: {e}")

            return shipments if len(shipments) > 0 else None

    async def save_and_sync_shipment(self, list_of_shipments: List[Shipment]):
        ids = ", ".join(f"'{shipment.ds_id}'" for shipment in list_of_shipments)

        async with PTSQLAnywhere(stage=ENVIRONMENT.PRD) as sybase_client:
            rows: Generator[Record, None, None] = sybase_client.SELECT(
                COMPLETE_SHIPMENT_QUERY.format(ids), result_type=dict
            )

        # if rows is empty, raise the exeption to close the process because we need to loggin just in application layer
        assert rows, f"did't not found shipments to sync at {datetime.now()}"

        # declare a set list to store the RateConfShipment objects
        unique_shipment_ids = set()
        # create a list of rateconf_shipment objects
        bulk_shipments: List[SAShipment] = []
        bulk_templates: List[SATemplate] = []

        async with WareHouseDbConnector(stage=ENVIRONMENT.PRD) as wh_client:
            row_next_id = wh_client.execute_select(NEXT_ID_WH.format("shipments"))
            row_next_id_template = wh_client.execute_select(NEXT_ID_WH.format("templates"))
            # Get List Shipment ID, DS_ID, HASH
            wh_shipments: List[Dict[str, Any]] = wh_client.execute_select(WAREHOUSE_SHIPMENTS.format(ids))

        shipments_hash_list = {wh_shipment['ds_id']: wh_shipment['hash'] for wh_shipment in wh_shipments}
        shipment_id_list = {wh_shipment['ds_id']: wh_shipment['id'] for wh_shipment in wh_shipments}

        assert row_next_id, f"Did't not found next Id for ''Shipments WH'' at {datetime.now()}"
        assert row_next_id_template, f"Did't not found next Id for ''Templates WH'' at {datetime.now()}"


        next_id = row_next_id[0]["NextId"] if row_next_id[0]["NextId"] is not None else 0
        next_id_template = row_next_id_template[0]["NextId"] if row_next_id_template[0]["NextId"] is not None else 0


        # read shipments_query one by one
        for row_query in rows:
            # create a KeyRateConfShipment object to store the data from the shipment
            shipment_hash = deep_hash(row_query)
            shipment_id = row_query["ds_id"]
            # Compare HASH

            # validate if the unique_rateconf_key is not in the set list to avoid duplicates of the same RateConfShipment
            if shipment_id not in unique_shipment_ids:
                # add the shipment_obj to the set list
                unique_shipment_ids.add(shipment_id)

                filtered_shipments = [shipment for shipment in list_of_shipments if shipment.ds_id == shipment_id]

                if filtered_shipments:
                    # get the shipment to update with the id to be inserted
                    filtered_shipment = filtered_shipments[0]
                    # Comparamos Hashes
                    if (shipment_hash and shipment_id in shipments_hash_list and shipments_hash_list[shipment_id]) and str(shipment_hash) == shipments_hash_list[shipment_id]:
                        filtered_shipment.id = int(shipment_id_list[shipment_id])
                        continue

                    # Create a new shipment object with the next id
                    new_shipment: SAShipment = SAShipment(**row_query)
                    template_id = re.sub('[^0-9]', '', new_shipment.template_id)
                    new_shipment.template_id = None if not template_id else int(template_id) 
                    new_shipment.id = next_id
                    new_shipment.hash = shipment_hash
                    new_shipment.created_at = datetime.utcnow().replace(second=0, microsecond=0)

                    # add to the list will use in the bulk copy
                    if new_shipment.ds_status != 'A':
                        bulk_shipments.append(new_shipment)
                    else:
                        new_sa_template = SATemplate(**dict(new_shipment))
                        new_sa_template.id =  next_id_template
                        bulk_templates.append(new_sa_template)

                    # fill the shipment entity with the data we need to calculate the KPI
                    filtered_shipment.tmp_type = new_shipment.TmpType
                    filtered_shipment.id = next_id

                    # add one to continue with the next Shipment.
                    next_id += 1
                else:
                    logging.warning(f"Did't find the shipment: {shipment_id}")

        # bulk copy shipments and template objects.
        async with WareHouseDbConnector(stage=ENVIRONMENT.PRD) as wh_client:
            wh_client.bulk_copy(bulk_shipments)
            wh_client.bulk_copy(bulk_templates)
