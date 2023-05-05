import logging
from datetime import datetime
from typing import Generator, List

from src.domain.entities.shipment import Event, Shipment
from src.domain.repository.shipment_abc import ShipmentRepositoryABC
from src.infrastructure.cross_cutting.environment import ENVIRONMENT
from src.infrastructure.data_access.alchemy.sa_session_impl import get_sa_session
from src.infrastructure.data_access.db_profit_tools_access.pt_anywhere_client import PTSQLAnywhere
from src.infrastructure.data_access.db_profit_tools_access.queries.queries import (
    COMPLETE_EVENT_QUERY,
    COMPLETE_SHIPMENT_QUERY,
    MODLOG_QUERY,
    NEXT_ID_WH,
)
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SALoaderLog, SAShipment
from src.infrastructure.data_access.db_ware_house_access.whdb_anywhere_client import WareHouseDbConnector
from src.infrastructure.data_access.sybase.sql_anywhere_impl import Record


class ShipmentImpl(ShipmentRepositoryABC):
    # async def get_entire_shipment(self, shipment_id: int):
    #    raw_shipment: Dict[str, Any] = {}
    #    async with PTSQLAnywhere(stage=ENVIRONMENT.UAT) as sybase_client:
    #        result: Generator[Record, None, None] = sybase_client.SELECT(COMPLETE_SHIPMENT_QUERY.format(shipment_id), result_type=dict)
    #        if result:
    #            raw_shipment = result[0]
    #        else:
    #            logging.error(f"I could not find data for this shipment id: {shipment_id}")
    #        return raw_shipment

    async def get_shipment_by_id(self, shipment: Shipment) -> Shipment:
        async with PTSQLAnywhere(stage=ENVIRONMENT.UAT) as sybase_client:
            result: Generator[Record, None, None] = sybase_client.SELECT(
                COMPLETE_EVENT_QUERY.format(shipment.ds_id), result_type=dict
            )
            if result:
                shipment.events = [Event(**event) for event in result]
            return shipment

    async def retrieve_shipment_list(
        self, last_modlog: int = None, query_to_execute: str = MODLOG_QUERY
    ) -> List[Shipment]:
        latest_sa_mod_log: SALoaderLog | None = None
        if last_modlog is None:
            async with get_sa_session() as session:
                latest_sa_mod_log = SALoaderLog.get_highest_version(db_session=session)
            if not latest_sa_mod_log:
                return

        async with PTSQLAnywhere(stage=ENVIRONMENT.UAT) as sybase_client:
            result: Generator[Record, None, None] = sybase_client.SELECT(
                query_to_execute.format(latest_sa_mod_log.mod_highest_version)
            )

            if result:
                try:
                    shipments: List[Shipment] = [
                        Shipment(ds_id=shipment["ds_id"], modlog=shipment["r_mod_id"])
                        for shipment in result
                        # if shipment_log.ds_id
                    ]
                except Exception as e:
                    logging.error(f"Error -> retrieve_shipment_list: {e}")

            return shipments

    async def save_and_sync_shipment(self, list_of_shipments: List[Shipment]):
        ids = ", ".join(f"'{shipment.ds_id}'" for shipment in list_of_shipments)

        async with PTSQLAnywhere(stage=ENVIRONMENT.UAT) as sybase_client:
            rows: Generator[Record, None, None] = sybase_client.SELECT(
                COMPLETE_SHIPMENT_QUERY.format(ids), result_type=dict
            )

        # if rows is empty, raise the exeption to close the process because we need to loggin just in application layer
        assert rows, f"don't not found shipments to sync at {datetime.now()}"

        # declare a set list to store the RateConfShipment objects
        unique_shipment = set()
        # create a list of rateconf_shipment objects
        bulk_shipments: List[SAShipment] = []

        async with WareHouseDbConnector(stage=ENVIRONMENT.UAT) as wh_client:
            row_next_id = wh_client.execute_select(NEXT_ID_WH.format("shipments"))
            # Get List Shipment ID, DS_ID, HASH

        assert row_next_id, f"Don't not found next Id for ''Shipments WH'' at {datetime.now()}"

        next_id = row_next_id[0]["NextId"]

        # read shipments_query one by one
        for row_query in rows:
            # create a KeyRateConfShipment object to store the data from the shipment
            unique_key = row_query["ds_id"]
            # Compare HASH

            # validate if the unique_rateconf_key is not in the set list to avoid duplicates of the same RateConfShipment
            if unique_key not in unique_shipment:
                # add the shipment_obj to the set list
                unique_shipment.add(unique_key)

                filtered_shipments = [shipment for shipment in list_of_shipments if shipment.ds_id == unique_key]

                if filtered_shipments:
                    # get the shipment to update with the id to be inserted
                    found_shipment = filtered_shipments[0]
                    new_shipment: SAShipment = SAShipment(**row_query)
                    new_shipment.id = next_id
                    found_shipment.id = new_shipment.id

                    # add to the list will use in the bulk copy
                    bulk_shipments.append(new_shipment)

                    # fill the shipment entity with the data we need to calculate the KPI
                    found_shipment.tmp_type = new_shipment.TmpType
                    found_shipment.id = next_id

                    # add one to continue with the next Shipment.
                    next_id += 1
                else:
                    logging.warning(f"Don't find the shipment: {unique_key}")

        # bulk copy de bulk_shipments
        async with WareHouseDbConnector(stage=ENVIRONMENT.UAT) as wh_client:
            wh_client.bulk_copy(bulk_shipments)
