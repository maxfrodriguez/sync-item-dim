import re
import logging
from datetime import datetime
import pandas as pd
from typing import Any, Dict, Generator, List, Literal

from src.domain.entities.shipment import Event, Shipment
from src.domain.repository.shipment_abc import ShipmentRepositoryABC
from src.infrastructure.cross_cutting.environment import ENVIRONMENT
from src.infrastructure.cross_cutting.hasher import deep_hash
from src.infrastructure.data_access.alchemy.sa_session_impl import get_sa_session
from src.infrastructure.data_access.db_profit_tools_access.pt_anywhere_client import (
    PTSQLAnywhere,
)
from src.infrastructure.data_access.db_profit_tools_access.queries.queries import (
    COMPLETE_EVENT_QUERY,
    COMPLETE_SHIPMENT_QUERY,
    ITEMS_QUERY,
    MODLOG_QUERY,
    NEXT_ID_WH,
    SHIPMENT_EQUIPMENT_SPLITTED_QUERY,
    SHIPMENT_SPLITTED_QUERY,
    SHIPMENTS_CUSTOM_FIELDS_QUERY,
    WAREHOUSE_SHIPMENTS,
)
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import (
    SACustomFields,
    SAItems,
    SALoaderLog,
    SAShipment,
    SATemplate,
)
from src.infrastructure.data_access.db_ware_house_access.whdb_anywhere_client import (
    WareHouseDbConnector,
)
from src.infrastructure.data_access.sybase.sql_anywhere_impl import Record
from src.infrastructure.repository.fact_customer_kpi_impl import FactCustomerKPIImpl
from src.infrastructure.repository.street_turn_impl import StreetTurnImpl
from src.infrastructure.cross_cutting.data_types import dtypes


class ShipmentImpl(ShipmentRepositoryABC):
    async def bulk_save(
        self,
        bulk_of_shipments: List[SAShipment],
        bulk_of_templates: List[SATemplate],
        bulk_of_items: List[SAItems],
        bulk_of_custom_fields: List[SACustomFields],
    ) -> None:
        async with WareHouseDbConnector(stage=ENVIRONMENT.PRD) as wh_client:
            wh_client.bulk_copy(bulk_of_shipments)
            wh_client.bulk_copy(bulk_of_templates)
            wh_client.bulk_copy(bulk_of_items)
            wh_client.bulk_copy(bulk_of_custom_fields)

    async def emit_to_eg_street_turn(self, eg_shipments: List[Shipment]):
        if len(eg_shipments) > 0:
            async with StreetTurnImpl(stage=ENVIRONMENT.PRD) as street_turn_client:
                await street_turn_client.send_street_turn_information(eg_shipments)

    async def emit_to_eg_customer_kpi(self, eg_shipments: List[Shipment]):
        if len(eg_shipments) > 0:
            async with FactCustomerKPIImpl(stage=ENVIRONMENT.PRD) as customer_kpi_impl:
                await customer_kpi_impl.send_customer_kd_info(eg_shipments)

    async def get_shipment_by_id(self, shipment: Shipment) -> Shipment:
        async with PTSQLAnywhere(stage=ENVIRONMENT.PRD) as sybase_client:
            result: Generator[Record, None, None] = sybase_client.SELECT(
                COMPLETE_EVENT_QUERY.format(shipment.ds_id), result_type=dict
            )
            if result:
                shipment.events = [Event(**event) for event in result]
            return shipment

    def remove_templates_from_shipments(
        self, shipment_list: List[Shipment]
    ) -> List[Shipment]:
        cleaned_shipments = [
            shipment for shipment in shipment_list if shipment.ds_status != "A"
        ]
        return cleaned_shipments

    def create_new_shipment(self, shipment: Shipment) -> Shipment:
        pass

    def create_new_template(self, shipment: Shipment) -> Shipment:
        pass

    def create_items_list(self, shipment: Shipment) -> Shipment:
        pass

    def create_new_custom_fields(self, shipment: Shipment) -> Shipment:
        pass

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
                        Shipment(
                            ds_id=shipment["ds_id"],
                            modlog=shipment["r_mod_id"],
                            ds_status=shipment["ds_status"],
                        )
                        for shipment in result
                    ]
                except Exception as e:
                    logging.error(f"Error in retrieve_shipment_list: {e}")

            return shipments if len(shipments) > 0 else None

    async def save_and_sync_shipment(
        self, list_of_shipments: List[Shipment]
    ) -> List[Shipment]:
        eg_shipments: List[Shipment] = []
        items_list: List[SAItems] = []
        custom_fields_list = []

        ids = ", ".join(f"'{shipment.ds_id}'" for shipment in list_of_shipments)

        async with PTSQLAnywhere(stage=ENVIRONMENT.PRD) as sybase_client:
            rows: Generator[Record, None, None] = sybase_client.SELECT(
                SHIPMENT_SPLITTED_QUERY.format(ids), result_type=dict
            )
            rows_equipment: Generator[Record, None, None] = sybase_client.SELECT(
                SHIPMENT_EQUIPMENT_SPLITTED_QUERY.format(ids), result_type=dict
            )
            rows_items: Generator[Record, None, None] = sybase_client.SELECT(
                ITEMS_QUERY.format(ids), result_type=dict
            )
            rows_custom_fields: Generator[Record, None, None] = sybase_client.SELECT(
                SHIPMENTS_CUSTOM_FIELDS_QUERY.format(ids), result_type=dict
            )

        # if rows/rows_items is empty, raise the exeption to close the process because we need to loggin just in application layer
        assert rows, f"did't not found shipments to sync at {datetime.now()}"
        assert rows_items, f"didn't found items to sync at {datetime.now()}"
        # assert (
        #     rows_custom_fields
        # ), f"didn't found custom fields to sync at {datetime.now()}"

        # Unifies the shipment and equipment rows using pandas.
        merged_list = []

        for row in rows:
            ds_id = row["ds_id"]
            matching_rows = [
                eq_row for eq_row in rows_equipment if eq_row["ds_id"] == ds_id
            ]

            if matching_rows:
                for matching_row in matching_rows:
                    merged_row = {**row, **matching_row}  # Merge the dictionaries
                    merged_list.append(merged_row)
            else:
                merged_list.append(row)  # Agregar la fila original sin modificaciones

        rows = merged_list
        # declare a set list to store the RateConfShipment objects
        unique_shipment_ids = set()
        # create a list of rateconf_shipment objects
        bulk_shipments: List[SAShipment] = []
        bulk_templates: List[SATemplate] = []
        bulk_of_custom_fields: List[SACustomFields] = []

        async with WareHouseDbConnector(stage=ENVIRONMENT.PRD) as wh_client:
            row_next_id = wh_client.execute_select(NEXT_ID_WH.format("shipments"))
            row_next_id_item = wh_client.execute_select(NEXT_ID_WH.format("items"))
            row_next_id_custom_fields = wh_client.execute_select(
                NEXT_ID_WH.format("shipments_custom_fields")
            )
            # Get List Shipment ID, DS_ID, HASH
            wh_shipments: List[Dict[str, Any]] = wh_client.execute_select(
                WAREHOUSE_SHIPMENTS.format(ids)
            )

        shipments_hash_list = {
            wh_shipment["ds_id"]: wh_shipment["hash"] for wh_shipment in wh_shipments
        }
        shipment_id_list = {
            wh_shipment["ds_id"]: wh_shipment["id"] for wh_shipment in wh_shipments
        }

        assert (
            row_next_id
        ), f"Did't not found next Id for ''Shipments WH'' at {datetime.now()}"

        next_id = (
            row_next_id[0]["NextId"] if row_next_id[0]["NextId"] is not None else 0
        )
        next_id_item = (
            row_next_id_item[0]["NextId"]
            if row_next_id_item[0]["NextId"] is not None
            else 0
        )
        next_id_custom_fields = (
            row_next_id_custom_fields[0]["NextId"]
            if row_next_id_custom_fields[0]["NextId"] is not None
            else 0
        )

        # read shipments_query one by one
        for row_query in rows:
            # create a KeyRateConfShipment object to store the data from the shipment
            shipment_hash = deep_hash(row_query)
            shipment_id = row_query["ds_id"]

            # validate if the unique_rateconf_key is not in the set list to avoid duplicates of the same RateConfShipment
            if shipment_id not in unique_shipment_ids:
                # add the shipment_obj to the set list
                unique_shipment_ids.add(shipment_id)

                filtered_shipments = [
                    shipment
                    for shipment in list_of_shipments
                    if shipment.ds_id == shipment_id
                ]
                list_of_items = [
                    item for item in rows_items if item["ds_id"] == shipment_id
                ]
                list_of_custom_fields = [
                    custom_field
                    for custom_field in rows_custom_fields
                    if custom_field["ds_id"] == shipment_id
                ]

                if filtered_shipments:
                    # get the shipment to update with the id to be inserted
                    filtered_shipment = filtered_shipments[0]
                    # Shipment custom fields
                    custom_fields = (
                        list_of_custom_fields[0] if list_of_custom_fields else None
                    )

                    # Comparamos Hashes
                    if (
                        shipment_hash
                        and shipment_id in shipments_hash_list
                        and shipments_hash_list[shipment_id]
                     )  and str(shipment_hash) == shipments_hash_list[shipment_id]:
                        filtered_shipment.id = int(shipment_id_list[shipment_id])
                        # For existing shipments:
                        # if custom_fields:
                        #     custom_fields['id'] = next_id_custom_fields
                        #     custom_fields['sk_id_shipment_fk'] = filtered_shipment.id
                        #     custom_fields['created_at'] = datetime.utcnow().replace(
                        #         second=0, microsecond=0
                        #     )

                        #     # Adds the custom fields to the list
                        #     custom_fields_list.append(custom_fields)
                        #     next_id_custom_fields += 1
                        continue

                    # Add shipment changed
                    eg_shipments.append(filtered_shipment)

                    # Create a new shipment object with the next id
                    del row_query["RateCodename"]
                    new_shipment: SAShipment = SAShipment(**row_query)
                    template_id = re.sub("[^0-9]", "", new_shipment.template_id)
                    new_shipment.template_id = (
                        None if not template_id else int(template_id)
                    )
                    # new_shipment.id = next_id
                    new_shipment.id = next_id
                    new_shipment.hash = shipment_hash
                    new_shipment.created_at = datetime.utcnow().replace(
                        second=0, microsecond=0
                    )

                    # Modifies the custom fields object to link it to the new shipment
                    if custom_fields:
                        custom_fields['id'] = next_id_custom_fields
                        custom_fields['sk_id_shipment_fk'] = new_shipment.id
                        custom_fields['created_at'] = datetime.utcnow().replace(
                            second=0, microsecond=0
                        )

                        # Adds the custom fields to the list
                        custom_fields_list.append(custom_fields)
                        next_id_custom_fields += 1

                    # Adds new quote_id and quote_note to Shipments and Templates
                    if (
                        new_shipment.quote_id is None
                        and new_shipment.quote_note is not None
                    ):
                        s = new_shipment.quote_note
                        match = re.search(r"QUOTE#\s*(.*?)\s*-", s)
                        if match:
                            new_shipment.quote_id = match.group(1)

                    # Saves item_list per shipment
                    for item in list_of_items:
                        new_sa_item: SAItems = SAItems(**item)
                        new_sa_item.id = next_id_item
                        new_sa_item.sk_id_shipment_fk = next_id
                        new_sa_item.created_at = datetime.utcnow().replace(
                            second=0, microsecond=0
                        )
                        items_list.append(new_sa_item)
                        next_id_item += 1

                    # add to the list will use in the bulk copy
                    if new_shipment.ds_status != "A":
                        bulk_shipments.append(new_shipment)
                    else:
                        del row_query["division"]
                        new_sa_template = SATemplate(**row_query)
                        template_id = re.sub("[^0-9]", "", new_sa_template.template_id)
                        new_sa_template.template_id = (
                            None if not template_id else int(template_id)
                        )
                        new_sa_template.hash = shipment_hash
                        new_sa_template.created_at = datetime.utcnow().replace(
                            second=0, microsecond=0
                        )
                        bulk_templates.append(new_sa_template)

                    # fill the shipment entity with the data we need to calculate the KPI
                    filtered_shipment.tmp_type = new_shipment.TmpType
                    filtered_shipment.id = next_id

                    # add one to continue with the next Shipment.
                    next_id += 1
                else:
                    logging.warning(f"Did't find the shipment: {shipment_id}")

        if custom_fields_list:
            for custom_field in custom_fields_list:
                new_sa_custom_fields: SACustomFields = SACustomFields(**custom_field)
                bulk_of_custom_fields.append(new_sa_custom_fields)

        # await self.bulk_save(bulk_shipments, bulk_templates, items_list, bulk_of_custom_fields)

        # Emit information to EG Street Turns
        # await self.emit_to_eg_street_turn(eg_shipments=eg_shipments)

        # Emit information to EG Customers KPIs
        # await self.emit_to_eg_customer_kpi(eg_shipments=eg_shipments)

        # Remove templates from Shipments
        list_of_shipments = self.remove_templates_from_shipments(
            shipment_list=list_of_shipments
        )
        return list_of_shipments
