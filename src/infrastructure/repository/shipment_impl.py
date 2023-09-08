import re
import logging
from datetime import datetime, timedelta
import pandas as pd
from typing import Any, Dict, Generator, List, Literal
from src.domain.entities.custom_field import CustomField
from src.domain.entities.customer import Customer

from src.domain.entities.shipment import Event, Shipment
from src.domain.repository.shipment_abc import ShipmentRepositoryABC
from src.infrastructure.cross_cutting.environment import ENVIRONMENT
from src.infrastructure.cross_cutting.hasher import deep_hash
from src.infrastructure.cross_cutting.service_bus.service_bus_impl import ServiceBusImpl
from src.infrastructure.cross_cutting.shipment_helper import get_quote_id, get_template_id
from src.infrastructure.data_access.alchemy.sa_session_impl import get_sa_session
from src.infrastructure.data_access.db_profit_tools_access.pt_anywhere_client import (
    PTSQLAnywhere,
)
from src.infrastructure.data_access.db_profit_tools_access.queries.queries import (
    COMPLETE_EVENT_QUERY,
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
from src.infrastructure.repository.custom_field_impl import CustomFieldImpl
from src.infrastructure.repository.fact_customer_kpi_impl import FactCustomerKPIImpl
from src.infrastructure.repository.item_impl import ItemImpl
from src.infrastructure.repository.street_turn_impl import StreetTurnImpl
from src.infrastructure.cross_cutting.data_types import dtypes
from src.infrastructure.repository.template_impl import TemplateImpl


class ShipmentImpl(ShipmentRepositoryABC):

    def dict_to_generator(self, data_dict):
        for key, value in data_dict.items():
            yield value


    async def send_list_sb(self, id_list: List[int]) -> None:
        id_set = set(id_list)
        id_list_from_set = list(id_set)
        
        await self.send_sb_message(id_list_from_set)

    async def send_sb_message(
        self, id: int, stage: ENVIRONMENT = ENVIRONMENT.PRD
    ) -> None:
        async with ServiceBusImpl(stage=stage) as servicebus_client:
            try:
                await servicebus_client.send_message(
                    data=id, queue_name=servicebus_client.queue_name
                )
            except Exception as e:
                logging.exception(
                    f"An exception occurred while sending the message: {str(e)}"
                )

    async def bulk_save(
        self,
        bulk_of_shipments: List[SAShipment],
        bulk_of_templates: List[SATemplate]
    ) -> None:
        async with WareHouseDbConnector(stage=ENVIRONMENT.PRD) as wh_client:
            wh_client.bulk_copy(bulk_of_shipments)
            wh_client.bulk_copy(bulk_of_templates)
    
    async def emit_to_eg_street_turn(self, eg_shipments: List[Shipment]):
        if len(eg_shipments) > 0:
            async with StreetTurnImpl(stage=ENVIRONMENT.PRD) as street_turn_client:
                await street_turn_client.send_street_turn_information(eg_shipments)

    async def emit_to_eg_customer_kpi(self, eg_shipments: List[Shipment]):
        if len(eg_shipments) > 0:
            async with FactCustomerKPIImpl(stage=ENVIRONMENT.PRD) as customer_kpi_impl:
                await customer_kpi_impl.send_customer_kd_info(eg_shipments)

    def remove_templates_from_shipments(
        self, shipment_list: List[Shipment]
    ) -> List[Shipment]:
        cleaned_shipments = [
            shipment for shipment in shipment_list if shipment.ds_status != "A"
        ]
        return cleaned_shipments

    async def retrieve_shipment_list(
        self, last_modlog: int = None, query_to_execute: str = MODLOG_QUERY
    ) -> List[Shipment] | None:
        shipments: List[Shipment] = []
        latest_sa_mod_log: SALoaderLog | None = None
        mod_datetime: datetime = datetime.utcnow() - timedelta(days=15)
        mod_datetime = mod_datetime.replace(hour=0, minute=0, second=0, microsecond=0)

        if last_modlog is None:
            async with get_sa_session() as session:
                latest_sa_mod_log = SALoaderLog.get_highest_version(db_session=session)
            if not latest_sa_mod_log:
                return

        async with PTSQLAnywhere(stage=ENVIRONMENT.PRD) as sybase_client:
            result: Generator[Record, None, None] = sybase_client.SELECT(
                query_to_execute.format(
                    latest_sa_mod_log.mod_highest_version, mod_datetime
                )
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
        templates_list = []
        eg_shipments: List[Shipment] = []
        items_list: List[SAItems] = []
        custom_fields_list = []
        shipments_id_list = []
        customers_shipments_list: List[Customer] = []
        item_client: ItemImpl = ItemImpl()
        template_client: TemplateImpl = TemplateImpl()
        custom_field_client: CustomFieldImpl = CustomFieldImpl()

        ids = ", ".join(f"'{shipment.ds_id}'" for shipment in list_of_shipments)

        async with PTSQLAnywhere(stage=ENVIRONMENT.PRD) as sybase_client:
            rows: Generator[Record, None, None] = sybase_client.SELECT(
                SHIPMENT_SPLITTED_QUERY.format(ids), result_type=dict
            )
            rows_equipment: Generator[Record, None, None] = sybase_client.SELECT(
                SHIPMENT_EQUIPMENT_SPLITTED_QUERY.format(ids), result_type=dict
            )

        result_dict = {}

        for item in rows_equipment:
            ds_id = item['ds_id']
            eq_type = item['eq_type']
            eq_ref = item['eq_ref']
            
            if eq_type == 'C' and item['Line'] is not None and item['Type'] is not None:
                ds_id = ds_id
                eq_c_info_eq_type = item['eq_type']
                container_id = eq_ref
                eq_c_info_line = item['Line']
                eq_c_info_type = item['Type']
                
                if ds_id not in result_dict:
                    result_dict[ds_id] = {}
                    
                result_dict[ds_id]['ds_id'] = ds_id
                result_dict[ds_id]['eq_c_info_eq_type'] = eq_c_info_eq_type
                result_dict[ds_id]['eq_c_info_line'] = eq_c_info_line
                result_dict[ds_id]['eq_c_info_type'] = eq_c_info_type
                result_dict[ds_id]['container_id'] = container_id

            elif eq_type == 'H':
                ds_id = ds_id
                eq_h_info_eq_type = item['eq_type']
                chassis_id = eq_ref
                eq_h_info_line = item['Line']
                eq_h_info_type = item['Type']
                
                if ds_id not in result_dict:
                    result_dict[ds_id] = {}
                    
                result_dict[ds_id]['ds_id'] = ds_id
                result_dict[ds_id]['eq_h_info_eq_type'] = eq_h_info_eq_type
                result_dict[ds_id]['eq_h_info_line'] = eq_h_info_line
                result_dict[ds_id]['eq_h_info_type'] = eq_h_info_type
                result_dict[ds_id]['chassis_id'] = chassis_id


        rows_items: Generator[Record, None, None] = await item_client.get_items(shipments=list_of_shipments)
        rows_custom_fields: Generator[Record, None, None] = await custom_field_client.get_custom_fields(shipments=list_of_shipments)

        assert rows, f"did't not find shipments to sync at {datetime.now()}"

        # Unifies the shipment and equipment rows using pandas.
        equipments_rows = list(result_dict.values())
        merged_list = []

        for row in rows:
            ds_id = row["ds_id"]
            matching_rows = [
                eq_row for eq_row in equipments_rows if eq_row["ds_id"] == ds_id
            ]

            if matching_rows:
                for matching_row in matching_rows:
                    merged_row = {**row, **matching_row}  # Merge the dictionaries
                    merged_list.append(merged_row)
            else:
                merged_list.append(row)  # Agregar la fila original sin modificaciones

        all_rows = merged_list
        # declare a set list to store the RateConfShipment objects
        unique_shipment_ids = set()
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
        for row_query in all_rows:
            shipment_hash = deep_hash(row_query)
            shipment_id = row_query["ds_id"]
            # validate if the unique_rateconf_key is not in the set list to avoid duplicates of the same RateConfShipment
            if shipment_id not in unique_shipment_ids:
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
                    # Gets the shipment to update with the id to be inserted
                    filtered_shipment = filtered_shipments[0]
                    custom_fields = (list_of_custom_fields[0] if list_of_custom_fields else None)

                    # Compares Hashes
                    if (
                        shipment_hash
                        and shipment_id in shipments_hash_list
                        and shipments_hash_list[shipment_id]
                    ) and str(shipment_hash) != shipments_hash_list[shipment_id]:
                        filtered_shipment.id = int(shipment_id_list[shipment_id])
                        continue

                    # Add shipment changed
                    eg_shipments.append(filtered_shipment)

                    # Create a new shipment object with the next id
                    del row_query["RateCodename"]
                    new_shipment: SAShipment = SAShipment(**row_query)
                    new_shipment.template_id = get_template_id(value=new_shipment.template_id)
                    new_shipment.id = next_id
                    new_shipment.hash = shipment_hash
                    new_shipment.created_at = datetime.utcnow().replace(second=0, microsecond=0)
                    shipments_id_list.append(new_shipment.ds_id)

                    #Add current shipment, customer and template info
                    new_customer: Customer = Customer(
                        tmp=new_shipment.ds_id,
                        template_id=new_shipment.template_id,
                        customer_id=new_shipment.customer_id
                    )
                    customers_shipments_list.append(new_customer)

                    # Modifies the custom fields object to link it to the new shipment
                    if custom_fields:
                        custom_fields["id"] = next_id_custom_fields
                        custom_fields["sk_id_shipment_fk"] = new_shipment.id
                        custom_fields["created_at"] = datetime.utcnow().replace(second=0, microsecond=0)
                        custom_fields_list.append(custom_fields)
                        next_id_custom_fields += 1

                    # Assigns the quote_id to the shipment
                    if (new_shipment.quote_id is None and new_shipment.quote_note is not None):
                        new_shipment.quote_id = get_quote_id(value=new_shipment.quote_note)

                    # Saves item_list per shipment
                    for item in list_of_items:
                        new_sa_item: SAItems = SAItems(**item)
                        new_sa_item.id = next_id_item
                        new_sa_item.sk_id_shipment_fk = next_id
                        new_sa_item.created_at = datetime.utcnow().replace(second=0, microsecond=0)
                        items_list.append(new_sa_item)
                        next_id_item += 1

                    # add to the list will use in the bulk copy
                    if new_shipment.ds_status == "A":
                        del row_query["division"]
                        row_query["hash"] = shipment_hash
                        templates_list.append(row_query)
                    else:
                        bulk_shipments.append(new_shipment)

                    # fill the shipment entity with the data we need to calculate the KPI
                    filtered_shipment.tmp_type = new_shipment.TmpType
                    filtered_shipment.id = next_id
                    next_id += 1
                else:
                    logging.warning(f"Did't find the shipment: {shipment_id}")

        if custom_fields_list:
            for custom_field in custom_fields_list:
                new_sa_custom_fields: SACustomFields = SACustomFields(**custom_field)
                bulk_of_custom_fields.append(new_sa_custom_fields)

        # Save Shipments and Templates
        await self.bulk_save(bulk_shipments, bulk_templates)

        # Save Templates, Items and Custom Fields
        await template_client.save_templates(templates_list)
        await item_client.save_items(items_list)
        await custom_field_client.save_custom_fields(bulk_of_custom_fields)

        # Emit information to EG Street Turns
        await self.emit_to_eg_street_turn(eg_shipments=eg_shipments)

        # Emit information to EG Customers KPIs
        await self.emit_to_eg_customer_kpi(eg_shipments=eg_shipments)

        # Remove templates from Shipments
        list_of_shipments = self.remove_templates_from_shipments(
            shipment_list=list_of_shipments
        )
        
        return list_of_shipments, shipments_id_list, customers_shipments_list
