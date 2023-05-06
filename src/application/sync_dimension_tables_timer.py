import logging
from typing import List, Set
from src.application.retrieve_shipments import retrieve_shipments_list
from src.application.sync_dimension_tables import sync_dimension_tables
from src.application.sync_loader_log import finish_synchronization
from src.domain.entities.shipment import Shipment


async def sync_dimension_tables_timer() -> None:
    total_movements: int = 0
    shipments: List[Shipment] = await retrieve_shipments_list()
    if shipments:
        modlog_ids = [shipment.modlog for shipment in shipments]

        await sync_dimension_tables(shipments=shipments)
        
        await finish_synchronization(
                lowest_modlog=min(modlog_ids),
                highest_modlog=max(modlog_ids),
                fact_movements_loaded=total_movements,
            )
    else:
        logging.warning("No modlogs!")