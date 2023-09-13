from typing import List

from src.domain.entities.shipment import Shipment
from src.infrastructure.data_access.db_profit_tools_access.queries.queries import CLOSED_MODLOGS_QUERY
from src.infrastructure.repository.shipment_impl import ShipmentImpl


async def retrieve_shipments_list() -> List[Shipment]:
    shipment_client = ShipmentImpl()
    shipments = await shipment_client.retrieve_shipment_list()

    return shipments
