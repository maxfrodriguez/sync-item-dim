from typing import List

from src.domain.entities.shipment import Shipment
from src.infrastructure.cross_cutting.environment import ENVIRONMENT
from src.infrastructure.repository.events_impl import EventImpl
from src.infrastructure.repository.recalculate_movements_impl import RecalculateMovementsImpl
from src.infrastructure.repository.shipment_impl import ShipmentImpl
from src.infrastructure.repository.stops_impl import StopsImpl


async def sync_dimension_tables(shipments: List[Shipment]):
    shipment_client = ShipmentImpl()

    event_client = EventImpl()
    stop_client = StopsImpl()

    shipments = await shipment_client.save_and_sync_shipment(list_of_shipments=shipments)

    if len(shipments) > 0:
        await event_client.save_and_sync_events(list_of_shipments=shipments)
        await stop_client.save_and_sync_stops(list_of_shipments=shipments)

        async with RecalculateMovementsImpl(stage=ENVIRONMENT.PRD) as calc_movements_client:
            for shipment in shipments:
                if shipment.has_changed_events or shipment.has_changed_stops and shipment.ds_status != "Template":
                    await calc_movements_client.recalculate_movements(shipment=shipment)