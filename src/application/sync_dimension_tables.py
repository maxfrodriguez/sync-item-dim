from typing import List

from src.domain.entities.shipment import Shipment
from src.infrastructure.repository.events_impl import EventImpl
from src.infrastructure.repository.recalculate_movements_impl import RecalculateMovementsImpl
from src.infrastructure.repository.shipment_impl import ShipmentImpl
from src.infrastructure.repository.stops_impl import StopsImpl


async def sync_dimension_tables(shipments: List[Shipment]):
    shipment_client = ShipmentImpl()

    event_client = EventImpl()
    stop_client = StopsImpl()
    recalculate_movements_repository = RecalculateMovementsImpl()

    await shipment_client.save_and_sync_shipment(list_of_shipments=shipments)
    await event_client.save_and_sync_events(list_of_shipments=shipments, recalculate_movements_repository=recalculate_movements_repository)
    await stop_client.save_and_sync_stops(list_of_shipments=shipments, recalculate_movements_repository=recalculate_movements_repository)