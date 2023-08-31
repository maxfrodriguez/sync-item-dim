from typing import List

from src.domain.entities.shipment import Shipment
from src.infrastructure.cross_cutting.environment import ENVIRONMENT
from src.infrastructure.cross_cutting.service_bus.service_bus_impl import ServiceBusImpl
from src.infrastructure.repository.empty_return_impl import EmptyReturnImpl
from src.infrastructure.repository.events_impl import EventImpl
from src.infrastructure.repository.recalculate_movements_impl import RecalculateMovementsImpl
from src.infrastructure.repository.shipment_impl import ShipmentImpl
from src.infrastructure.repository.stops_impl import StopsImpl
from src.infrastructure.repository.street_turn_impl import StreetTurnImpl


async def sync_dimension_tables(shipments: List[Shipment]):
    shipment_client = ShipmentImpl()

    event_client = EventImpl()
    stop_client = StopsImpl()

    shipments, shipments_id_list = await shipment_client.save_and_sync_shipment(list_of_shipments=shipments)

    if shipments:
        await event_client.save_and_sync_events(list_of_shipments=shipments)
        await stop_client.save_and_sync_stops(list_of_shipments=shipments)

        id_set = set(shipments_id_list)
        id_list_from_set = [id for id in id_set]
        
        async with EmptyReturnImpl(stage=ENVIRONMENT.PRD) as empty_return_client:
            await empty_return_client.create_empty_return(shipment_list=shipments)

        async with ServiceBusImpl(stage=ENVIRONMENT.PRD ) as service_bus_client:
            await service_bus_client.send_message(id_list_from_set,'dim_dates_queue')