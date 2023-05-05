from typing import List
import pytest
from src.application.sync_dimension_tables import sync_dimension_tables
from src.domain.entities.shipment import Shipment
from src.application.retrieve_shipments import retrieve_shipments_list


@pytest.mark.asyncio
class TestSyncUp:
    async def test_sync_dimension_tables(self):
        shipments: List[Shipment] = await retrieve_shipments_list()
        await sync_dimension_tables(shipments=shipments)