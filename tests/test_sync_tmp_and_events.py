import pytest

from src.sync_tmp_events.extract.tmp_repository import TmpRepository
from src.sync_tmp_events.load.sync_shipment_repository import SyncShipmentRepository
from src.sync_tmp_events.sync_tmp_events_chaged import SyncronizerTmpAndEventsChaged

@pytest.mark.asyncio
class TestSyncronizerTmpAndEvents:

    async def test_syncronizer_tmp_and_events(self):
        # Arrange
        syncronizer = SyncronizerTmpAndEventsChaged(
            tmp_repository=TmpRepository()
            , sync_information=SyncShipmentRepository()
            )

        # Act
        await syncronizer.syncronize()

        # Assert
        assert syncronizer.tmps != None
