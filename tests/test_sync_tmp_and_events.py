import pytest

from common.common_infrastructure.cross_cutting.environment import ENVIRONMENT
from src.sync_tmp_events.extract.tmp_repository import TmpRepository
from src.sync_tmp_events.load.sync_shipment_repository import SyncShipmentRepository
from src.sync_tmp_events.sync_tmp_events_chaged import SyncronizerTmpAndEventsChaged

@pytest.mark.asyncio
class TestSyncronizerTmpAndEvents:

    async def test_syncronizer_tmp_and_events(self):
        # Arrange
        syncronizer = SyncronizerTmpAndEventsChaged(
            stage=ENVIRONMENT.UAT
            , tmp_repository=TmpRepository(ENVIRONMENT.UAT)
            , sync_information=SyncShipmentRepository(ENVIRONMENT.UAT)
            )

        # Act
        await syncronizer.syncronize()

        # Assert
        assert syncronizer.tmps != None
