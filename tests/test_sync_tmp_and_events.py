import pytest

from src.sync_tmp_events.extract.extract_facade import ExtractFacade
from src.sync_tmp_events.load.load_facade import LoadFacade
from src.sync_tmp_events.transform.transform_facade import TransformFacade
from src.sync_tmp_events.ETL.sync_tmp_events_chaged import SyncronizerEventsChaged

@pytest.mark.asyncio
class TestSyncronizerEvents:

    async def test_syncronizer_events(self): 
        # Arrange
        tmps = [
            {"tmp":146576,"template_id":144412,"customer_id":120}
            ,{"tmp":146574,"template_id":144412,"customer_id":120}
            ,{"tmp":146572,"template_id":144412,"customer_id":120}
            ,{"tmp":146571,"template_id":144412,"customer_id":120}
            ,{"tmp":146573,"template_id":144412,"customer_id":120}
            ,{"tmp":146575,"template_id":144412,"customer_id":120}
            ,{"tmp":146372,"template_id":89312,"customer_id":31}
            ,{"tmp":146371,"template_id":89312,"customer_id":31}
            ,{"tmp":146250,"template_id":88856,"customer_id":31}
            ,{"tmp":146098,"template_id":100194,"customer_id":4545}
        ]


        syncronizer = SyncronizerEventsChaged(
            extract_service=ExtractFacade()
            , transform_repository=TransformFacade()
            , load_facade=LoadFacade()
            )

        # Act
        await syncronizer.syncronize(tmps)

        # Assert
        assert syncronizer != None

    async def test_delete_events_empty(self):
        tmps = [
            {"de_id":806026}
            , {"de_id":806027}
        ]

        await LoadFacade().delete_events(tmps)
        
