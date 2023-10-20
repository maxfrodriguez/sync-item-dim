import datetime
import logging

import azure.functions as func

from src.sync_tmp_events.extract.tmp_repository import TmpRepository
from src.sync_tmp_events.load.sync_shipment_repository import SyncShipmentRepository

from src.sync_tmp_events.sync_tmp_events_chaged import SyncronizerTmpAndEventsChaged




async def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    logging.info('Timer trigger function ran at %s', utc_timestamp)

    syncronizer = SyncronizerTmpAndEventsChaged(
        tmp_repository=TmpRepository()
        , sync_information=SyncShipmentRepository()
        )

    # Act
    await syncronizer.syncronize()
