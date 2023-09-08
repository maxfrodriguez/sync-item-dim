import datetime
import logging

import azure.functions as func

from src.application.sync_dimension_tables_timer import sync_dimension_tables_timer


async def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    await sync_dimension_tables_timer()

    logging.info('Timer trigger function ran at %s', utc_timestamp)
