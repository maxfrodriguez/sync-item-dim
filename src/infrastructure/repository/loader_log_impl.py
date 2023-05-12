from datetime import datetime
import logging
from typing import List

from src.domain.repository.loader_logs_abc import LoaderLogRepositoryABC
from src.infrastructure.cross_cutting.environment import ENVIRONMENT
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SALoaderLog
from src.infrastructure.data_access.db_ware_house_access.whdb_anywhere_client import WareHouseDbConnector


class LoaderLogImpl(LoaderLogRepositoryABC):
    async def save_latest_loader_logs(
        self, lowest_modlog: int, highest_modlog: int, fact_movements_loaded: int
    ) -> None:
        try:
            new_modlog: SALoaderLog = SALoaderLog(
                mod_lowest_version=lowest_modlog,
                mod_highest_version=highest_modlog,
                num_fact_movements_loaded=fact_movements_loaded,
                created_at= datetime.utcnow().replace(second=0, microsecond=0)
            )

            async with WareHouseDbConnector(stage=ENVIRONMENT.UAT) as wh_client:
                wh_client.save_object(new_modlog)
            
        except Exception as e:
            logging.error(f"error in save_latest_loader_logs:{e} at {datetime.now()}")
