import pytest
from src.domain.entities.driver import Driver
from src.infrastructure.cross_cutting.environment import ENVIRONMENT
from src.infrastructure.data_access.db_profit_tools_access.queries.queries import WH_DRIVERS_QUERY
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SADrivers

from src.infrastructure.data_access.db_ware_house_access.whdb_anywhere_client import WareHouseDbConnector
from src.infrastructure.repository.driver_impl import DriverImpl

class TestDriverImpl:


    @pytest.mark.asyncio
    @pytest.mark.parametrize("pt_driver_id", [10024])
    async def test_save_driver(self, pt_driver_id:int) -> None:
        driver_impl = DriverImpl()
        await driver_impl.save_driver(pt_driver_id=pt_driver_id)
