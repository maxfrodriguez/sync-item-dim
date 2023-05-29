from abc import abstractmethod

from src.infrastructure.cross_cutting import ENVIRONMENT
from src.infrastructure.data_access.alchemy.custom_context_manager_abc import AsyncContextManagerABC


class SQLAnywhereABC(AsyncContextManagerABC):
    @abstractmethod
    def __init__(self, stage: ENVIRONMENT) -> None:
        raise NotImplementedError

    @abstractmethod
    async def connect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def close_all(self) -> None:
        raise NotImplementedError
