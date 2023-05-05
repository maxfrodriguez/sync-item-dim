from abc import abstractmethod

from src.infrastructure.data_access.alchemy.custom_context_manager_abc import AsyncContextManagerABC


class TableStorageABC(AsyncContextManagerABC):
    @abstractmethod
    def __init__(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def connect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def close_all(self) -> None:
        raise NotImplementedError
