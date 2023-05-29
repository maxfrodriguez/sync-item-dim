from abc import abstractmethod

from src.infrastructure.data_access.alchemy.custom_context_manager_abc import AsyncContextManagerABC


class GeotabABC(AsyncContextManagerABC):
    @abstractmethod
    def __init__(self, *args, **kwargs) -> None:
        raise NotImplementedError

    @abstractmethod
    async def auth(self) -> None:
        raise NotImplementedError
