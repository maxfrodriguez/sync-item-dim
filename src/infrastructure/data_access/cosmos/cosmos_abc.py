from abc import abstractmethod

from azure.cosmos.aio._container import ContainerProxy

from src.infrastructure.data_access.alchemy.custom_context_manager_abc import AsyncContextManagerABC


class CosmosABC(AsyncContextManagerABC):
    @abstractmethod
    def __init__(self, container: ContainerProxy) -> None:
        raise NotImplementedError

    @abstractmethod
    async def connect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def close_all(self) -> None:
        raise NotImplementedError
