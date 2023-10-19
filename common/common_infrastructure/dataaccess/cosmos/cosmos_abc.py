from abc import abstractmethod

from azure.cosmos.aio._container import ContainerProxy


class CosmosABC:
    @abstractmethod
    def __init__(self, container: ContainerProxy) -> None:
        raise NotImplementedError

    @abstractmethod
    def __enter__(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def __exit__(self) -> None:
        raise NotImplementedError
