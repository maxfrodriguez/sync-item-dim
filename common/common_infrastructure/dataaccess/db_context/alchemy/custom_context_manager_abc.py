from abc import ABC, abstractmethod

from typing_extensions import Self


class AsyncContextManagerABC(ABC):
    @abstractmethod
    async def __aenter__(self) -> Self:
        raise NotImplementedError

    @abstractmethod
    async def __aexit__(self, *_) -> None:
        raise NotImplementedError


class SyncContextManagerABC(ABC):
    @abstractmethod
    def __aenter__(self) -> Self:
        raise NotImplementedError

    @abstractmethod
    def __aexit__(self, *_) -> None:
        raise NotImplementedError
