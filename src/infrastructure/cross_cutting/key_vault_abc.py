from abc import ABC, abstractmethod

from typing_extensions import Self

from src.infrastructure.cross_cutting import ENVIRONMENT


class KeyVaultABC(ABC):
    @abstractmethod
    def __init__(self, stage: ENVIRONMENT) -> None:
        raise NotImplementedError

    @abstractmethod
    async def __aenter__(self) -> Self:
        raise NotImplementedError

    @abstractmethod
    async def __aexit__(self, *_) -> None:
        raise NotImplementedError

    @abstractmethod
    async def close_all(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def get_secret(self, secret_name: str) -> str | None:
        raise NotImplementedError
