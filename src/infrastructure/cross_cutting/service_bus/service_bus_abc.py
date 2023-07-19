from abc import ABC, abstractmethod
from typing_extensions import Self
from src.infrastructure.cross_cutting.environment import ENVIRONMENT


class ServiceBusABC:
    @abstractmethod
    def __init__(self, stage: ENVIRONMENT) -> None:
        raise NotImplementedError

    @abstractmethod
    async def __enter__(self) -> Self:
        raise NotImplementedError

    @abstractmethod
    async def __exit__(self, *_) -> None:
        raise NotImplementedError

    @abstractmethod
    async def send_message(self, message: str) -> None:
        raise NotImplementedError