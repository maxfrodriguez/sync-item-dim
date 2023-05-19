from abc import ABC, abstractmethod

from typing_extensions import Self

class EventGridABC(ABC):
    @abstractmethod
    def __init__(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def __aenter__(self) -> Self:
        raise NotImplementedError

    @abstractmethod
    async def __aexit__(self, *_) -> None:
        raise NotImplementedError

    @abstractmethod
    async def send_event(self):
        raise NotImplementedError
