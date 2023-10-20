from abc import abstractmethod

from typing_extensions import Self



class ServiceBusABC:
    @abstractmethod
    def __init__(self) -> None:
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
