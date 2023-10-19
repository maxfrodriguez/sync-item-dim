from abc import abstractmethod

from mygeotab import API
from typing_extensions import Self


class GeotabABC:
    @abstractmethod
    def __init__(self, client: API) -> None:
        raise NotImplementedError

    @abstractmethod
    async def __enter__(self) -> Self:
        raise NotImplementedError

    @abstractmethod
    async def __exit__(self, *_) -> None:
        raise NotImplementedError
