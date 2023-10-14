from abc import ABC, abstractmethod
from typing import Union

from typing_extensions import Self

from common.common_infrastructure.cross_cutting.environment import ENVIRONMENT


class KeyVaultABC(ABC):
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
    async def close_all(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def get_secret(self, secret_name: str) -> Union[str, None]:
        raise NotImplementedError
