from abc import ABC, abstractmethod

from typing_extensions import Self


class BaseId(ABC):
    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def __hash__(self):
        raise NotImplementedError

    @abstractmethod
    def __eq__(self, other: Self):
        raise NotImplementedError
