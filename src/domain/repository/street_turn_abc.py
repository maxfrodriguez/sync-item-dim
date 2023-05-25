
from abc import ABC, abstractmethod


class StreetTurnRepositoryABC(ABC):

    @abstractmethod
    def send_street_turn_information(self) -> None:
        raise NotImplementedError