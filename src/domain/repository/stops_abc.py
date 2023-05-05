from abc import ABC, abstractmethod
from typing import List

from src.domain.entities.stop import Stop
from src.infrastructure.data_access.db_121tower_access.tower121_anywhere_client import Tower121DdConnector


class StopsRepositoryABC(ABC):
    @abstractmethod
    def get_stop_by_id(self, tower121_client: Tower121DdConnector, event_id: int) -> Stop:
        raise NotImplementedError

    @abstractmethod
    async def save_stops(self, stops: List[Stop], session) -> None:
        raise NotImplementedError
