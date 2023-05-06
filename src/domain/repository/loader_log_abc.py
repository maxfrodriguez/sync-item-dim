from abc import ABC, abstractmethod


class LoaderLogRepositoryABC(ABC):
    @abstractmethod
    async def save_latest_loader_logs(
        self, lowest_modlog: int, highest_modlog: int, fact_movements_loaded: int
    ) -> None:
        raise NotImplementedError
