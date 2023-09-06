from abc import ABC, abstractmethod

class ItemABC(ABC):

    @abstractmethod
    async def get_items(self, item: str):
        raise NotImplementedError

    @abstractmethod
    async def save_items(self, item: str):
        raise NotImplementedError