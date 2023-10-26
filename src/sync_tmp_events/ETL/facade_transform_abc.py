from abc import ABC, abstractmethod
from typing import List
from src.sync_tmp_events.extract.data.EventPTDTO import EventPTDTO



class TransformFacadeABC(ABC):
    @abstractmethod
    async def transform_events_to_sync(self, list_events: List[EventPTDTO]) -> List[EventPTDTO]:
        raise NotImplementedError