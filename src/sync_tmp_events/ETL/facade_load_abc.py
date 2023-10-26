from abc import ABC, abstractmethod
from typing import Any, Dict, List

from src.sync_tmp_events.extract.data.EventPTDTO import EventPTDTO



class LoadFacadeABC(ABC):
    @abstractmethod
    async def load_events(self, list_events: List[EventPTDTO]) -> None:
        raise NotImplementedError
    
    @abstractmethod
    async def delete_events(self, event_to_delete: List[Dict[str, Any]]) -> None:
        raise NotImplementedError