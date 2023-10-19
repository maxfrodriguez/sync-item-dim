from abc import ABC, abstractmethod
from typing import List

from src.sync_tmp_events.extract.data.modlog import ModLog


class TmpRepositoryABC(ABC):
    @abstractmethod
    def get_tmp_changed(self) -> List[ModLog]:
        raise NotImplementedError
    
    @abstractmethod
    def next_shipments(self, pack_size):
        raise NotImplementedError