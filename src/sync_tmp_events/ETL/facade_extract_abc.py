from abc import ABC, abstractmethod

class ExtractFacadeABC(ABC):
    @abstractmethod
    def get_events_tmp_changed(self) -> None:
        raise NotImplementedError
    
    @abstractmethod
    def next_events(self, pack_size):
        raise NotImplementedError