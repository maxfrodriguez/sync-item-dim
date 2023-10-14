from abc import ABC, abstractmethod

class Notifier(ABC):    
    def __init__(self, stage):
        self._stage = stage
    
    @abstractmethod
    async def send_information(self, list_shipments):
        raise NotImplementedError
