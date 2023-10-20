from abc import ABC, abstractmethod

class Notifier(ABC):    
    def __init__(self):
        pass
    
    @abstractmethod
    async def send_information(self, list_shipments):
        raise NotImplementedError
