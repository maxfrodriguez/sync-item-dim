from abc import ABC, abstractmethod
from typing import List

from src.domain.entities.customer import Customer

class HubspotABC(ABC):
    @abstractmethod
    async def send_customer_sb(self, shipments_customers: List[Customer]):
        raise NotImplementedError