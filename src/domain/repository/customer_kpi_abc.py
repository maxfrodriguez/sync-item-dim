from abc import ABC, abstractmethod

class CustomerKpiABC(ABC):

    @abstractmethod
    async def send_customer_kpi_sb(self, shipment_list: list):
        raise NotImplementedError