from abc import ABC, abstractmethod


class FactCustomerKPIRepositoryABC(ABC):
    @abstractmethod
    def send_customer_kd_info(self) -> None:
        raise NotImplementedError