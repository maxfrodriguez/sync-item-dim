from abc import abstractmethod

from common.common_infrastructure.cross_cutting import ENVIRONMENT


class SQLAnywhereABC:
    @abstractmethod
    def __init__(self, stage: ENVIRONMENT) -> None:
        raise NotImplementedError

    @abstractmethod
    def connect(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def close_all(self) -> None:
        raise NotImplementedError
