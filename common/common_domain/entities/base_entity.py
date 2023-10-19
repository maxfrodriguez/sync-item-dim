from typing import Any

from common.common_domain.value_object.base_id import BaseId


class BaseEntity:
    def __init__(self, id: BaseId):
        self.__id: BaseId = id

    @property
    def id(self):
        return self.__id

    def __str__(self):
        return self.__id

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, BaseEntity):
            return self.__id == other.id
        return False
