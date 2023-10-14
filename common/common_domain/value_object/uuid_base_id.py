from typing import Union
from uuid import UUID, uuid4

from common.common_domain.value_object.base_id import BaseId


class UUIDBaseId(BaseId):
    def __init__(self, id: Union[UUID, str] = None):
        if isinstance(id, str):
            self.__value_id = UUID(hex=id, version=4)
        else:
            if not id:
                self.__value_id = uuid4()
            else:
                self.__value_id = id

    def __str__(self) -> str:
        return str(self.__value_id)

    def __hash__(self):
        return hash(self.__value_id)

    def __eq__(self, other):
        if isinstance(other, UUIDBaseId):
            return self.__value_id == other.__value_id
        return False
