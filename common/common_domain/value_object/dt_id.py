from datetime import datetime
from typing import Union

from common.common_domain.dt_converter import DTConverter
from common.common_domain.value_object.base_id import BaseId


class DTId(BaseId):
    def __init__(self, id: Union[datetime, str] = None):
        if isinstance(id, str):
            self.__value_id = id.replace("-", "")
        elif isinstance(id, datetime):
            self.__value_id = DTConverter.split_dt(dt=id)[0].replace("-", "")
        else:
            self.__value_id = DTConverter.split_dt(dt=DTConverter.utc_to_pst(dt=DTConverter.utc_now()))[0].replace(
                "-", ""
            )

    def __str__(self) -> str:
        return self.__value_id

    def __hash__(self):
        return hash(self.__value_id)

    def __eq__(self, other):
        if isinstance(other, DTId):
            return self.__value_id == other.__value_id
        return False
