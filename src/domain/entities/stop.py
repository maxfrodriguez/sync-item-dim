from dataclasses import dataclass
from datetime import datetime
from typing import List

from typing_extensions import Self


@dataclass
class Stop:
    bf_event_id: int
    pt_event_id: int
    stop_id: int = None
    location_event_name: str = None
    geo_fence_id: int = None
    bf_driver_id: int = None
    pt_driver_id: int = None
    geotab_driver_id: str = None
    driver_name: str = None
    bf_truck_id: int = None
    geotab_truck_id: str = None
    truck_name: str = None
    arrival_dt: datetime = None
    departure_dt: datetime = None
    id: int = None
    duplicate: bool = None
    duplicate_counter: int = None

    def __post_init__(self) -> None:
        try:
            for attr in ["arrival_dt", "departure_dt"]:
                datetime_value = getattr(self, attr)
                if datetime_value and not isinstance(datetime_value, datetime):
                    setattr(self, attr, datetime.strptime(datetime_value, "%Y-%m-%d %H:%M:%S.%f"))
        except ValueError:
            for attr in ["arrival_dt", "departure_dt"]:
                datetime_value = getattr(self, attr)
                if datetime_value and not isinstance(datetime_value, datetime):
                    setattr(self, attr, datetime.strptime(datetime_value, "%Y-%m-%dT%H:%M:%S"))

    def create_stops(data: List[dict]) -> List[Self]:
        stops = [Stop(**d) for d in data]
        return stops
