from dataclasses import dataclass
from datetime import datetime

from src.domain.entities.stop import Stop


@dataclass
class Event:
    de_id: int
    de_type: str
    location_id: int
    location_name: str
    location_zip: int
    de_ship_seq: int
    de_conf: str
    de_routable: str
    de_driver: int = None
    carrier_id: int = None
    de_appointment: datetime = None
    de_departure: datetime = None
    de_arrival: datetime = None
    de_earliest: datetime = None
    de_latest: datetime = None
    id: int = None
    driver_name: str = None
    stop: Stop = None

    # def arrival_time(self) -> datetime | None:
    #     if self.stop and self.stop.arrival_dt:
    #         return self.stop.arrival_dt
    #     elif self.de_arrival:
    #         return self.de_arrival
    #     else:
    #         return None
        
    # def departure_time(self) -> datetime:
    #     if self.stop and self.stop.departure_dt:
    #         return self.stop.departure_dt
    #     elif self.de_departure:
    #         return self.de_departure
    #     else:
    #         return None

    def __post_init__(self) -> None:
        try:
            for attr in ["de_appointment", "de_departure", "de_arrival", "de_earliest", "de_latest"]:
                datetime_value = getattr(self, attr)
                if datetime_value and not isinstance(datetime_value, datetime):
                    setattr(self, attr, datetime.strptime(datetime_value, "%Y-%m-%d %H:%M:%S.%f"))
        except ValueError:
            for attr in ["de_appointment", "de_departure", "de_arrival", "de_earliest", "de_latest"]:
                datetime_value = getattr(self, attr)
                if datetime_value and not isinstance(datetime_value, datetime):
                    setattr(self, attr, datetime.strptime(datetime_value, "%Y-%m-%dT%H:%M:%S"))
