import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import List

from .distance_vo import DistanceVO
from .event import Event


@dataclass
class _MovementParts:
    event_origin: Event
    event_destination: Event
    id: int = 0
    real_distance_vo: DistanceVO = None
    calc_distance_vo: DistanceVO = None


@dataclass
class Movement:
    def __created_at() -> datetime:
        return datetime.utcnow().replace(minute=0, second=0, microsecond=0)

    # shipment: Shipment
    event_origin: Event
    event_destination: Event
    driver: int
    carrier_id: int | None = None
    id: int = 0
    events_amount: int =  None
    real_distance_vo: DistanceVO = None
    calc_distance_vo: DistanceVO = None
    parts: List[_MovementParts] = field(default_factory=list)
    created_at: datetime = field(default_factory=__created_at)
    kpi_driver_adherence: bool = None

    def calc_kpis(self):
        # self.__calculate_distance_vo()
        if self.event_origin.stop and self.event_destination.stop:
            if (
                self.event_origin.stop.arrival_dt and self.event_destination.stop.departure_dt
            ):  # validate the event_origint.de_arraival
                self.__calculate_kpi_driver_adherence()
                self.__calculate_real_distance_vo()

    def attach_part(self, event_origin: Event, event_destination: Event) -> None:
        # if self.parts is None:
        #     self.parts = []

        self.parts.append(_MovementParts(event_origin=event_origin, event_destination=event_destination))

    # def __calculate_distance_vo(self) -> None:
    #    # TODO 4: -> retrieve the calc from google maps between the events.
    #    self.calc_distance_vo = DistanceVO(miles=100, time_in_min=32)

    def __calculate_real_distance_vo(self) -> None:
        try:
            # TODO 6: -> retrieve the calc from the stops related to this events in geotab
            time_in_minutes = self.event_destination.stop.departure_dt - self.event_origin.stop.arrival_dt
            if  self.event_destination.stop.duplicate:
                time_in_minutes = time_in_minutes//self.event_destination.stop.duplicate_counter
                
            self.real_distance_vo = DistanceVO(time_in_min=time_in_minutes.total_seconds() // 60)
        except Exception:
            logging.error(f"We can't calculate the time in minutes: {self.event_origin.id}")

    def __calculate_kpi_driver_adherence(self) -> None:
        if (
            self.event_origin.stop.arrival_dt < self.created_at
            and self.event_destination.stop.departure_dt < self.created_at
        ):
            if (
                self.event_origin.de_arrival.day == self.event_origin.stop.arrival_dt.day
                and self.event_destination.de_departure.day == self.event_destination.stop.departure_dt.day
            ):
                self.kpi_driver_adherence = True
            else:
                self.kpi_driver_adherence = False
