from dataclasses import dataclass
from datetime import datetime


@dataclass
class EventPTDTO:
    ds_id: int
    de_id: int
    de_type: str  # Corresponds to the CASE statement
    de_driver: int
    carrier_id: int
    location_id: int
    location_name: str
    location_zip: str
    de_ship_seq: int
    de_conf: str
    de_routable: bool
    de_appointment_dt: datetime
    de_appointment_tm: datetime
    de_arrival_dt: datetime
    de_arrival_tm: datetime
    de_departure_dt: datetime
    de_departure_tm: datetime
    de_earliest_dt: datetime
    de_earliest_tm: datetime
    de_latest_dt: datetime
    de_latest_tm: datetime
    driver_name: str  # Concatenation of em_fn and em_ln
    hash: str = None

    def calculate_datetime_fields(self):
        #WH Model expect datetime fields to be in the format: 'YYYY-MM-DD HH:MM:SS.mmm' we need to sum the date and time fields from profit tools
        if self.de_appointment_tm:
            self.de_appointment = self.de_appointment_dt + ' ' + self.de_appointment_tm
        else:
            self.de_appointment = self.de_appointment_dt + ' 00:00:00.000' if self.de_appointment_dt else None

        if self.de_arrival_tm:
            self.de_arrival = self.de_arrival_dt + ' ' + self.de_arrival_tm
        else:
            self.de_arrival = self.de_arrival_dt + ' 00:00:00.000' if self.de_arrival_dt else None

        if self.de_departure_tm:
            self.de_departure = self.de_departure_dt + ' ' + self.de_departure_tm
        else:
            self.de_departure = self.de_departure_dt + ' 00:00:00.000' if self.de_departure_dt else None 

        if self.de_earliest_tm:
            self.de_earliest = self.de_earliest_dt + ' ' + self.de_earliest_tm
        else:
            self.de_earliest = self.de_earliest_dt + ' 00:00:00.000' if self.de_earliest_dt else None

        if self.de_latest_tm:
            self.de_latest = self.de_latest_dt + ' ' + self.de_latest_tm
        else:
            self.de_latest = self.de_latest_dt + ' 00:00:00.000' if self.de_latest_dt else None