from datetime import datetime
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SAFactEvent


class DimFactEventAdapter(SAFactEvent):
    def __init__(self, **kwargs):
        del kwargs["de_appointment_dt"]
        del kwargs["de_appointment_tm"]
        del kwargs["de_arrival_dt"]
        del kwargs["de_arrival_tm"]
        del kwargs["de_departure_dt"]
        del kwargs["de_departure_tm"]
        del kwargs["de_earliest_dt"]
        del kwargs["de_earliest_tm"]
        del kwargs["de_latest_dt"]
        del kwargs["de_latest_tm"]
        super().__init__(**kwargs)
        #self.id = kwargs.get("id")
        self.created_at = datetime.utcnow().replace(second=0, microsecond=0)