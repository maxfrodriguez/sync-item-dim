from datetime import datetime
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SAFactEvent


class FactEventAdapter(SAFactEvent):
    def __init__(self, event_hash: str, shipment_id: int, **kwargs):
        super().__init__(**kwargs)
        self.created_at = datetime.utcnow().replace(second=0, microsecond=0)
        self.event_hash = event_hash
        self.shipment_id = shipment_id