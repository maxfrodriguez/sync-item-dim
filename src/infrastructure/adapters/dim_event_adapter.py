from datetime import datetime
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SAEvent


class DimEventAdapter(SAEvent):
    def __init__(self, event_hash: str, next_id: int, shipment_id: int, **kwargs):
        super().__init__(**kwargs)
        self.created_at = datetime.utcnow().replace(second=0, microsecond=0)
        self.event_hash = event_hash
        self.next_id = next_id
        self.shipment_id = shipment_id