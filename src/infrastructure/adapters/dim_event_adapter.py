from datetime import datetime
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SAEvent


class DimEventAdapter(SAEvent):
    def __init__(self, hash: str, next_id: int, shipment_id: int, **kwargs):
        self.hash = hash
        self.next_id = next_id
        self.shipment_id = shipment_id
        super().__init__(**kwargs)
        self.post_init()

    def post_init(self):
        self.hash = self.hash
        self.id = self.next_id
        self.shipment_id = self.shipment_id
        self.created_at = datetime.utcnow().replace(second=0, microsecond=0)