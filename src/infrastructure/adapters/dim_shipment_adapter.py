from datetime import datetime
from src.infrastructure.cross_cutting.shipment_helper import get_template_id
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SAShipment


class DimShipmentAdapter(SAShipment):
    def __init__(self, shipment_hash: str, next_id: int,**kwargs):
        self.shipment_hash = shipment_hash
        self.next_id = next_id
        super().__init__(**kwargs)
        self.post_init()

    def post_init(self):
        self.template_id = get_template_id(value=self.template_id)
        self.hash = self.shipment_hash
        self.id = self.next_id
        self.created_at = datetime.utcnow().replace(second=0, microsecond=0)