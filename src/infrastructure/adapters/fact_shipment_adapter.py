from datetime import datetime
from src.infrastructure.cross_cutting.shipment_helper import get_template_id
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SAFactShipment


class FactShipmentAdapter(SAFactShipment):
    def __init__(self, shipment_hash: str ,**kwargs):
        self.shipment_hash = shipment_hash
        super().__init__(**kwargs)
        self.post_init()

    def post_init(self):
        self.template_id = get_template_id(value=self.template_id)
        self.hash = self.shipment_hash
        self.created_at = datetime.utcnow().replace(second=0, microsecond=0)