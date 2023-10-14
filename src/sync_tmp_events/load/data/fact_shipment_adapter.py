from datetime import datetime
from src.infrastructure.cross_cutting.shipment_helper import get_template_id
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SAFactShipment


class FactShipmentAdapter(SAFactShipment):
    def __init__(self, sk_last_shipment_id, **kwargs):
        super().__init__(**kwargs)
        self.sk_last_shipment_id = sk_last_shipment_id
        self.template_id = get_template_id(value=self.template_id)
        self.created_at = datetime.strptime(kwargs.get('mod_created_pt_dt'), '%Y-%m-%d %H:%M:%S.%f')