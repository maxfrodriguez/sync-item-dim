from datetime import datetime
from src.infrastructure.cross_cutting.shipment_helper import get_template_id
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SAShipment


class DimShipmentAdapter(SAShipment):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        #self.id = next_id
        self.template_id = get_template_id(value=self.template_id)
        self.created_at = datetime.utcnow().replace(second=0, microsecond=0)
        #self.next_id = next_id