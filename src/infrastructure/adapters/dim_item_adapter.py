from datetime import datetime
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SAItems


class DimItemAdapter(SAItems):
    def __init__(self, next_id: int, **kwargs):
        super().__init__(**kwargs)
        self.next_id = next_id
        self.created_at = datetime.utcnow().replace(second=0, microsecond=0)