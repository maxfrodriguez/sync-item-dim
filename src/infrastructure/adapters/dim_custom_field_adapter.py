from datetime import datetime
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SACustomFields


class DimCustomFieldAdapter(SACustomFields):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.created_at = datetime.utcnow().replace(second=0, microsecond=0)