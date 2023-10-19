from datetime import datetime
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SATemplate


class DimTemplateAdapter(SATemplate):
    def __init__(self, **kwargs):
        del kwargs["division"]
        del kwargs["id"]
        super().__init__(**kwargs)
        self.created_at = datetime.utcnow().replace(second=0, microsecond=0)