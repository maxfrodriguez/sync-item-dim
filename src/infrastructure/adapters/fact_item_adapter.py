from datetime import datetime
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SAFactItems, SAItems


class FactItemAdapter(SAFactItems):
    def __init__(self, gui_id: str,**kwargs):
        super().__init__(**kwargs)
        self.id = gui_id
        self.created_at = datetime.utcnow().replace(second=0, microsecond=0)