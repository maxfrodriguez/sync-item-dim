from dataclasses import dataclass, asdict
from datetime import datetime

@dataclass
class CustomField:
    created_at: datetime.utcnow()
    id: int = None
    sk_id_shipment_fk: int = None

    def to_dict(self):
        return asdict(self)