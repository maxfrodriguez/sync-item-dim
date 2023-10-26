from dataclasses import dataclass, asdict

@dataclass
class ShipmentsChanged:
    tmp: int
    template_id: int
    customer_id: int

    def to_dict(self):
        return asdict(self)