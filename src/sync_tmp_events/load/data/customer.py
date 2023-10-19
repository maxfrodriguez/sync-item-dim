from dataclasses import dataclass, asdict

@dataclass
class Customer:
    tmp: int
    template_id: int
    customer_id: int

    def to_dict(self):
        return asdict(self)