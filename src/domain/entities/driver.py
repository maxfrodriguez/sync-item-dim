from dataclasses import dataclass

@dataclass
class Driver:
    di_id: int
    name: str
    status: str
    fleet: str