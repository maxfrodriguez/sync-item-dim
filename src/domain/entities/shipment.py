from dataclasses import dataclass, field
from typing import List

from .event import Event
from .movement import Movement


@dataclass
class Shipment:
    ds_id: int
    modlog: int

    events: List[Event] = field(default_factory=list)
    movements: List[Movement] = field(default_factory=list)
    id: int = None
    tmp_type: str = None
    has_changed_events: bool = False
    has_changed_stops: bool = False
