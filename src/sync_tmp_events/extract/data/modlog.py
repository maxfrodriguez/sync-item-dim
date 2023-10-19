from dataclasses import dataclass
from datetime import datetime

@dataclass
class ModLog:
    mod_lowest_version : int = None
    mod_highest_version : int = None
    num_fact_movements_loaded : int = None
    created_at : datetime = None