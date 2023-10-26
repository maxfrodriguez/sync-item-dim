from typing import Final

WAREHOUSE_DELETE_EVENTS: Final[str] = """
    DELETE FROM fact_events
    WHERE de_id IN ({});
"""