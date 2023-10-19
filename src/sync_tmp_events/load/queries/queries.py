from typing import Final


WAREHOUSE_SHIPMENTS: Final[str] = """
    WITH last_created_tmp
    AS (SELECT DISTINCT
        ds_id,
        MAX(created_at) AS max_created_at
    FROM shipments
    GROUP BY ds_id)
    SELECT
        MAX
        (shipments.id) AS id,
        shipments.ds_id,
        shipments.hash,
        shipments.created_at
    FROM shipments
    INNER JOIN last_created_tmp dt
        ON dt.ds_id = shipments.ds_id
        AND dt.max_created_at = shipments.created_at
    WHERE shipments.ds_id IN ({})
    GROUP BY shipments.ds_id,
            shipments.hash,
            shipments.created_at
    """

NEXT_ID_WH: Final[
    str
] = """
SELECT MAX(id) + 1 as NextId 
FROM [{}]
"""