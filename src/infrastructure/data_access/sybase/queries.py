from typing import final

FIRST_QUERY: final = """
SELECT DISTINCT
  ds.ds_id,
  ds.ds_status,
  MAX(md_ds.mod_id) AS r_mod_id
FROM [DBA].[disp_ship] ds
LEFT JOIN [DBA].[modlog_ship] md_ds
  ON ds.ds_id = md_ds.ds_id
WHERE ds.ds_status = 'K'
OR (ds.ds_status IN ('N', 'Q', 'T', 'W')
AND md_ds.mod_id IS NOT NULL
)
GROUP BY ds.ds_id,
         ds.ds_status
HAVING ds.ds_status = 'K'
OR MAX(md_ds.mod_id) > 3519841
ORDER BY ds.ds_status DESC
"""
