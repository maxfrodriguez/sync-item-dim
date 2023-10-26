from typing import Final

COMPLETE_EVENT_QUERY: Final[
    str
] = """
 SELECT DISTINCT
    ds.ds_id,
    de.de_id,
    CASE
        WHEN de_event_type = 'H' THEN 'HOOK'
        WHEN de_event_type = 'D' THEN 'DELIVER'
        WHEN de_event_type = 'P' THEN 'PICKUP'
        WHEN de_event_type = 'R' THEN 'DROP'
        WHEN de_event_type = 'N' THEN 'DISMOUNT'
        WHEN de_event_type = 'M' THEN 'MOUNT'
        ELSE 'NOT FOUND'
    END AS de_type,
    de.de_driver,
    ca.carrierid AS carrier_id,
    de.de_site AS location_id,
    c1.co_name AS location_name,
    c1.co_zip AS location_zip,
    de.de_ship_seq,
    de.de_conf,
    de.Routable AS de_routable,
    de.de_apptdate AS de_appointment_dt,
    de.de_appttime AS de_appointment_tm,
    de.de_arrdate AS de_arrival_dt,
    de.de_arrtime AS de_arrival_tm,
    CASE WHEN de_apptdate IS NULL THEN de_arrdate
    ELSE de_apptdate
    END AS de_departure_dt,
    de_deptime AS de_departure_tm,
    de.earliestDate AS de_earliest_dt,
    de.earliestTime AS de_earliest_tm,
    de.latestDate AS de_latest_dt,
    de.latestTime AS de_latest_tm,
    (em_fn + ' ' + em_ln) AS driver_name
FROM [DBA].[disp_ship] ds
INNER JOIN [DBA].[disp_events] de
    ON de.de_shipment_id = ds.ds_id
LEFT JOIN [DBA].[companies] c1
    ON c1.co_id = de.de_site
LEFT JOIN [DBA].[event_carrier_assignments] ca
    ON ca.de_id = de.de_id
LEFT JOIN dba.equip2_info eqp
    ON de.de_tractor = eqp.eq_id
LEFT JOIN [DBA].employees
    ON employees.em_id = de.de_driver
WHERE ds.ds_id IN ({})
ORDER BY de.de_ship_seq ASC
"""

WAREHOUSE_EVENTS: Final[str] = """
    SELECT
        de_id,
        ds_id,
        [hash]
    from fact_events
    where de_id IN ({})
"""