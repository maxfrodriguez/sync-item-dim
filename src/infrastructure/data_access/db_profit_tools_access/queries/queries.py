from typing import Final

MODLOG_QUERY: Final[str] = """
SELECT DISTINCT
  ds.ds_id,
  ds.ds_status,
  MAX(md_ds.mod_id) AS r_mod_id
FROM [DBA].[modlog_ship] md_ds
INNER JOIN [DBA].[modlog] md 
  ON  md.mod_id = md_ds.mod_id
LEFT JOIN [DBA].[disp_ship] ds
  ON ds.ds_id = md_ds.ds_id
GROUP BY ds.ds_id,
         ds.ds_status
HAVING MAX(md_ds.mod_id) > {0}
OR (ds.ds_status = 'K' AND MAX(md.mod_datetime) > '{1}')
ORDER BY ds.ds_status DESC
"""

NEXT_ID_WH: Final[
    str
] = """
SELECT MAX(id) + 1 as NextId 
FROM [{}]
"""

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


# COMPLETE_EVENT_QUERY: Final[
#     str
# ] = """
#  SELECT DISTINCT
#     ds.ds_id,
#     de.de_id,
#     CASE
#         WHEN de_event_type = 'H' THEN 'HOOK'
#         WHEN de_event_type = 'D' THEN 'DELIVER'
#         WHEN de_event_type = 'P' THEN 'PICKUP'
#         WHEN de_event_type = 'R' THEN 'DROP'
#         WHEN de_event_type = 'N' THEN 'DISMOUNT'
#         WHEN de_event_type = 'M' THEN 'MOUNT'
#         ELSE 'NOT FOUND'
#     END AS de_type,
#     de.de_driver,
#     ca.carrierid AS carrier_id,
#     de.de_site AS location_id,
#     c1.co_name AS location_name,
#     c1.co_zip AS location_zip,
#     de.de_ship_seq,
#     de.de_conf,
#     de.Routable AS de_routable,
#     de.de_apptdate + de.de_appttime AS de_appointment,
#     de.de_arrdate + de.de_arrtime AS de_arrival,
#     CASE WHEN de_apptdate IS NULL THEN de_arrdate
#     ELSE de_apptdate
#     END + de_deptime AS de_departure,
#     de.earliestDate + de.earliestTime AS de_earliest,
#     de.latestDate + de.latestTime AS de_latest,
#     (em_fn + ' ' + em_ln) AS driver_name
# FROM [DBA].[disp_ship] ds
# INNER JOIN [DBA].[disp_events] de
#     ON de.de_shipment_id = ds.ds_id
# LEFT JOIN [DBA].[companies] c1
#     ON c1.co_id = de.de_site
# LEFT JOIN [DBA].[event_carrier_assignments] ca
#     ON ca.de_id = de.de_id
# LEFT JOIN dba.equip2_info eqp
#     ON de.de_tractor = eqp.eq_id
# LEFT JOIN [DBA].employees
#     ON employees.em_id = de.de_driver
# WHERE ds.ds_id IN ({})
# ORDER BY de.de_ship_seq ASC
# """


COMPLETE_SHIPMENT_QUERY: Final[
    str
] = """
SELECT DISTINCT
  ds.ds_id,
  ds.ds_status,
  md_ds.mod_value AS template_id,
  CASE ds.ds_status
    WHEN 'A' THEN 'Template'
    WHEN 'C' THEN 'Cancelled'
    WHEN 'D' THEN 'Declined'
    WHEN 'E' THEN 'Quoted'
    WHEN 'F' THEN 'Offered'
    WHEN 'H' THEN 'Pending'
    WHEN 'K' THEN 'Open'
    WHEN 'N' THEN 'Authorized'
    WHEN 'Q' THEN 'Audit Required'
    WHEN 'T' THEN 'Audited'
    WHEN 'W' THEN 'Billed'
    ELSE 'Unknown'
  END AS ds_status_text,
  ds.MasterBL,
  ds.ds_hazmat,
  ds.ds_expedite,
  ds.ds_lh_totamt,
  ds.ds_bill_charge,
  ds.ds_ac_totamt,
  ds.ds_parentid,
  cs.linkedeq1type,
  cs.linkedeq1ref,
  cs.linkedeq1leaseline,
  cs.linkedeq1leasetype,
  cs.linkedeq2type,
  cs.linkedeq2ref,
  cs.linkedeq2leaseline,
  cs.linkedeq2leasetype,
  c1.co_id AS customer_id,
  c1.co_name AS customer_name,
  c1.co_addr1 AS customer_address,
  c1.co_city AS customer_city,
  c1.co_state AS customer_state,
  c1.co_country AS customer_country,
  c1.co_zip AS customer_zip,
  cs.cs_routed,
  cs.cs_assigned,
  cs.cs_completed,
  cs.cs_event_count,
  (CASE
    WHEN ds.movecode = 'E' THEN ds.pickupbydate
    ELSE ds.delbydate
  END) AS del_pk_date,
  (CASE
    WHEN ds.movecode = 'E' THEN ds.pickupbytime
    ELSE ds.delbytime
  END) AS del_pk_time,
  ds.ds_origin_id,
  org.co_city as org_city,
  org.co_name AS org_name,
  org.co_zip AS org_zip,
  ds.ds_findest_id,
  des.co_name AS destination_name,
  des.co_city as destination_city,
  des.co_zip AS destinantion_zip,
  (CASE
    WHEN ds.movecode = 'I' THEN 'IMPORT'
    WHEN ds.movecode = 'E' THEN 'EXPORT'
    WHEN ds.movecode = 'O' THEN 'ONEWAY'
    ELSE 'NOT FOUND'
  END) AS TmpType,
  eqpC.eq_id AS eq_c_info_id,
  eqinfoC.eq_type AS eq_c_info_eq_type,
  eqinfoC.eq_ref AS container_id,
  eqlitC.Line AS eq_c_info_line,
  eqlitC.Type AS eq_c_info_type,
  eqpH.eq_id AS eq_h_info_id,
  eqinfoH.eq_type AS eq_h_info_eq_type,
  eqinfoH.eq_ref AS chassis_id,
  ds.custom9 AS st_custom_9,
  ds.custom1 AS quote_id,
  di_hcap.Note AS quote_note,
  md.mod_datetime as mod_created_pt_dt
FROM [DBA].[disp_ship] ds
INNER JOIN [DBA].[companies] c1
  ON c1.co_id = ds.ds_billto_id
INNER JOIN [DBA].[companies] org
  ON org.co_id = ds.ds_origin_id
INNER JOIN [DBA].[companies] des
  ON des.co_id = ds.ds_findest_id
LEFT JOIN [DBA].current_shipments cs
  ON cs.cs_id = ds.ds_id
LEFT JOIN [DBA].[modlog_ship] md_ds
  ON ds.ds_id = md_ds.ds_id
LEFT JOIN [DBA].[modlog] md
  ON md.mod_id = md_ds.mod_id
LEFT JOIN [DBA].[equip2_shiplinks] eqpC
  ON eqpC.ds_id = ds.ds_id
INNER JOIN [DBA].[equip2_info] eqinfoC
  ON eqinfoC.eq_id = eqpC.eq_id
  AND eqinfoC.eq_type = 'C'
LEFT JOIN [DBA].[disp_items] di_hcap 
  ON ds.ds_id = di_hcap.di_shipment_id 
  AND di_hcap.AmountType = 1
LEFT JOIN [DBA].[equip2_leaseInfo_EP] eqliC
    ON eqinfoC.eq_id = eqliC.oe_id
LEFT JOIN [DBA].[EquipmentLeaseType] eqlitC
    ON eqlitC.id = eqliC.fkequipmentleasetype
LEFT JOIN [DBA].[equip2_shiplinks] eqpH
  ON eqpH.ds_id = ds.ds_id
INNER JOIN [DBA].[equip2_info] eqinfoH
  ON eqinfoH.eq_id = eqpH.eq_id
  AND eqinfoH.eq_type = 'H'
WHERE ds.ds_id IN ({})
AND md_ds.mod_type = 'C'
ORDER BY ds.ds_id
"""

SHIPMENT_SPLITTED_QUERY : Final [str]= """
SELECT DISTINCT
    ds.ds_id
    , ds.ds_status
    , md_ds.mod_value as template_id
    , CASE ds.ds_status
        WHEN 'A' THEN 'Template'
        WHEN 'C' THEN 'Cancelled'
        WHEN 'D' THEN 'Declined'
        WHEN 'E' THEN 'Quoted'
        WHEN 'F' THEN 'Offered'
        WHEN 'H' THEN 'Pending'
        WHEN 'K' THEN 'Open'
        WHEN 'N' THEN 'Authorized'
        WHEN 'Q' THEN 'Audit Required'
        WHEN 'T' THEN 'Audited'
        WHEN 'W' THEN 'Billed'
        ELSE 'Unknown'
    END AS ds_status_text
	, (CASE
		WHEN ds.ds_ship_type = 2201 THEN 'SNE'
		WHEN ds.ds_ship_type = 2205 THEN 'LBS'
		WHEN ds.ds_ship_type = 2206 THEN 'DBS'
        WHEN ds.ds_ship_type = 2208 THEN 'DBS-SAP'
		ELSE 'NOT FOUND' END
	) AS division
    , ds.MasterBL
    , ds.ds_hazmat
    , ds.ds_expedite
    , ds.ds_lh_totamt
    , ds.ds_bill_charge
    , ds.ds_ac_totamt
    , ds.ds_parentid
    -- , cs.linkedeq1type
    -- , cs.linkedeq1ref
    -- , cs.linkedeq1leaseline
    -- , cs.linkedeq1leasetype
    -- , cs.linkedeq2type
    -- , cs.linkedeq2ref
    -- , cs.linkedeq2leaseline
    -- , cs.linkedeq2leasetype
    , c1.co_id AS customer_id
    , c1.co_name AS customer_name
    , c1.co_addr1 AS customer_address
    , c1.co_city AS customer_city
    , c1.co_state AS customer_state
    , c1.co_country AS customer_country
    , c1.co_zip AS customer_zip
    , cs.cs_routed
    , cs.cs_assigned
    , cs.cs_completed
    , cs.cs_event_count
    , (CASE
        WHEN ds.movecode = 'E' THEN ds.pickupbydate
        ELSE ds.delbydate
    END) AS del_pk_date
    , (CASE
        WHEN ds.movecode = 'E' THEN ds.pickupbytime
        ELSE ds.delbytime
    END) AS del_pk_time
	, (CASE
        WHEN ds.movecode = 'E' THEN REPLACE(ds.pickupbyby, ':', '')
        ELSE REPLACE(ds.delbyby, ':', '')
    END) AS del_appt_time
    , ds.ds_origin_id
    , org.co_name AS org_name
	, org.co_city as org_city
    , org.co_zip AS org_zip
    , ds.ds_findest_id
    , des.co_name AS destination_name
	, des.co_city as destination_city
    , des.co_zip AS destinantion_zip
    , (CASE
        WHEN ds.movecode = 'I' THEN 'IMPORT'
        WHEN ds.movecode = 'E' THEN 'EXPORT'
        WHEN ds.movecode = 'O' THEN 'ONEWAY'
        ELSE 'NOT FOUND'
    END) AS TmpType
	, ds.custom9 as st_custom_9
	, ds.custom1 AS quote_id
    , di_hcap.Note AS quote_note
    , di_hcap.RateCodename
	, md.mod_datetime as mod_created_pt_dt
FROM [DBA].[disp_ship] ds
	LEFT JOIN [DBA].[companies] c1 ON c1.co_id = ds.ds_billto_id
	LEFT JOIN [DBA].[companies] org ON org.co_id = ds.ds_origin_id
	LEFT JOIN [DBA].[companies] des ON des.co_id = ds.ds_findest_id
	LEFT JOIN [DBA].[disp_items] di_hcap ON ds.ds_id = di_hcap.di_shipment_id and di_hcap.AmountType = 1
	LEFT JOIN [DBA].current_shipments cs ON cs.cs_id = ds.ds_id
	LEFT JOIN [DBA].[modlog_ship] md_ds on ds.ds_id = md_ds.ds_id
	LEFT JOIN [DBA].[modlog] md on md.mod_id = md_ds.mod_id

WHERE 
    ds.ds_id IN ({})
    and
    md_ds.mod_type = 'C'
ORDER BY ds.ds_id
	, di_hcap.RateCodename
""" 

# SHIPMENT_EQUIPMENT_SPLITTED_QUERY: Final [str] = """
# SELECT DISTINCT
#     ds.ds_id
#     , eqpC.eq_id AS eq_c_info_id
#     , eqinfoC.eq_type AS eq_c_info_eq_type
#     , eqinfoC.eq_ref AS container_id
#     , eqpH.eq_id AS eq_h_info_id
#     , eqinfoH.eq_type AS eq_h_info_eq_type
#     , eqinfoH.eq_ref AS chassis_id
# FROM [DBA].[disp_ship] ds
# 	LEFT JOIN [DBA].[equip2_shiplinks] eqpC ON eqpC.ds_id = ds.ds_id
# 	INNER JOIN [DBA].[equip2_info] eqinfoC ON eqinfoC.eq_id = eqpC.eq_id and eqinfoC.eq_type = 'C' 
# 	LEFT JOIN [DBA].[equip2_shiplinks] eqpH ON eqpH.ds_id = ds.ds_id
# 	INNER JOIN [DBA].[equip2_info] eqinfoH ON eqinfoH.eq_id = eqpH.eq_id and eqinfoH.eq_type = 'H'
# WHERE 
#     ds.ds_id IN ({})
# ORDER BY ds.ds_id
# """

SHIPMENT_EQUIPMENT_SPLITTED_QUERY: Final [str] = """
SELECT DISTINCT
    ds.ds_id
    , eqtype.Line
    , eqtype.Type
    , eq.fkequipmentleasetype
    , eqinfoC.eq_type
    , eqinfoC.eq_ref

FROM [DBA].[disp_ship] ds
    LEFT JOIN [DBA].[equip2_leaseInfo_EP] eq on eq.originationshipment = ds.ds_id
    LEFT JOIN [DBA].[equipmentleasetype] eqtype ON eqtype.id = eq.fkequipmentleasetype
    LEFT JOIN [DBA].[equip2_info] eqinfoC ON eqinfoC.eq_id = eq.oe_id
WHERE 
    ds.ds_id IN ({})
ORDER BY ds.ds_id

"""

SHIPMENTS_CUSTOM_FIELDS_QUERY : Final [str] = """
SELECT DISTINCT
    ds.ds_id
    , ds.ds_status
    , ds.custom1 as ds_custom_1
    , ds.custom2 as ds_custom_2
    , ds.custom3 as ds_custom_3
    , ds.custom4 as ds_custom_4
    , ds.custom5 as ds_custom_5
    , ds.custom6 as ds_custom_6
    , ds.custom7 as ds_custom_7
    , ds.custom8 as ds_custom_8
    , ds.custom9 as ds_custom_9
    , ds.custom10 as ds_custom_10
    , c1.custom1 as client_custom_1
    , c1.custom2 as client_custom_2
    , c1.custom3 as client_custom_3
    , c1.custom4 as client_custom_4
    , c1.custom5 as client_custom_5
    , c1.custom6 as client_custom_6
    , c1.custom7 as client_custom_7
    , c1.custom8 as client_custom_8
    , c1.custom9 as client_custom_9
    , c1.custom10 as client_custom_10
    , org.custom1 as origin_custom_1
    , org.custom2 as origin_custom_2
    , org.custom3 as origin_custom_3
    , org.custom4 as origin_custom_4
    , org.custom5 as origin_custom_5
    , org.custom6 as origin_custom_6
    , org.custom7 as origin_custom_7
    , org.custom8 as origin_custom_8
    , org.custom9 as origin_custom_9
    , org.custom10 as origin_custom_10
    , des.custom1 as destination_custom_1
    , des.custom2 as destination_custom_2
    , des.custom3 as destination_custom_3
    , des.custom4 as destination_custom_4
    , des.custom5 as destination_custom_5
    , des.custom6 as destination_custom_6
    , des.custom7 as destination_custom_7
    , des.custom8 as destination_custom_8
    , des.custom9 as destination_custom_9
    , des.custom10 as destination_custom_10
	, ca_rp.amount AS carrier_pay 
FROM [DBA].[disp_ship] ds
	LEFT JOIN [DBA].[companies] c1 ON c1.co_id = ds.ds_billto_id
	LEFT JOIN [DBA].[companies] org ON org.co_id = ds.ds_origin_id
	LEFT JOIN [DBA].[companies] des ON des.co_id = ds.ds_findest_id
    INNER JOIN dba.disp_items di_hcap ON ds.ds_id = di_hcap.di_shipment_id and di_hcap.AmountType = 1
	LEFT JOIN dba.revenuesplitspay ca_rp ON ca_rp.itemid = di_hcap.di_item_id AND ca_rp.amount > 0

WHERE 
    ds.ds_id IN ({})
"""

STOPS_QUERY: Final = """
SELECT
  events_external_references.external_id AS pt_event_id,
  events."id" AS bf_event_id,
  stops."id" AS stop_id,
  locations.name AS Location_event_name,
  locations_geo_fences.geo_fence_id,
  drivers."id" AS bf_driver_id,
  dv_pt.external_id AS pt_driver_id,
  dv_geo.external_id AS geotab_driver_id,
  drivers."name" AS driver_name,
  gps_devices."id" AS bf_truck_id,
  gps_devices_external_references.external_id AS geotab_truck_id,
  gps_devices."name" AS truck_name,
  stops__events.gps_arrival_dt AS arrival_dt,
  stops__events.gps_departure_dt AS departure_dt
FROM supra.events

INNER JOIN supra.shipments
 ON shipments.id = events.shipment_id
INNER JOIN supra.events_external_references
  ON events_external_references.linked_object_id = events."id"

INNER JOIN supra.locations
  ON events.location_Id = locations.Id
LEFT JOIN supra.locations_geo_fences
  ON locations.Id = locations_geo_fences.location_id
LEFT JOIN supra.geo_fences
  ON locations_geo_fences.geo_fence_id = geo_fences.Id

LEFT JOIN supra.drivers
  ON drivers."id" = events.driver_id
  
left join supra.drivers_external_references dv_pt on drivers.Id = (
  select 
		Id
	from 
		supra.drivers_external_references lim
	where
		lim.linked_object_id = drivers.Id and lim.connection_id = 5	
	limit 1
)
left join supra.drivers_external_references dv_geo on drivers.Id = 
(
	select
		Id
	from 
		supra.drivers_external_references lim
	where
		lim.linked_object_id = drivers.Id and lim.connection_id = 7		
	limit 1
)

LEFT JOIN supra.stops__events
  ON stops__events.event_id = events."id"
  
LEFT JOIN supra.stops
  ON stops."id" = stops__events.stop_id
  
LEFT JOIN supra.gps_devices
  ON gps_devices."id" = stops.gps_device_id
  
LEFT JOIN supra.gps_devices_external_references
  ON gps_devices_external_references.linked_object_id = gps_devices."id"
  AND gps_devices_external_references.connection_id = 7
  
WHERE shipments.reference IN ({})
  AND locations_geo_fences.deactivated_dt is Null
"""


CLOSED_MODLOGS_QUERY: Final = """
SELECT DISTINCT
  ds.ds_id,
  ds.ds_status,
  MAX(md_ds.mod_id) AS r_mod_id
FROM DBA.disp_ship ds
LEFT JOIN [DBA].[modlog_ship] md_ds
  ON ds.ds_id = md_ds.ds_id
LEFT JOIN [DBA].[modlog] md
  ON md.mod_id = md_ds.mod_id
WHERE md.mod_datetime BETWEEN '2023-09-01 14:00:00' AND '2023-09-01 14:15:59'
-- AND mod_type = 'C'
-- WHERE ds.ds_id IN (140250,140496)
AND ds.ds_status NOT IN ('A')
GROUP BY ds.ds_id,
  ds.ds_status
"""


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


WAREHOUSE_EVENTS: Final[str] = """
WITH last_created_event
AS (SELECT DISTINCT
    de_id,
    MAX(id) AS max_Id
FROM events
GROUP BY de_id)
SELECT
    ev.de_id,
    ev.id,
    ev.hash
FROM events ev
INNER JOIN last_created_event
    ON last_created_event.de_id = ev.de_id
    AND last_created_event.max_Id = ev.id
WHERE ev.de_id IN ({})
ORDER BY ev.de_id
"""


WAREHOUSE_STOPS: Final[str] = """
WITH last_created_tmp
AS (SELECT DISTINCT
    pt_event_id,
    MAX(created_at) AS max_created_at
FROM stops
GROUP BY pt_event_id)
SELECT
    MAX
    (stops.id) AS id,
    stops.pt_event_id,
    stops.hash,
    stops.created_at
FROM stops
INNER JOIN last_created_tmp dt
    ON dt.pt_event_id = stops.pt_event_id
    AND dt.max_created_at = stops.created_at
WHERE stops.pt_event_id IN ({})
GROUP BY stops.pt_event_id,
         stops.hash,
         stops.created_at
"""

PT_DRIVERS_QUERY: Final[str] = """
SELECT DISTINCT
    di_id,
    (em_fn + ' ' + em_ln) AS name,
    (CASE
        WHEN em_status = 'K' THEN 'ACTIVE'
        WHEN em_status = 'D' THEN 'DEACTIVATED'
        ELSE 'NOT FOUND'
    END) AS status,
    di_Fleet as fleet
FROM [DBA].driverinfo
INNER JOIN [DBA].employees
    ON em_id = di_id
WHERE di_id IN ({})
"""

WH_DRIVERS_QUERY: Final[str] = """
SELECT [di_id]
      ,[name]
      ,[status]
      ,[fleet]
      ,[id]
      ,[created_at]
FROM  [dbo].[drivers]
WHERE [di_id] = {}
"""


FACT_CUSTOMERS_KPI_QUERY: Final[str] = """
WITH last_shipment
AS (SELECT
    ds.ds_id,
    MAX(created_at) AS created_at
FROM shipments ds
GROUP BY ds.ds_id)
SELECT
    tm.customer_id,
    tm.customer_name,
    tm.ds_id,
    ds.ds_id,
    ds.ds_bill_charge,
    ds.created_at,
    SUM(mov.kpi_distance_time) AS distance_time
--    , count(ds.ds_id) as 'Number Of shipments'
--    , SUM(ds.ds_bill_charge) as 'Total Revenue'
--    , MONTH(ds.created_at) as MONTH
FROM templates tm
INNER JOIN shipments ds
    ON ds.template_id = tm.template_id
INNER JOIN last_shipment last
    ON ds.ds_id = last.ds_id
    AND ds.created_at = last.created_at
INNER JOIN movements mov
    ON mov.sk_shipment_id_fk = ds.id

GROUP BY tm.customer_id,
         tm.customer_name,
         tm.ds_id,
         ds.ds_id,
         ds.ds_bill_charge,
         ds.created_at

ORDER BY ds.created_at DESC
"""

ITEMS_QUERY: Final[str] = """
SELECT DISTINCT
    ds.ds_id
    , di_revenue.di_item_id
    , di_revenue.AmountType AS amount_type
    , amty.Name AS name
    , di_revenue.RateCodename AS rate_code_name
    , di_revenue.di_description
    , di_revenue.di_our_itemamt
    , di_revenue.di_pay_itemamt 
    , di_revenue.di_qty AS di_quantity
    , di_revenue.LastRatedBy AS last_rated_by
    , di_revenue.Taglist AS tag_list
    , di_revenue.Note AS note
    
FROM [DBA].[disp_ship] ds
INNER JOIN dba.disp_items di_revenue ON ds.ds_id = di_revenue.di_shipment_id
inner join dba.AmountType amty on amty.Id = di_revenue.AmountType
WHERE 
    ds.ds_id IN ({})
ORDER BY 
    ds.ds_id
"""