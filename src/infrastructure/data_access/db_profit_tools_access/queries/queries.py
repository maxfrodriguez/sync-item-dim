from typing import Final

MODLOG_QUERY: Final[str] = """
SELECT DISTINCT
  ds.ds_id,
  ds.ds_status,
  MAX(md_ds.mod_id) AS r_mod_id
FROM [DBA].[modlog_ship] md_ds
LEFT JOIN [DBA].[disp_ship] ds
  ON ds.ds_id = md_ds.ds_id
GROUP BY ds.ds_id,
         ds.ds_status
HAVING MAX(md_ds.mod_id) > {}
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
    CAST(de.de_apptdate AS datetime) + CAST(de.de_appttime AS datetime) AS de_appointment,
    CAST(de.de_arrdate AS datetime) + CAST(de.de_arrtime AS datetime) AS de_arrival,
    CAST(CAST(CASE WHEN de_apptdate IS NULL THEN de_arrdate
    ELSE de_apptdate
    END AS DATETIME) + CAST(de_deptime AS DATETIME) AS DATETIME) AS de_departure,
    CAST(de.earliestDate AS datetime) + CAST(de.earliestTime AS datetime) AS de_earliest,
    CAST(de.latestDate AS datetime) + CAST(de.latestTime AS datetime) AS de_latest,
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
    , cs.linkedeq1type
    , cs.linkedeq1ref
    , cs.linkedeq1leaseline
    , cs.linkedeq1leasetype
    , cs.linkedeq2type
    , cs.linkedeq2ref
    , cs.linkedeq2leaseline
    , cs.linkedeq2leasetype
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
ORDER BY ds.ds_id, di_hcap.RateCodename DESC
""" 

SHIPMENT_EQUIPMENT_SPLITTED_QUERY: Final [str] = """
SELECT DISTINCT
    ds.ds_id
    , eqpC.eq_id AS eq_c_info_id
    , eqinfoC.eq_type AS eq_c_info_eq_type
    , eqinfoC.eq_ref AS container_id
    , eqpH.eq_id AS eq_h_info_id
    , eqinfoH.eq_type AS eq_h_info_eq_type
    , eqinfoH.eq_ref AS chassis_id
FROM [DBA].[disp_ship] ds
	LEFT JOIN [DBA].[equip2_shiplinks] eqpC ON eqpC.ds_id = ds.ds_id
	INNER JOIN [DBA].[equip2_info] eqinfoC ON eqinfoC.eq_id = eqpC.eq_id and eqinfoC.eq_type = 'C' 
	LEFT JOIN [DBA].[equip2_shiplinks] eqpH ON eqpH.ds_id = ds.ds_id
	INNER JOIN [DBA].[equip2_info] eqinfoH ON eqinfoH.eq_id = eqpH.eq_id and eqinfoH.eq_type = 'H'
WHERE 
    ds.ds_id IN ({})
ORDER BY ds.ds_id
"""

SHIPMENTS_CUSTOM_FIELDS_QUERY : Final [str] = """
SELECT DISTINCT
    ds.ds_id,
    ds.ds_status,
    ds.custom1 AS ds_custom_1,
    ds.custom2 AS ds_custom_2,
    ds.custom3 AS ds_custom_3,
    ds.custom4 AS ds_custom_4,
    ds.custom5 AS ds_custom_5,
    ds.custom6 AS ds_custom_6,
    ds.custom7 AS ds_custom_7,
    ds.custom8 AS ds_custom_8,
    ds.custom9 AS ds_custom_9,
    ds.custom10 AS ds_custom_10,
    c1.custom1 AS client_custom_1,
    c1.custom2 AS client_custom_2,
    c1.custom3 AS client_custom_3,
    c1.custom4 AS client_custom_4,
    c1.custom5 AS client_custom_5,
    c1.custom6 AS client_custom_6,
    c1.custom7 AS client_custom_7,
    c1.custom8 AS client_custom_8,
    c1.custom9 AS client_custom_9,
    c1.custom10 AS client_custom_10,
    org.custom1 AS origin_custom_1,
    org.custom2 AS origin_custom_2,
    org.custom3 AS origin_custom_3,
    org.custom4 AS origin_custom_4,
    org.custom5 AS origin_custom_5,
    org.custom6 AS origin_custom_6,
    org.custom7 AS origin_custom_7,
    org.custom8 AS origin_custom_8,
    org.custom9 AS origin_custom_9,
    org.custom10 AS origin_custom_10,
    des.custom1 AS destination_custom_1,
    des.custom2 AS destination_custom_2,
    des.custom3 AS destination_custom_3,
    des.custom4 AS destination_custom_4,
    des.custom5 AS destination_custom_5,
    des.custom6 AS destination_custom_6,
    des.custom7 AS destination_custom_7,
    des.custom8 AS destination_custom_8,
    des.custom9 AS destination_custom_9,
    des.custom10 AS destination_custom_10
FROM [DBA].[disp_ship] ds
LEFT JOIN [DBA].[companies] c1
    ON c1.co_id = ds.ds_billto_id
LEFT JOIN [DBA].[companies] org
    ON org.co_id = ds.ds_origin_id
LEFT JOIN [DBA].[companies] des
    ON des.co_id = ds.ds_findest_id

WHERE ds.ds_id IN ({})
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
WHERE md.mod_datetime BETWEEN '2023-03-01' AND '2023-03-31'
AND mod_type = 'C'
AND ds.ds_id IN (134790, 134789, 134788, 134787, 134786, 134785, 134783, 134782, 134781, 134780, 134779, 134778, 134777, 134776, 134775, 134774, 134773, 134772, 134771, 134770, 134769, 134768, 134767, 134766, 134765, 134764, 134763, 134762, 134761, 134760, 134759, 134758, 134757, 134756, 134755, 134754, 134753, 134752, 134751, 134750, 134749, 134748, 134747, 134746, 134745, 134744, 134743, 134742, 134741, 134740, 134739, 134738, 134737, 134736, 134735, 134734, 134733, 134732, 134731, 134730, 134729, 134728, 134727, 134726, 134725, 134724, 134723, 134722, 134720, 134719, 134718, 134717, 134716, 134715, 134714, 134713, 134712, 134711, 134710, 134709, 134708, 134707, 134706, 134705, 134704, 134703, 134702, 134701, 134700, 134699, 134698, 134697, 134696, 134695, 134694, 134693, 134692, 134691, 134690, 134689, 134688, 134687, 134686, 134685, 134684, 134683, 134682, 134681, 134680, 134679, 134678, 134677, 134676, 134675, 134674, 134673, 134672, 134671, 134670, 134669, 134668, 134667, 134666, 134665, 134664, 134663, 134662, 134661, 134660, 134659, 134658, 134657, 134656, 134655, 134654, 134653, 134652, 134651, 134650, 134648, 134647, 134646, 134645, 134644, 134643, 134642, 134641, 134640, 134639, 134638, 134637, 134636, 134635, 134634, 134633, 134632, 134631, 134630, 134629, 134628, 134627, 134626, 134625, 134624, 134623, 134622, 134621, 134620, 134619, 134617, 134616, 134615, 134614, 134613, 134612, 134600, 134599, 134598, 134597, 134596, 134595, 134594, 134593, 134592, 134591, 134590, 134589, 134588, 134587, 134586, 134585, 134584, 134583, 134582, 134581, 134580, 134579, 134578, 134577, 134576, 134575, 134574, 134573, 134572, 134571, 134570, 134569, 134567, 134566, 134565, 134564, 134563, 134562, 134561, 134560, 134559, 134558, 134557, 134556, 134555, 134554, 134552, 134551, 134550, 134549, 134548, 134547, 134546, 134545, 134544, 134543, 134542, 134541, 134540, 134539, 134538, 134537, 134536, 134535, 134534, 134533, 134532, 134531, 134530, 134529, 134528, 134527, 134526, 134525, 134524, 134523, 134522, 134521, 134520, 134519, 134518, 134517, 134516, 134515, 134514, 134513, 134512, 134511, 134510, 134509, 134508, 134507, 134506, 134505, 134504, 134503, 134502, 134501, 134500, 134499, 134498, 134497, 134496, 134495, 134494, 134493, 134492, 134491, 134490, 134489, 134488, 134487, 134486, 134485, 134484, 134483, 134482, 134481, 134480, 134479, 134478, 134477, 134476, 134475, 134474, 134473, 134472, 134471, 134470, 134469, 134468, 134467, 134466, 134465, 134464, 134463, 134462, 134461, 134460, 134459, 134458, 134457, 134456, 134454, 134453, 134452, 134451, 134450, 134449, 134448, 134447, 134446, 134445, 134444, 134443, 134442, 134441, 134439, 134438, 134437, 134436, 134435, 134434, 134433, 134432, 134431, 134430, 134429, 134428, 134427, 134426, 134425, 134424, 134423, 134422, 134421, 134420, 134419, 134418, 134417, 134416, 134415, 134414, 134413, 134412, 134411, 134410, 134409, 134408, 134407, 134406, 134405, 134404, 134403, 134402, 134401, 134400, 134399, 134398, 134397, 134396, 134395, 134394, 134392, 134391, 134390, 134389, 134388, 134385, 134384, 134383, 134382, 134381, 134380, 134379, 134378, 134377, 134376, 134375, 134374, 134373, 134372, 134371, 134370, 134369, 134368, 134367, 134366, 134365, 134364, 134363, 134362, 134361, 134360, 134359, 134358, 134357, 134356, 134355, 134354, 134353, 134352, 134351, 134350, 134349, 134348, 134347, 134346, 134345, 134344, 134343, 134342, 134341, 134340, 134339, 134338, 134337, 134335, 134334, 134333, 134332, 134331, 134330, 134329, 134328, 134327, 134326, 134325, 134324, 134323, 134322, 134320, 134319, 134317, 134316, 134315, 134314, 134313, 134312, 134311, 134310, 134309, 134308, 134306, 134305, 134304, 134303, 134302, 134301, 134300, 134299, 134298, 134297, 134296, 134295, 134294, 134293, 134292, 134291, 134290, 134289, 134288, 134287, 134286, 134285, 134284, 134283, 134282, 134281, 134280, 134279, 134278, 134277, 134276, 134275, 134274, 134273, 134272, 134271, 134270, 134269, 134268, 134267, 134266, 134265, 134264, 134263, 134262, 134261, 134260, 134259, 134258, 134257, 134256, 134255, 134254, 134253, 134252, 134251, 134250, 134249, 134248, 134247, 134246, 134245, 134244, 134243, 134242, 134241, 134240, 134239, 134238, 134237, 134236, 134235, 134234, 134233, 134232, 134231, 134230, 134229, 134228, 134227, 134226, 134225, 134224, 134223, 134222, 134221, 134220, 134219, 134218, 134217, 134216, 134215, 134214, 134213, 134212, 134211, 134210, 134209, 134208, 134207, 134206, 134205, 134204, 134203, 134202, 134201, 134200, 134199, 134198, 134197, 134196, 134195, 134194, 134193, 134192, 134191, 134190, 134189, 134188, 134187, 134186, 134185, 134184, 134183, 134182, 134181, 134180, 134179, 134178, 134177, 134176, 134175, 134174, 134173, 134172, 134171, 134170, 134169, 134168, 134167, 134166, 134165, 134164, 134163, 134162, 134161, 134160, 134159, 134158, 134157, 134156, 134155, 134154, 134153, 134152, 134151, 134149, 134148, 134147, 134146, 134145, 134144, 134143, 134142, 134141, 134140, 134139, 134138, 134137, 134136, 134135, 134134, 134133, 134132, 134131, 134130, 134129, 134128, 134127, 134126, 134125, 134124, 134123, 134122, 134121, 134120, 134119, 134118, 134117, 134116, 134115, 134114, 134113, 134112, 134111, 134109, 134108, 134107, 134106, 134105, 134104, 134103, 134102, 134101, 134100, 134099, 134098, 134097, 134096, 134095, 134094, 134093, 134092, 134091, 134090, 134089, 134088, 134087, 134086, 134085, 134084, 134083, 134082, 134081, 134080, 134079, 134078, 134077, 134076, 134075, 134074, 134073, 134072, 134064, 134063, 134062, 134061, 134060, 134059, 134058, 134057, 134056, 134055, 134054, 134053, 134052, 134051, 134050, 134049, 134048, 134047, 134046, 134045, 134044, 134043, 134042, 134041, 134040, 134039, 134037, 134036, 134035, 134034, 134033, 134032, 134031, 134030, 134029, 134028, 134027, 134026, 134025, 134024, 134023, 134022, 134021, 134020, 134019, 134018, 134017, 134016, 134015, 134014, 134013, 134012, 134011, 134010, 134009, 134008, 134007, 134006, 134005)
-- AND ds.ds_status NOT IN ('A')
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
WITH last_created_tmp
AS (SELECT DISTINCT
    de_id,
    MAX(created_at) AS max_created_at
FROM events
GROUP BY de_id)
SELECT
    MAX
    (events.id) AS id,
    events.de_id,
    events.hash,
    events.created_at
FROM events
INNER JOIN last_created_tmp dt
    ON dt.de_id = events.de_id
    AND dt.max_created_at = events.created_at
WHERE events.de_id IN ({})
GROUP BY events.de_id,
         events.hash,
         events.created_at
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