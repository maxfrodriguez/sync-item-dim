from typing import Final

MODLOG_QUERY: Final[str] = """
SELECT
    md_ds.ds_id
    , MAX(md_ds.ship_mod_id) as ship_mod_id
FROM [DBA].[modlog_ship] md_ds
where md_ds.ship_mod_id > {0}
GROUP BY md_ds.ds_id
ORDER BY ship_mod_id
"""
# SELECT DISTINCT
# ds.ds_id,
# ds.ds_status,
# MAX(md_ds.mod_id) AS r_mod_id
# FROM [DBA].[modlog_ship] md_ds
# INNER JOIN [DBA].[modlog] md 
# ON  md.mod_id = md_ds.mod_id
# LEFT JOIN [DBA].[disp_ship] ds
# ON ds.ds_id = md_ds.ds_id
# GROUP BY ds.ds_id,
#         ds.ds_status
# HAVING MAX(md_ds.mod_id) > {0}
# OR (ds.ds_status = 'K' AND MAX(md.mod_datetime) > '{1}')
# ORDER BY ds.ds_status DESC

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
        --, di_hcap.RateCodename
        , md.mod_datetime as mod_created_pt_dt
    FROM [DBA].[disp_ship] ds
        LEFT JOIN [DBA].[companies] c1 ON c1.co_id = ds.ds_billto_id
        LEFT JOIN [DBA].[companies] org ON org.co_id = ds.ds_origin_id
        LEFT JOIN [DBA].[companies] des ON des.co_id = ds.ds_findest_id
        LEFT JOIN [DBA].[disp_items] di_hcap ON ds.ds_id = di_hcap.di_shipment_id and di_hcap.AmountType = 1 and di_hcap.Note is not null
        LEFT JOIN [DBA].current_shipments cs ON cs.cs_id = ds.ds_id
        LEFT JOIN [DBA].[modlog_ship] md_ds on ds.ds_id = md_ds.ds_id
        LEFT JOIN [DBA].[modlog] md on md.mod_id = md_ds.mod_id

    WHERE 
        ds.ds_id IN ({})
        and
        md_ds.mod_type = 'C'
    ORDER BY ds.ds_id
        --, di_hcap.RateCodename
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