from dataclasses import dataclass
from datetime import datetime


@dataclass
class Shipment:
    ds_id: int
    ds_status: str
    template_id: int
    ds_status_text: str
    division : str
    MasterBL : str
    ds_hazmat : str
    ds_expedite : str
    ds_lh_totamt : float
    ds_bill_charge : float
    ds_ac_totamt : float
    ds_parentid : int
    customer_id : int
    customer_name : str
    customer_address : str
    customer_city : str
    customer_state : str
    customer_country : str
    customer_zip : str
    cs_routed : int
    cs_assigned : int
    cs_completed : int
    cs_event_count : int
    del_pk_date : datetime
    del_pk_time : datetime
    del_appt_time : datetime
    ds_origin_id : int
    org_name : str
    org_city : str
    org_zip : str
    ds_findest_id : int
    destination_name : str
    destination_city : str
    destinantion_zip : str
    TmpType : str
    st_custom_9 : str
    quote_id : str
    quote_note : str
    #RateCodename : str
    mod_created_pt_dt : datetime
    id : int = None
    eq_c_info_eq_type : str = None
    eq_c_info_line : str = None
    eq_c_info_type : str = None
    container_id : str = None
    eq_h_info_eq_type : str = None
    eq_h_info_line : str = None
    eq_h_info_type : str = None
    chassis_id : str = None
    hash : int = None