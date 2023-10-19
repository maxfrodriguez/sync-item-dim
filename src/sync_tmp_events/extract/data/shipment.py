import re
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

    # post init to load the temp_id
    def __post_init__(self) -> None:
        try:
            self.template_id = self.__get_template_id(value=self.template_id)
            if (self.quote_id is None and self.quote_note is not None):
                self.quote_id = self.__get_quote_id(value=self.quote_note)
        except Exception as e:
            print(f"Error in Shipment.__post_init__: {e}")


    def __get_template_id(self, value: str) -> int:
        template_id = re.sub("[^0-9]", "", value)
        return None if not template_id else int(template_id)

    def __get_quote_id(self, value: str) -> str:
        s = value
        match = re.search(r"QUOTE#\s*(.*?)\s*-", s)
        return match.group(1) if match else None