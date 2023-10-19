from datetime import datetime
import pytest

from common.common_infrastructure.cross_cutting.environment import ENVIRONMENT
from src.sync_tmp_events.extract.data.shipment import Shipment
from src.sync_tmp_events.load.notification.customer_kpi_notification import TmpChangedNotifier
from src.sync_tmp_events.load.notification.notifier_manager import NotifierManager
from src.sync_tmp_events.load.notification.street_turn_notification import StreetTurnNotifier

@pytest.mark.asyncio
class TestSyncronizerTmpAndEvents:

    async def test_notifier_tmp(self):
        # Arrange
        notifier_manager = NotifierManager(ENVIRONMENT.UAT)
        list_shipments = [Shipment(
            ds_id = 100
            , ds_status = "K"
            , template_id = "100"
            , ds_status_text = ""
            , division  = ""
            , MasterBL  = ""
            , ds_hazmat  = ""
            , ds_expedite  = ""
            , ds_lh_totamt = 0.0
            , ds_bill_charge = 0.0
            , ds_ac_totamt = 0.0
            , ds_parentid  = 0
            , customer_id  = 100
            , customer_name  = ""
            , customer_address  = ""
            , customer_city  = ""
            , customer_state  = ""
            , customer_country  = ""
            , customer_zip  = ""
            , cs_routed  = 0
            , cs_assigned  = 0
            , cs_completed  = 0
            , cs_event_count  = 0
            , del_pk_date = datetime.now()
            , del_pk_time = datetime.now()
            , del_appt_time = datetime.now()
            , ds_origin_id  = 0
            , org_name  = ""
            , org_city  = ""
            , org_zip  = ""
            , ds_findest_id  = 0
            , destination_name  = ""
            , destination_city  = ""
            , destinantion_zip  = ""
            , TmpType  = ""
            , st_custom_9  = ""
            , quote_id  = ""
            , quote_note  = "QUOTE # 45"
            , mod_created_pt_dt = datetime.now()
        )]


        notifier_manager.register_notifier(TmpChangedNotifier)
        # notifier_manager.register_notifier(StreetTurnNotifier)
        # notifier_manager.register_notifier(DimChangeStatusChange)
        # notifier_manager.register_notifier(OnTimeDeliveryNotifier)
        # notifier_manager.register_notifier(HubSpotNotifier)

            
        await notifier_manager.notify_all_by_pakages(list_shipments, size_pagake=10)