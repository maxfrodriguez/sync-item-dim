from datetime import datetime
import logging
from typing import List
from os import getenv
from common.common_infrastructure.cross_cutting.environment import ConfigurationEnvHelper

from src.domain.entities.shipment import Shipment
from src.sync_tmp_events.extract.tmp_repository_abc import TmpRepositoryABC
from src.sync_tmp_events.load.notification.customer_kpi_notification import TmpChangedNotifier
from src.sync_tmp_events.load.notification.dim_change_status_notification import DimChangeStatusChange
from src.sync_tmp_events.load.notification.hubspot_notification import HubSpotNotifier
from src.sync_tmp_events.load.notification.notifier_manager import NotifierManager
from src.sync_tmp_events.load.notification.on_time_delivery_notification import OnTimeDeliveryNotifier
from src.sync_tmp_events.load.notification.street_turn_notification import StreetTurnNotifier
from src.sync_tmp_events.load.sync_shipment_repository_abc import SyncShipmentRepositoryABC


class SyncronizerTmpAndEventsChaged:
    def __init__(self, stage, tmp_repository: TmpRepositoryABC, sync_information: SyncShipmentRepositoryABC) -> None:
        self.__stage = stage
        self.tmp_repository = tmp_repository
        self.sync_information = sync_information

    async def syncronize(self) -> None:

        # process to syncronize
        async with self.tmp_repository:
            await self.find_next_tmps_chaged()

            start_process = datetime.now()
            # sincronize in packs of 100
            pack_size=ConfigurationEnvHelper(stage=self.__stage).get_secret("PackageSizeToSync")
            for shipmets_to_sync in self.tmp_repository.next_shipments(pack_size=pack_size):
                print(f"Start process at {start_process} with {len(shipmets_to_sync)} shipments")
                self.tmp_repository.complement_with_equipment_info(shipmets_to_sync)
                shipmets_to_sync = await self.identify_changes(shipmets_to_sync)
                print(f"Identify changes {len(shipmets_to_sync)} shipments, time elapsed: {datetime.now() - start_process}")
                custom_fields = await self.tmp_repository.get_custom_fields(shipmets_to_sync)
                shipments_to_notify = await self.load(shipmets_to_sync, custom_fields)
                print(f"Load {len(shipments_to_notify)} shipments, time elapsed: {datetime.now() - start_process}")
                await self.notify(shipments_to_notify)
                print(f"Notify {len(shipments_to_notify)} shipments, time elapsed: {datetime.now() - start_process}")
                start_process = datetime.now()
        

    async def find_next_tmps_chaged(self):
        await self.tmp_repository.get_tmp_changed();
        return self
    
    async def identify_changes(self, list_shipments: List[Shipment]):
        return await self.sync_information.find_shipments_to_sync(list_shipments)
    
    async def load(self, list_shipments: List[Shipment], custom_fields: List[dict]):
        shipment_to_sync = await self.sync_information.load_shipments(list_shipments, custom_fields)
    
        #get min id from list_shipments.id
        if shipment_to_sync:
            lowest_modlog = min(list_shipments, key=lambda x: x.id).id #id get modlog_id
            highest_modlog = max(list_shipments, key=lambda x: x.id).id #id get modlog_id
            fact_movements_loaded = len(list_shipments)

        await self.sync_information.save_latest_loader_logs(lowest_modlog, highest_modlog, fact_movements_loaded)

        return shipment_to_sync

    async def notify(self, list_shipments: List[Shipment]):
        try:
            #add any new notification here
            notifier_manager = NotifierManager(self.__stage)

            notifier_manager.register_notifier(TmpChangedNotifier)
            # notifier_manager.register_notifier(StreetTurnNotifier)
            # notifier_manager.register_notifier(DimChangeStatusChange)
            # notifier_manager.register_notifier(OnTimeDeliveryNotifier)
            # notifier_manager.register_notifier(HubSpotNotifier)

            
            await notifier_manager.notify_all_by_pakages(list_shipments, size_pagake=10)
        
        except Exception as e:
            logging.error(f"Error in notify: {e}")
        
    

    
        