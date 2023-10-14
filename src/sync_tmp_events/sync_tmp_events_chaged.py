import logging
from typing import List
from src.domain.entities.shipment import Shipment
from src.sync_tmp_events.extract.tmp_repository_abc import TmpRepositoryABC
from src.sync_tmp_events.load.notification.customer_kpi_notification import CustomerKpiNotifier
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

            # sincronize in packs of 100
            for shipmets_to_sync in self.tmp_repository.next_shipments(pack_size=10):
                self.tmp_repository.complement_with_equipment_info(shipmets_to_sync)
                shipmets_to_sync = await self.identify_changes(shipmets_to_sync)
                custom_fields = await self.tmp_repository.get_custom_fields(shipmets_to_sync)
                shipments_to_notify = await self.load(shipmets_to_sync, custom_fields)
                await self.notify(shipments_to_notify)
        

    async def find_next_tmps_chaged(self):
        await self.tmp_repository.get_tmp_changed();
        return self
    
    async def identify_changes(self, list_shipments: List[Shipment]):
        return await self.sync_information.find_shipments_to_sync(list_shipments)
    
    async def load(self, list_shipments: List[Shipment], custom_fields: List[dict]):
        return await self.sync_information.load_shipments(list_shipments, custom_fields)

    async def notify(self, list_shipments: List[Shipment]):
        try:
            #add any new notification here
            notifier_manager = NotifierManager(self.__stage)
            
            #notifier_manager.register_notifier(EventsNotifier)
            #notifier_manager.register_notifier(ItemsNotifier)

            notifier_manager.register_notifier(StreetTurnNotifier)
            notifier_manager.register_notifier(DimChangeStatusChange)
            notifier_manager.register_notifier(OnTimeDeliveryNotifier)
            notifier_manager.register_notifier(CustomerKpiNotifier)
            notifier_manager.register_notifier(HubSpotNotifier)

            await notifier_manager.notify_all_by_pakages(list_shipments, size_pagake=10)
        
        except Exception as e:
            logging.error(f"Error in notify: {e}")
        
    

    
        