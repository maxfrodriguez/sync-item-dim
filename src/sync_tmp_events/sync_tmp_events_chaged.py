from datetime import datetime
import logging
from typing import List
from common.common_infrastructure.cross_cutting.ConfigurationEnvHelper import ConfigurationEnvHelper
from src.sync_tmp_events.extract.data.shipment import Shipment


from src.sync_tmp_events.extract.tmp_repository_abc import TmpRepositoryABC
from src.sync_tmp_events.load.notification.customer_kpi_notification import TmpChangedNotifier
from src.sync_tmp_events.load.notification.notifier_manager import NotifierManager
from src.sync_tmp_events.load.sync_shipment_repository_abc import SyncShipmentRepositoryABC


class SyncronizerTmpAndEventsChaged:
    def __init__(self, tmp_repository: TmpRepositoryABC, sync_information: SyncShipmentRepositoryABC) -> None:
        self.tmp_repository = tmp_repository
        self.sync_information = sync_information

    async def syncronize(self) -> None:

        # process to syncronize
        async with self.tmp_repository:
            await self.tmp_repository.get_tmp_changed();
            
            start_process = datetime.now()
            # sincronize in packs of 100
            pack_size=ConfigurationEnvHelper().get_secret("PackageSizeToSync")
            for shipmets_to_sync in self.tmp_repository.next_shipments(pack_size=pack_size):
                try:

                    logging.info(f"Syncronize {len(shipmets_to_sync)} shipments, starting at {start_process}")

                    self.tmp_repository.complement_with_equipment_info(shipmets_to_sync)
                    shipmets_to_sync = await self.sync_information.find_shipments_to_sync(shipmets_to_sync)
                    custom_fields = await self.tmp_repository.get_custom_fields(shipmets_to_sync)
                    shipments_to_notify = await self.load(shipmets_to_sync, custom_fields)
                    await self.notify(shipments_to_notify)

                    timeelapsed = datetime.now() - start_process
                    logging.info(f"Syncronize {len(shipmets_to_sync)} shipments, finished at {datetime.now()} in {timeelapsed}")

                except Exception as e:
                    ids = ", ".join(f"'{shipment.ds_id}'" for shipment in shipmets_to_sync)
                    logging.error(f"Error in syncronize shipments: {ids} with error: {e}")
                
    
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
            notifier_manager = NotifierManager()

            notifier_manager.register_notifier(TmpChangedNotifier)
            # notifier_manager.register_notifier(StreetTurnNotifier)
            # notifier_manager.register_notifier(DimChangeStatusChange)
            # notifier_manager.register_notifier(OnTimeDeliveryNotifier)
            # notifier_manager.register_notifier(HubSpotNotifier)

            
            await notifier_manager.notify_all_by_pakages(list_shipments, size_pagake=10)
        
        except Exception as e:
            logging.error(f"Error in notify: {e}")
        
    

    
        