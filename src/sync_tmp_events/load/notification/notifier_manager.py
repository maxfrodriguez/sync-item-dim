#Observer pattern
import datetime
import logging


class NotifierManager:
    
    def __init__(self, stage):
        self.stage = stage
        self.notifiers = []
        
    def register_notifier(self, notifier):
        self.notifiers.append(notifier(self.stage))
        
    async def notify_all(self, list_shipments):
        for notifier in self.notifiers:
            try:
                await notifier.send_information(list_shipments)
            except Exception as e:
                logging.error(f"Error at the momento to notify shipments to next wh sincronizer: {notifier} at {datetime.now()}, with error: {e}")


    async def notify_all_by_pakages(self, list_shipments, size_pagake:int = 10):
        for i in range(0, len(list_shipments), size_pagake):
            for notifier in self.notifiers:
                try:
                    await notifier.send_information(list_shipments[i:i + size_pagake])
                except Exception as e:
                    logging.error(f"Error at the momento to notify shipments to next wh sincronizer: {notifier} at {datetime.now()}, with error: {e}")

