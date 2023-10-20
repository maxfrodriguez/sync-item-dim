# from datetime import datetime
# import logging
# from typing import List, Optional
# from typing_extensions import Self


# class EmptyReturnImpl(EmptyReturnABC):
#     def __init__(self) -> None:
#         self.eg_client: EventGridImpl = None
#         self.__credential: str= "EVENT-GRID-ACCESS-KEY-EMPTY-RETURN" 
#         self.__endpoint: str= "EVENT-GRID-ENDPOINT-EMPTY-RETURN" 

#     async def __aenter__(self) -> Self:
#         async with EventGridImpl(self.__enviroment, self.__credential, self.__endpoint) as client:
#             self.eg_client = client
#         return self
    
#     async def __aexit__(self, exc_type: Optional[Exception], value, traceback):
#         if exc_type is not None:
#             logging.info(f"An Exception has occured {value}")

#     async def create_empty_return(self, shipment_list: List[Shipment]):
#         shipments_id: List[str] = [shipment.ds_id for shipment in shipment_list if shipment.ds_status != 'A' and shipment.has_changed_events]
#         data: dict = {
#             "shipments_id": shipments_id
#         }
#         try:
#             if data:
#                 self.eg_client.send_event(data=data)
                
#         except Exception as e:
#             logging.error(f"Error in create_empty_return: {e} at {datetime.now()}")