import logging
from typing import List

from src.domain.entities.shipment import Shipment
from src.domain.repository.movement_abc import MovementRepositoryABC
from src.infrastructure.cross_cutting.environment import ENVIRONMENT
from src.infrastructure.data_access.alchemy.sa_session_impl import get_sa_session
from src.infrastructure.data_access.db_ware_house_access.sa_models_whdb import SAMovement, SAMovementPart
from src.infrastructure.data_access.db_ware_house_access.whdb_anywhere_client import WareHouseDbConnector


class MovementImpl(MovementRepositoryABC):
    async def save_movements(self, shipment: Shipment) -> bool:
        success: bool = False

        if shipment.movements:
            async with get_sa_session() as session:
                try:
                    for movement in shipment.movements:
                        if shipment.id:
                            new_sa_movement = SAMovement(
                                shipment_id=shipment.id,
                                event_origin_id=movement.event_origin.id,
                                event_destination_id=movement.event_destination.id,
                                driver_id=movement.driver,
                                carrier_id=movement.carrier_id,
                                kpi_driver_adherence=movement.kpi_driver_adherence
                                if movement.kpi_driver_adherence
                                else None,
                                kpi_distance_time=movement.real_distance_vo.time_in_min
                                if movement.real_distance_vo
                                else None,
                            )
                            new_sa_movement.save(db_session=session)
                            for part in movement.parts:
                                new_sa_movement_parts: SAMovementPart = SAMovementPart(
                                    event_origin_id=part.event_origin.id,
                                    event_destination_id=part.event_destination.id,
                                    movement=new_sa_movement,
                                )
                                new_sa_movement_parts.save(db_session=session)
                    success = True

                except Exception as e:
                    logging.exception(f"Exception saving movements: {e}")

        return success

    async def saving_movements(self, shipment: Shipment):
        success: bool = False
        bulk_of_movements: List[SAMovement] = []
        bulk_of_part_movements: List[SAMovementPart] = []
        if shipment.movements:
            try:
                for movement in shipment.movements:
                    if shipment.id:
                        new_sa_movement = SAMovement(
                            shipment_id=shipment.id,
                            event_origin_id=movement.event_origin.id,
                            event_destination_id=movement.event_destination.id,
                            driver_id=movement.driver,
                            carrier_id=movement.carrier_id,
                            kpi_driver_adherence=movement.kpi_driver_adherence,
                            kpi_distance_time=movement.real_distance_vo.time_in_min
                            if movement.real_distance_vo
                            else None,
                            used_events=movement.events_amount
                            if movement.events_amount
                            else None,
                            total_events = len(shipment.events)
                            if shipment.events
                            else None,
                        )

                        bulk_of_movements.append(new_sa_movement)
                        for part in movement.parts:
                            new_sa_movement_parts: SAMovementPart = SAMovementPart(
                                event_origin_id=part.event_origin.id,
                                event_destination_id=part.event_destination.id,
                                movement=new_sa_movement,
                            )

                            bulk_of_part_movements.append(new_sa_movement_parts)

                async with WareHouseDbConnector(stage=ENVIRONMENT.UAT) as wh_client:
                    wh_client.bulk_copy(bulk_of_movements)
                    wh_client.bulk_copy(bulk_of_part_movements)

                success = True

            except Exception as e:
                logging.exception(f"Exception saving movements: {e}")

        return success
