import logging
from datetime import datetime
from typing import Any, ClassVar, Iterable, Type, no_type_check

import pytz
from geoalchemy2 import Geography
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    select,
    types,
)
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import Mapped, relationship
from sqlalchemy.orm.relationships import RelationshipProperty
from sqlalchemy.sql import expression
from typing_extensions import Self

from common.common_infrastructure.dataaccess.db_121tower_access.base import Base
from common.common_infrastructure.dataaccess.db_121tower_access.constants import (
    ChassisType,
    EquipmentType,
    EventType,
    ShipmentStatus,
    ShipmentType,
)
from common.common_infrastructure.dataaccess.db_context.alchemy.sa_session_impl import AlchemyBase


# this method is used to create a custom utcnow function to be used in the server_default of the column: "updated_dt"
def custom_utcnow():
    return datetime.utcnow()


class utcnow(expression.FunctionElement):  # noqa: N801
    type = types.DateTime()


class ImplicitUTCDateTime(types.TypeDecorator):
    impl = types.DateTime

    def process_result_value(self, dt: Any, dialect: Any) -> Any:
        if isinstance(dt, datetime.datetime):
            return dt.replace(tzinfo=pytz.utc)
        else:
            return dt


class CreatedTimestampMixin:
    created_dt = Column(DateTime, server_default=utcnow(), nullable=False)
    # created_dt = Column(DateTime, server_default=func.now(), nullable=False)


class TimestampMixin(CreatedTimestampMixin):
    updated_dt = Column(DateTime, server_default=utcnow(), onupdate=custom_utcnow(), nullable=False)
    # updated_dt = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)


class SAExternalRefBase:
    @declared_attr
    def __table_args__(cls: Any) -> tuple:  # noqa: N805
        return (
            UniqueConstraint(
                "external_id",
                "connection_id",
                name=f"{cls.__tablename__}_external_id_connection_id_uc",
            ),
        )

    id = Column(Integer, primary_key=True)
    external_id = Column(String, nullable=False)
    raw_label = Column(String)
    cleaned_label = Column(String)
    entered_manually = Column(Boolean, nullable=False)
    linked_object_id = Column(Integer)  # over-ridden in SAExternalRefMixin

    @declared_attr
    def connection_id(cls: Any) -> Mapped[Column]:  # noqa: N805
        return Column(Integer, ForeignKey("connections.id"), nullable=False)


class SAExternalRefMixin:
    ExternalRef: ClassVar[Type[SAExternalRefBase]]

    @declared_attr
    @no_type_check
    def external_refs(cls) -> Mapped[RelationshipProperty]:  # noqa: N805
        cls.ExternalRef = type(
            f"{cls.__name__}ExtRef",
            (SAExternalRefBase, Base),
            {
                "__tablename__": f"{cls.__tablename__}_external_references",
                "linked_object_id": Column(
                    Integer,
                    ForeignKey(f"{cls.__tablename__}.id", ondelete="cascade"),
                    index=True,
                ),
                "linked_object": relationship(cls),
            },
        )
        return relationship(cls.ExternalRef, lazy="joined")


class SALocation(TimestampMixin, SAExternalRefMixin, Base):
    __tablename__ = "locations"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

    @classmethod
    def get_location_by_id(cls, orm_client: AlchemyBase, id: int) -> Self | None:
        try:
            statement = select(cls).where(cls.id == id)
            result = orm_client.execute_statement(query=statement)
            instance: Self | None = result
            return instance
        except Exception:
            logging.error(f"Error executing query: {statement}")


class SAGeoFence(TimestampMixin, Base):
    __tablename__ = "geo_fences"

    id = Column(Integer, primary_key=True)
    deleted = Column(Boolean, server_default=expression.false())
    shape = Column(Geography("POLYGON", spatial_index=False), nullable=False)


class SALocationGeoFence(Base):
    __tablename__ = "locations_geo_fences"

    id = Column(Integer, primary_key=True)
    location_id = Column(Integer, ForeignKey("locations.id"), nullable=False, index=True)
    geo_fence_id = Column(Integer, ForeignKey("geo_fences.id"), nullable=False, index=True)
    activated_dt = Column(DateTime, nullable=False)
    deactivated_dt = Column(DateTime)
    geo_fence = relationship("SAGeoFence", lazy="raise", uselist=False, viewonly=True)


class SAWaypoint(Base):
    __tablename__ = "waypoints"

    # SQLAlchemy will create a unique index on gps_device_id, date_time called
    # "waypoints_pkey" which we can use for our hypertable
    gps_device_id = Column(
        Integer,
        ForeignKey("gps_devices.id"),
        nullable=False,
        primary_key=True,
    )
    date_time = Column(DateTime, nullable=False, primary_key=True)
    point = Column(Geography("POINT", spatial_index=False), nullable=False)
    recorded_mph = Column(Float)


class SADriver(TimestampMixin, SAExternalRefMixin, Base):
    __tablename__ = "drivers"

    id = Column(Integer, primary_key=True)
    blob_id = Column(Integer, index=True)
    name = Column(String, unique=False)  # TODO: Set nullable=False

    @classmethod
    def get_external_drivers_by_geotab_ids(cls, orm_client: AlchemyBase, ids: list[str]) -> Self | None:
        try:
            query: str = """SELECT
                drivers.Id as Id_121,
                drivers.Name as Driver_Name,
                driverExtPT.external_id as Id_PT,
                driverExtGeo.raw_label as DriverName_PT,
                driverExtGeo.external_id as Id_GeoTab,
                driverExtGeo.raw_label as DriverName_GeoTab
            from supra.drivers
            left join supra.drivers_external_references driverExtPT on driverExtPT.linked_object_id = drivers.Id and driverExtPT.connection_id = 5
            left join supra.drivers_external_references driverExtGeo on driverExtGeo.linked_object_id = drivers.Id and driverExtGeo.connection_id = 7
            WHERE
                driverExtGeo.external_id IN ({})
            """
            values_string = ", ".join("'" + element + "'" for element in ids)
            result = orm_client.execute_select(query=query.format(values_string))
            instance: Self | None = result
            return instance
        except Exception:
            logging.error(f"Error executing query: {query}")


class SAChassis(TimestampMixin, SAExternalRefMixin, Base):
    __tablename__ = "chassis"

    id = Column(Integer, primary_key=True)
    reference = Column(String, nullable=False)
    length = Column(Integer)  # 20, 40, 53
    max_length = Column(Integer)
    type = Column(Enum(ChassisType, name="chassis_type_enum"))
    axles = Column(Integer)
    gooseneck = Column(Boolean)


class SAGPSDevice(TimestampMixin, SAExternalRefMixin, Base):
    __tablename__ = "gps_devices"

    id = Column(Integer, primary_key=True)
    name = Column(String)
    blob_id = Column(Integer, index=True)
    chassis_id = Column(Integer, ForeignKey("chassis.id"), index=True)
    equipment_type = Column(Enum(EquipmentType, name="gps_devices_type_enum"), nullable=False)

    chassis = relationship("SAChassis", lazy="raise_on_sql")

    @classmethod
    def get_external_devices_by_geotab_ids(cls, orm_client: AlchemyBase, ids: list[str]) -> Self | None:
        try:
            query: str = """SELECT
            	gps_devices."id" as id,
            	gps_devices."name" as name,
            	deviceExtPT.external_id as id_geotab
            from supra.gps_devices
            left join supra.gps_devices_external_references deviceExtPT ON deviceExtPT.linked_object_id = gps_devices."id" and deviceExtPT.connection_id = 7
            WHERE 
                deviceExtPT.external_id in ({})
            """
            values_string = ", ".join("'" + element + "'" for element in ids)
            result = orm_client.execute_select(query=query.format(values_string))
            instance: Self | None = result
            return instance
        except Exception:
            logging.error(f"Error executing query: {query}")


class SAStop(TimestampMixin, Base):
    UPDATEABLE_FIELDS = [
        "line_dt",
        "arrival_dt",
        "departure_dt",
        "last_waypoint_dt",
        "pending",
        "final",
        "stop_segment_id",
        "updated_dt",
    ]
    __tablename__ = "stops"

    id = Column(Integer, primary_key=True)
    geo_fence_id = Column(Integer, ForeignKey("geo_fences.id"), nullable=False, index=True)
    gps_device_id = Column(Integer, ForeignKey("gps_devices.id"), nullable=False, index=True)
    # TODO: Change to be non-nullable
    stop_segment_id = Column(Integer, ForeignKey("stop_segments.id"), index=True)
    line_dt = Column(DateTime, nullable=False)
    arrival_dt = Column(DateTime, nullable=False, index=True)
    departure_dt = Column(DateTime, nullable=True)
    last_waypoint_dt = Column(DateTime, nullable=False)
    pending = Column(Boolean, nullable=False)
    final = Column(Boolean, nullable=False)

    possible_locations = association_proxy("geo_fence", "locations")
    geo_fence = relationship(SAGeoFence, lazy="raise_on_sql")
    gps_device = relationship(SAGPSDevice, lazy="joined")
    # Defined here for typing reasons, overwritten at the bottom of this file
    event_links = relationship("SAStopEventLink", lazy="joined", uselist=True, viewonly=True)


class SAStopSegment(TimestampMixin, Base):
    __tablename__ = "stop_segments"
    id = Column(Integer, primary_key=True)
    gps_device_id = Column(Integer, ForeignKey("gps_devices.id"), nullable=False, index=True)
    start_dt = Column(DateTime, nullable=False, index=True)
    in_progress = Column(Boolean, nullable=False, index=True, server_default=expression.true())

    stops = relationship(SAStop, lazy="joined", uselist=True)
    gps_device = relationship(SAGPSDevice, lazy="joined")
    # possible_drivers = relationship("SAPossibleDriver", lazy="raise", uselist=True)


class SAEvent(TimestampMixin, SAExternalRefMixin, Base):
    __tablename__ = "events"
    __table_args__ = (
        Index(
            "events__driver_id__driver_sequence__ix",
            "driver_id",
            "driver_sequence",
            unique=False,
        ),
    )
    # TODO: Add unique constraint on (shipment_id, sequence)

    # NOTE: If you update columns, make sure to update Persister._save_event
    id = Column(Integer, primary_key=True)
    shipment_id = Column(
        Integer,
        ForeignKey("shipments.id", ondelete="cascade"),
        nullable=False,
        index=True,
    )
    type = Column(Enum(EventType, name="event_type_enum"), nullable=False)
    loaded = Column(Boolean, nullable=False)
    completed = Column(Boolean, nullable=False)
    location_id = Column(Integer, ForeignKey("locations.id"))
    driver_id = Column(Integer, ForeignKey("drivers.id"), index=True)
    sequence = Column(Integer, nullable=False)
    driver_sequence = Column(Numeric)
    planned_date = Column(Date, index=True)
    arrival_dt = Column(DateTime)
    departure_dt = Column(DateTime)
    appointment_start_dt = Column(DateTime)
    appointment_end_dt = Column(DateTime)
    location_external_id = Column(String)
    driver_external_id = Column(String)

    location = relationship(SALocation, lazy="raise_on_sql")
    driver = relationship("SADriver", lazy="raise_on_sql")
    shipment = relationship("SAShipment", lazy="raise_on_sql")
    stop_link = relationship("SAStopEventLink", lazy="raise_on_sql", uselist=False)
    gps_arrival_dt = association_proxy("stop_link", "gps_arrival_dt")
    gps_departure_dt = association_proxy("stop_link", "gps_departure_dt")

    @property
    def stop_links(self) -> Iterable["SAStopEventLink"]:
        if self.stop_link:
            return [self.stop_link]
        else:
            return []

    @property
    def linked_stops(self) -> Iterable["SAStopEventLink"]:
        return self.stop_links


class SAStopEventLink(CreatedTimestampMixin, Base):
    __tablename__ = "stops__events"
    __table_args__ = (
        UniqueConstraint(
            "event_id",
            name=f"{__tablename__}__stop_id__event_id__uc",
        ),
    )

    id = Column(Integer, primary_key=True)
    stop_id = Column(Integer, ForeignKey("stops.id", ondelete="cascade"), nullable=False, index=True)
    event_id = Column(Integer, ForeignKey("events.id", ondelete="cascade"), nullable=False, index=True)
    possible_driver_id = Column(
        Integer,
        ForeignKey("possible_drivers.id", ondelete="cascade"),
        nullable=False,
        index=True,
    )
    customer_notified = Column(Boolean, nullable=False)
    manually_linked = Column(Boolean, nullable=False)
    gps_arrival_dt = Column(DateTime)
    gps_departure_dt = Column(DateTime)

    # I removed `lazy="raise"` because for some reason our options setting for a joined
    # load does not work in all scenarios. Maybe SQLAlchemy treats things differently
    # if there is a single row or something. So just allow the normal loading to happen
    # for now
    stop = relationship(SAStop)
    event = relationship(SAEvent)


class SAShipment(TimestampMixin, SAExternalRefMixin, Base):
    __tablename__ = "shipments"

    # Identifiers
    id = Column(Integer, primary_key=True)
    blob_id = Column(Integer, index=True)
    # TODO: Set nullable=False
    reference = Column(
        String,
        unique=True,
        doc="If this shipment was imported from another system, an unique identifer "
        "from that system should be put here.",
        index=True,
    )
    status = Column(Enum(ShipmentStatus, name="shipment_status_enum"), nullable=False)
    # TODO: Maybe bill tos shouldn't have to be full locations?
    bill_to_id = Column(Integer, ForeignKey("shippers.id"), nullable=False, index=True)

    type = Column(Enum(ShipmentType, name="shipment_type_enum"), nullable=False)
    last_updated_in_tms_dt = Column(DateTime, nullable=False, index=True)
    origin_id = Column(Integer, ForeignKey("locations.id"), nullable=False)
    destination_id = Column(Integer, ForeignKey("locations.id"), nullable=False, index=True)
    is_live_load = Column(Boolean, nullable=False)
    # Equipment
    # # Chassis
    chassis_id = Column(Integer, ForeignKey("chassis.id"), index=True)
    # # Container
    container_length = Column(Integer)
    container_class = Column(String)
    container_line = Column(String)
    container_reference = Column(String)
    # References
    master_bl = Column(String)
    booking_number = Column(String)
    customer_ref = Column(String)

    # Constraints
    # TODO: datetimes, qualifications

    origin = relationship(SALocation, foreign_keys=[origin_id], lazy="joined", innerjoin=True)
    destination = relationship(SALocation, foreign_keys=[destination_id], lazy="joined", innerjoin=True)
    # TODO: Redundant, remove and convert call sites to bill_to
    events = relationship(SAEvent, lazy="selectin", order_by="SAEvent.sequence", uselist=True)


class SAPossibleDriver(TimestampMixin, Base):
    __tablename__ = "possible_drivers"

    # NOTE: If you change any normal attributes, remember to change "on conflict update"
    # fields and associated test as we are relying on those in the usage of this model
    id = Column(Integer, primary_key=True)
    stop_segment_id = Column(
        Integer,
        ForeignKey("stop_segments.id", ondelete="cascade"),
        nullable=False,
        index=True,
        unique=True,
    )
    driver_id = Column(Integer, ForeignKey("drivers.id"), nullable=False, index=True)
    score = Column(Float, nullable=False)
    login_percentage = Column(Float, nullable=False)
    normalized_score = Column(Float, nullable=False)
    historically_adjusted_score = Column(Float, nullable=False)
    set_by_user = Column(Boolean, nullable=False)
    explanation = Column(String)

    stop_event_links = relationship("SAStopEventLink", uselist=True, lazy="joined")
    driver = relationship("SADriver", lazy="joined")


# class EquipmentType(str, Enum):
#     TRACTOR = "tractor"
#     CHASSIS = "chassis"


# class SAGPSDevice(Base):
#     __tablename__ = "gps_devices"

#     id = Column(Integer, primary_key=True)
#     name = Column(String)
#     blob_id = Column(Integer, index=True)
#     chassis_id = Column(Integer, ForeignKey("chassis.id"), index=True)
#     equipment_type = Column(Enum(EquipmentType, name="gps_devices_type_enum"), nullable=False)

#     chassis = relationship("SAChassis", lazy="raise_on_sql")


# class SAStop(Base):
#     UPDATEABLE_FIELDS = [
#         "line_dt",
#         "arrival_dt",
#         "departure_dt",
#         "last_waypoint_dt",
#         "pending",
#         "final",
#         "stop_segment_id",
#     ]
#     __tablename__ = "stops"

#     id = Column(Integer, primary_key=True)
#     geo_fence_id = Column(Integer, ForeignKey("geo_fences.id"), nullable=False, index=True)
#     gps_device_id = Column(Integer, ForeignKey("gps_devices.id"), nullable=False, index=True)
#     # TODO: Change to be non-nullable
#     stop_segment_id = Column(Integer, ForeignKey("stop_segments.id"), index=True)
#     line_dt = Column(DateTime, nullable=False)
#     arrival_dt = Column(DateTime, nullable=False, index=True)
#     departure_dt = Column(DateTime, nullable=True)
#     last_waypoint_dt = Column(DateTime, nullable=False)
#     pending = Column(Boolean, nullable=False)
#     final = Column(Boolean, nullable=False)

#     possible_locations = association_proxy("geo_fence", "locations")
#     geo_fence = relationship(SAGeoFence, lazy="raise_on_sql")
#     gps_device = relationship(SAGPSDevice, lazy="joined")
#     # Defined here for typing reasons, overwritten at the bottom of this file
#     event_links = relationship("SAStopEventLink", lazy="joined", uselist=True, viewonly=True)


# class EventType(str, Enum):
#     HOOK = "hook"
#     # HOOK_WITH_CONTAINER = "HookWithContainer"
#     MOUNT = "mount"
#     PICKUP = "pickup"
#     DELIVER = "deliver"
#     DISMOUNT = "dismount"
#     # DROP_WITH_CONTAINER = "DropWithContainer"
#     DROP = "drop"
#     SCALE = "scale"
#     OTHER = "other"
#     # STOP = "Stop"


# class SAEvent(Base):
#     __tablename__ = "events"
#     # TODO: Add unique constraint on (shipment_id, sequence)

#     # NOTE: If you update columns, make sure to update Persister._save_event
#     id = Column(Integer, primary_key=True)
#     shipment_id = Column(
#         Integer,
#         ForeignKey("shipments.id", ondelete="cascade"),
#         nullable=False,
#         index=True,
#     )
#     type = Column(Enum(EventType, name="event_type_enum"), nullable=False)
#     loaded = Column(Boolean, nullable=False)
#     completed = Column(Boolean, nullable=False)
#     location_id = Column(Integer, ForeignKey("locations.id"))
#     sequence = Column(Integer, nullable=False)
#     driver_sequence = Column(Numeric)
#     planned_date = Column(Date, index=True)
#     arrival_dt = Column(DateTime)
#     departure_dt = Column(DateTime)
#     appointment_start_dt = Column(DateTime)
#     appointment_end_dt = Column(DateTime)
#     location_external_id = Column(String)
#     driver_external_id = Column(String)

#     location = relationship(SALocation, lazy="raise_on_sql")
#     stop_link = relationship("SAStopEventLink", lazy="raise_on_sql", uselist=False)
#     gps_arrival_dt = association_proxy("stop_link", "gps_arrival_dt")
#     gps_departure_dt = association_proxy("stop_link", "gps_departure_dt")

#     @property
#     def stop_links(self) -> Iterable["SAStopEventLink"]:
#         if self.stop_link:
#             return [self.stop_link]
#         else:
#             return []

#     @property
#     def linked_stops(self) -> Iterable["SAStopEventLink"]:
#         return self.stop_links


# class SAStopEventLink(Base):
#     __tablename__ = "stops__events"

#     id = Column(Integer, primary_key=True)
#     stop_id = Column(Integer, ForeignKey("stops.id", ondelete="cascade"), nullable=False, index=True)
#     event_id = Column(Integer, ForeignKey("events.id", ondelete="cascade"), nullable=False, index=True)
#     possible_driver_id = Column(
#         Integer,
#         ForeignKey("possible_drivers.id", ondelete="cascade"),
#         nullable=False,
#         index=True,
#     )
#     customer_notified = Column(Boolean, nullable=False)
#     manually_linked = Column(Boolean, nullable=False)
#     gps_arrival_dt = Column(DateTime)
#     gps_departure_dt = Column(DateTime)

#     # I removed `lazy="raise"` because for some reason our options setting for a joined
#     # load does not work in all scenarios. Maybe SQLAlchemy treats things differently
#     # if there is a single row or something. So just allow the normal loading to happen
#     # for now
#     stop = relationship(SAStop)
#     event = relationship(SAEvent)
