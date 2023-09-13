from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Time,
    desc,
    select,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, relationship
from typing_extensions import Self

from src.infrastructure.data_access.alchemy.sa_session_impl import AlchemyBase
from src.infrastructure.data_access.db_ware_house_access.base_model import SAModelBaseWareHouse

naming_convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

metadata = MetaData(naming_convention=naming_convention)
Base = declarative_base(metadata=metadata)


class SALoaderLog(Base, SAModelBaseWareHouse):
    __tablename__ = "loader_logs"

    mod_lowest_version = Column(BigInteger, nullable=False, index=True)
    mod_highest_version = Column(BigInteger, nullable=False, index=True)
    num_fact_movements_loaded = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)

    @classmethod
    def get_highest_version(cls, db_session: Session) -> Self | None:
        return db_session.query(cls).order_by(desc(cls.mod_highest_version)).first()


class SAShipment(Base, SAModelBaseWareHouse):
    __tablename__ = "shipments"

    ds_id = Column(Integer, nullable=False)
    ds_status = Column(String, nullable=True)
    template_id = Column(Integer, nullable=True)
    ds_status_text = Column(String, nullable=True)
    division = Column(String, nullable=True)
    MasterBL = Column(String, nullable=True)
    ds_hazmat = Column(String, nullable=True)
    ds_expedite = Column(String, nullable=True)
    ds_lh_totamt = Column(Float, nullable=True)
    ds_bill_charge = Column(Float, nullable=True)
    ds_ac_totamt = Column(Float, nullable=True)
    ds_parentid = Column(Integer, nullable=True)
    linkedeq1type = Column(String, nullable=True)
    linkedeq1ref = Column(String, nullable=True)
    linkedeq1leaseline = Column(String, nullable=True)
    linkedeq1leasetype = Column(String, nullable=True)
    linkedeq2type = Column(String, nullable=True)
    linkedeq2ref = Column(String, nullable=True)
    linkedeq2leaseline = Column(String, nullable=True)
    linkedeq2leasetype = Column(String, nullable=True)
    customer_id = Column(Integer, nullable=True)
    customer_name = Column(String, nullable=True)
    customer_address = Column(String, nullable=True)
    customer_city = Column(String, nullable=True)
    customer_state = Column(String, nullable=True)
    customer_country = Column(String, nullable=True)
    customer_zip = Column(String, nullable=True)
    cs_routed = Column(Integer, nullable=True)
    cs_assigned = Column(Integer, nullable=True)
    cs_completed = Column(Integer, nullable=True)
    cs_event_count = Column(Integer, nullable=True)
    del_pk_date = Column(Date, nullable=True)
    del_pk_time = Column(Time, nullable=True)
    ds_origin_id = Column(Integer, nullable=True)
    org_name = Column(String, nullable=True)
    org_city = Column(String, nullable=True)
    org_zip = Column(String, nullable=True)
    ds_findest_id = Column(Integer, nullable=True)
    destination_name = Column(String, nullable=True)
    destination_city = Column(String, nullable=True)
    destinantion_zip = Column(String, nullable=True)
    TmpType = Column(String, nullable=True)
    eq_c_info_id = Column(Integer, nullable=True)
    eq_c_info_eq_type = Column(String, nullable=True)
    container_id = Column(String, nullable=True)
    eq_c_info_line = Column(String, nullable=True)
    eq_c_info_type = Column(String, nullable=True)
    eq_h_info_id = Column(Integer, nullable=True)
    eq_h_info_eq_type = Column(String, nullable=True)
    chassis_id = Column(String, nullable=True)
    eq_h_info_line = Column(String, nullable=True)
    eq_h_info_type = Column(String, nullable=True)
    st_custom_9 = Column(String, nullable=True)
    hash = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False)
    mod_created_pt_dt = Column(DateTime(timezone=True), nullable=True)
    quote_id = Column(String, nullable=True)
    quote_note = Column(String, nullable=True)
    del_appt_time = Column(String, nullable=True)
    # line = Column(String, nullable=True)
    # type = Column(String, nullable=True)
    # fkequipmentleasetype = Column(String, nullable=True)
    # chasis_1 = Column(String, nullable=True)
    # chasis_2 = Column(String, nullable=True)
    # chasis_3 = Column(String, nullable=True)
    # chasis_4 = Column(String, nullable=True)

    @classmethod
    def find_by_hash(cls, db_session: Session, hash: str) -> int:
        statement = select(cls.ds_id).where(cls.hash == hash)
        result = db_session.execute(statement)
        instance: int | None = result.scalars().first()
        return instance

    @classmethod
    def bulk_save(cls, orm_client: AlchemyBase) -> None:
        ...

    events = relationship("SAEvent", back_populates="shipment")

class SATemplate(Base):
    __tablename__ = "templates"

    ds_id = Column(Integer, nullable=False)
    ds_id = Column(BigInteger, primary_key=True, unique=True, nullable=False)
    ds_status = Column(String, nullable=True)
    template_id = Column(Integer, nullable=True)
    ds_status_text = Column(String, nullable=True)
    MasterBL = Column(String, nullable=True)
    ds_hazmat = Column(String, nullable=True)
    ds_expedite = Column(String, nullable=True)
    ds_lh_totamt = Column(Float, nullable=True)
    ds_bill_charge = Column(Float, nullable=True)
    ds_ac_totamt = Column(Float, nullable=True)
    ds_parentid = Column(Integer, nullable=True)
    linkedeq1type = Column(String, nullable=True)
    linkedeq1ref = Column(String, nullable=True)
    linkedeq1leaseline = Column(String, nullable=True)
    linkedeq1leasetype = Column(String, nullable=True)
    linkedeq2type = Column(String, nullable=True)
    linkedeq2ref = Column(String, nullable=True)
    linkedeq2leaseline = Column(String, nullable=True)
    linkedeq2leasetype = Column(String, nullable=True)
    customer_id = Column(Integer, nullable=True)
    customer_name = Column(String, nullable=True)
    customer_address = Column(String, nullable=True)
    customer_city = Column(String, nullable=True)
    customer_state = Column(String, nullable=True)
    customer_country = Column(String, nullable=True)
    customer_zip = Column(String, nullable=True)
    cs_routed = Column(Integer, nullable=True)
    cs_assigned = Column(Integer, nullable=True)
    cs_completed = Column(Integer, nullable=True)
    cs_event_count = Column(Integer, nullable=True)
    del_pk_date = Column(Date, nullable=True)
    del_pk_time = Column(Time, nullable=True)
    ds_origin_id = Column(Integer, nullable=True)
    org_name = Column(String, nullable=True)
    org_city = Column(String, nullable=True)
    org_zip = Column(String, nullable=True)
    ds_findest_id = Column(Integer, nullable=True)
    destination_name = Column(String, nullable=True)
    destination_city = Column(String, nullable=True)
    destinantion_zip = Column(String, nullable=True)
    TmpType = Column(String, nullable=True)
    eq_c_info_id = Column(Integer, nullable=True)
    eq_c_info_eq_type = Column(String, nullable=True)
    container_id = Column(String, nullable=True)
    eq_c_info_line = Column(String, nullable=True)
    eq_c_info_type = Column(String, nullable=True)
    eq_h_info_id = Column(Integer, nullable=True)
    eq_h_info_eq_type = Column(String, nullable=True)
    chassis_id = Column(String, nullable=True)
    eq_h_info_line = Column(String, nullable=True)
    eq_h_info_type = Column(String, nullable=True)
    st_custom_9 = Column(String, nullable=True)
    hash = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False)
    mod_created_pt_dt = Column(DateTime(timezone=True), nullable=True)
    quote_id = Column(String, nullable=True)
    quote_note = Column(String, nullable=True)
    del_appt_time = Column(String, nullable=True)

class SAEvent(Base, SAModelBaseWareHouse):
    __tablename__ = "events"

    de_id = Column(Integer, nullable=False)
    de_type = Column(String, nullable=True)
    location_id = Column(Integer, nullable=True)
    location_name = Column(String, nullable=True)
    location_zip = Column(String, nullable=True)
    de_ship_seq = Column(Integer, nullable=True)
    de_conf = Column(String, nullable=True)
    de_routable = Column(String, nullable=True)
    de_driver = Column(Integer, nullable=True)
    carrier_id = Column(Integer, nullable=True)
    de_appointment = Column(DateTime, nullable=True)
    de_departure = Column(DateTime, nullable=True)
    de_arrival = Column(DateTime, nullable=True)
    de_earliest = Column(DateTime, nullable=True)
    de_latest = Column(DateTime, nullable=True)
    driver_name = Column(String, nullable=True)
    hash = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False)

    shipment_id = Column(BigInteger, ForeignKey("shipments.id", ondelete="CASCADE"), nullable=True)
    shipment = relationship("SAShipment", uselist=False, back_populates="events")
    # stops = relationship("SAStops", uselist=False, back_populates="event")


class SAMovement(Base, SAModelBaseWareHouse):
    __tablename__ = "fact_movements"

    driver_id = Column(BigInteger, nullable=True)
    carrier_id = Column(Integer, nullable=True)
    kpi_distance_miles = Column(Float, nullable=True)
    kpi_distance_time = Column(Integer, nullable=True)
    kpi_is_st_capable = Column(Boolean, nullable=True)
    kpi_is_st_achieved = Column(Boolean, nullable=True)
    kpi_driver_adherence = Column(Boolean, nullable=True)
    used_events = Column(Integer, nullable=True)
    total_events = Column(Integer, nullable=True)

    movement_parts = relationship("SAMovementPart", cascade="all, delete-orphan", back_populates="movement")

    # Add a many-to-one relationship to SADriver with the 'save-update' cascade option.
    # driver = relationship("SADriver", uselist=False, back_populates="movements", cascade="save-update, merge")

    shipment_id = Column(BigInteger, ForeignKey("shipments.id"), nullable=False)
    # shipments = relationship("SAShipment", back_populates="movements", cascade="all, delete-orphan")

    event_origin_id = Column(BigInteger, ForeignKey("events.id"), nullable=False)
    # event_origin = relationship("SAEvent", uselist=False, back_populates="movement_origin")

    event_destination_id = Column(BigInteger, ForeignKey("events.id"), nullable=False)
    # event_destination = relationship("SAEvent", uselist=False, back_populates="movement_destination")


class SAMovementPart(Base, SAModelBaseWareHouse):
    __tablename__ = "movement_parts"

    event_origin_id = Column(Integer, nullable=True)
    event_destination_id = Column(Integer, nullable=True)

    movement_id = Column(BigInteger, ForeignKey("fact_movements.id", ondelete="CASCADE"), nullable=False)
    movement = relationship("SAMovement", uselist=False, back_populates="movement_parts")


class SAStops(Base, SAModelBaseWareHouse):
    __tablename__ = "stops"

    bf_event_id = Column(Integer)
    pt_event_id = Column(Integer, nullable=False)
    stop_id = Column(Integer, nullable=True)
    location_event_name = Column(String, nullable=True)
    geo_fence_id = Column(Integer, nullable=True)
    bf_driver_id = Column(Integer, nullable=True)
    pt_driver_id = Column(Integer, nullable=True)
    geotab_driver_id = Column(String, nullable=True)
    driver_name = Column(String, nullable=True)
    bf_truck_id = Column(Integer, nullable=True)
    geotab_truck_id = Column(String, nullable=True)
    truck_name = Column(String, nullable=True)
    arrival_dt = Column(DateTime, nullable=True)
    departure_dt = Column(DateTime, nullable=True)
    hash = Column(String, nullable=True)
    event_id = Column(BigInteger, ForeignKey("events.id"), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False)

    # event = relationship("SAEvent", uselist=False, back_populates="stops")


class SADrivers(Base, SAModelBaseWareHouse):
    __tablename__ = "drivers"

    di_id = Column(Integer)
    name = Column(String, nullable=True)
    status = Column(String, nullable=True)
    fleet = Column(String, nullable=True)


class SAItems(Base, SAModelBaseWareHouse):
    __tablename__ = "items"

    sk_id_shipment_fk = Column(BigInteger, nullable=False)
    ds_id = Column(Integer, nullable=False)
    di_item_id = Column(Integer, nullable=False)
    amount_type = Column(Integer, nullable=True)
    name = Column(String, nullable=True)
    rate_code_name = Column(String, nullable=True)
    di_description = Column(String, nullable=True)
    di_our_itemamt = Column(Float, nullable=True)
    di_pay_itemamt = Column(Float, nullable=True)
    di_quantity = Column(String, nullable=True)
    last_rated_by = Column(String, nullable=True)
    tag_list = Column(String, nullable=True)
    note = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False)


class SACustomFields(Base, SAModelBaseWareHouse):
    __tablename__ = "shipments_custom_fields"

    ds_id = Column(Integer, nullable=False)
    ds_status = Column(String, nullable=True)
    ds_custom_1 = Column(String, nullable=True)
    ds_custom_2 = Column(String, nullable=True)
    ds_custom_3 = Column(String, nullable=True)
    ds_custom_4 = Column(String, nullable=True)
    ds_custom_5 = Column(String, nullable=True)
    ds_custom_6 = Column(String, nullable=True)
    ds_custom_7 = Column(String, nullable=True)
    ds_custom_8 = Column(String, nullable=True)
    ds_custom_9 = Column(String, nullable=True)
    ds_custom_10 = Column(String, nullable=True)
    client_custom_1 = Column(String, nullable=True)
    client_custom_2 = Column(String, nullable=True)
    client_custom_3 = Column(String, nullable=True)
    client_custom_4 = Column(String, nullable=True)
    client_custom_5 = Column(String, nullable=True)
    client_custom_6 = Column(String, nullable=True)
    client_custom_7 = Column(String, nullable=True)
    client_custom_8 = Column(String, nullable=True)
    client_custom_9 = Column(String, nullable=True)
    client_custom_10 = Column(String, nullable=True)
    origin_custom_1 = Column(String, nullable=True)
    origin_custom_2 = Column(String, nullable=True)
    origin_custom_3 = Column(String, nullable=True)
    origin_custom_4 = Column(String, nullable=True)
    origin_custom_5 = Column(String, nullable=True)
    origin_custom_6 = Column(String, nullable=True)
    origin_custom_7 = Column(String, nullable=True)
    origin_custom_8 = Column(String, nullable=True)
    origin_custom_9 = Column(String, nullable=True)
    origin_custom_10 = Column(String, nullable=True)
    destination_custom_1 = Column(String, nullable=True)
    destination_custom_2 = Column(String, nullable=True)
    destination_custom_3 = Column(String, nullable=True)
    destination_custom_4 = Column(String, nullable=True)
    destination_custom_5 = Column(String, nullable=True)
    destination_custom_6 = Column(String, nullable=True)
    destination_custom_7 = Column(String, nullable=True)
    destination_custom_8 = Column(String, nullable=True)
    destination_custom_9 = Column(String, nullable=True)
    destination_custom_10 = Column(String, nullable=True)
    sk_id_shipment_fk = Column(BigInteger, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)
    carrier_pay = Column(Float, nullable=True)


class SAShipmentEvent(Base, SAModelBaseWareHouse):
    __tablename__ = "shipments_events"

    sk_shipment_id = Column(BigInteger, nullable= False)
    sk_event_id = Column(BigInteger, nullable=False)
    sequence_event_id = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False)


# Main Shipment Table
class SAFactShipment(Base):
    __tablename__ = "fact_shipments"

    ds_id = Column(Integer, nullable=False)
    ds_status = Column(String, nullable=True)
    template_id = Column(Integer, nullable=True)
    ds_status_text = Column(String, nullable=True)
    division = Column(String, nullable=True)
    MasterBL = Column(String, nullable=True)
    ds_hazmat = Column(String, nullable=True)
    ds_expedite = Column(String, nullable=True)
    ds_lh_totamt = Column(Float, nullable=True)
    ds_bill_charge = Column(Float, nullable=True)
    ds_ac_totamt = Column(Float, nullable=True)
    ds_parentid = Column(Integer, nullable=True)
    linkedeq1type = Column(String, nullable=True)
    linkedeq1ref = Column(String, nullable=True)
    linkedeq1leaseline = Column(String, nullable=True)
    linkedeq1leasetype = Column(String, nullable=True)
    linkedeq2type = Column(String, nullable=True)
    linkedeq2ref = Column(String, nullable=True)
    linkedeq2leaseline = Column(String, nullable=True)
    linkedeq2leasetype = Column(String, nullable=True)
    customer_id = Column(Integer, nullable=True)
    customer_name = Column(String, nullable=True)
    customer_address = Column(String, nullable=True)
    customer_city = Column(String, nullable=True)
    customer_state = Column(String, nullable=True)
    customer_country = Column(String, nullable=True)
    customer_zip = Column(String, nullable=True)
    cs_routed = Column(Integer, nullable=True)
    cs_assigned = Column(Integer, nullable=True)
    cs_completed = Column(Integer, nullable=True)
    cs_event_count = Column(Integer, nullable=True)
    del_pk_date = Column(Date, nullable=True)
    del_pk_time = Column(Time, nullable=True)
    ds_origin_id = Column(Integer, nullable=True)
    org_name = Column(String, nullable=True)
    org_city = Column(String, nullable=True)
    org_zip = Column(String, nullable=True)
    ds_findest_id = Column(Integer, nullable=True)
    destination_name = Column(String, nullable=True)
    destination_city = Column(String, nullable=True)
    destinantion_zip = Column(String, nullable=True)
    TmpType = Column(String, nullable=True)
    eq_c_info_id = Column(Integer, nullable=True)
    eq_c_info_eq_type = Column(String, nullable=True)
    container_id = Column(String, nullable=True)
    eq_c_info_line = Column(String, nullable=True)
    eq_c_info_type = Column(String, nullable=True)
    eq_h_info_id = Column(Integer, nullable=True)
    eq_h_info_eq_type = Column(String, nullable=True)
    chassis_id = Column(String, nullable=True)
    eq_h_info_line = Column(String, nullable=True)
    eq_h_info_type = Column(String, nullable=True)
    st_custom_9 = Column(String, nullable=True)
    hash = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False)
    mod_created_pt_dt = Column(DateTime(timezone=True), nullable=True)
    quote_id = Column(String, nullable=True)
    quote_note = Column(String, nullable=True)
    del_appt_time = Column(String, nullable=True)


# Main events
class SAFactEvent(Base):
    __tablename__ = "fact_events"

    de_id = Column(Integer, nullable=False)
    de_type = Column(String, nullable=True)
    location_id = Column(Integer, nullable=True)
    location_name = Column(String, nullable=True)
    location_zip = Column(String, nullable=True)
    de_ship_seq = Column(Integer, nullable=True)
    de_conf = Column(String, nullable=True)
    de_routable = Column(String, nullable=True)
    de_driver = Column(Integer, nullable=True)
    carrier_id = Column(Integer, nullable=True)
    de_appointment = Column(DateTime, nullable=True)
    de_departure = Column(DateTime, nullable=True)
    de_arrival = Column(DateTime, nullable=True)
    de_earliest = Column(DateTime, nullable=True)
    de_latest = Column(DateTime, nullable=True)
    driver_name = Column(String, nullable=True)
    hash = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False)

    shipment_id = Column(Integer, ForeignKey("fact_shipments.ds_id", ondelete="CASCADE"), nullable=True)
    shipment = relationship("SAFactShipment", uselist=False, back_populates="events")