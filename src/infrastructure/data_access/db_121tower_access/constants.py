from enum import Enum

SYSTEM_SCHEMA_NAME = "system"
TENANT_SCHEMA_TEMPLATE_NAME = "tenant_schema_template"
SA_CONSTRAINT_NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_name)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class EquipmentType(str, Enum):
    TRACTOR = "tractor"
    CHASSIS = "chassis"


class ChassisType(str, Enum):
    STANDARD = "standard"
    SLIDER = "slider"


class EventType(str, Enum):
    HOOK = "hook"
    # HOOK_WITH_CONTAINER = "HookWithContainer"
    MOUNT = "mount"
    PICKUP = "pickup"
    DELIVER = "deliver"
    DISMOUNT = "dismount"
    # DROP_WITH_CONTAINER = "DropWithContainer"
    DROP = "drop"
    SCALE = "scale"
    OTHER = "other"
    # STOP = "Stop"


class ShipmentStatus(str, Enum):
    PRE_DISPATCH = "pre_dispatch"
    CANCELLED = "cancelled"
    DISPATCHING = "dispatching"
    READY_FOR_BILLING = "ready_for_billing"
    BILLING_HOLD = "billing_hold"
    BILLED = "billed"


class ShipmentType(str, Enum):
    IMPORT = "import"
    EXPORT = "export"
    ONEWAY = "oneway"
    # I think we want to use this in the future to differentiate these types of Oneways
    # CHASSIS_REPOSITION = "ChassisReposition"
