from typing import Final

from sqlalchemy import Column, MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from typing_extensions import Protocol

SA_CONSTRAINT_NAMING_CONVENTION: Final[dict[str, str]] = {
    "ix": "ix_%(table_name)s_%(column_0_name)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


metadata = MetaData(
    schema="supra",
    naming_convention=SA_CONSTRAINT_NAMING_CONVENTION,
)


class SAProtocolNoID(Protocol):
    __tablename__: str
    __table__: Table


class SAProtocol(SAProtocolNoID, Protocol):
    id: Column[int]


Base: SAProtocol = declarative_base(metadata=metadata)
