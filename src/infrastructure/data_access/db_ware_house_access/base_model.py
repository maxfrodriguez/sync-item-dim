import logging
from datetime import datetime
from typing import Any, Dict, Final

from pytz import utc
from sqlalchemy import BigInteger, Column, DateTime, select
from sqlalchemy.orm import Session
from typing_extensions import Self

SA_CONSTRAINT_NAMING_CONVENTION: Final[Dict[str, str]] = {
    "ix": "ix_%(table_name)s_%(column_0_name)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class SAModelBaseWareHouse:
    # id = Column(
    #     UNIQUEIDENTIFIER,
    #     primary_key=True,
    #     default=uuid4,
    #     unique=True,
    #     nullable=False,
    # )
    id = Column(
        BigInteger,
        primary_key=True,
        unique=True,
        nullable=False,
    )
    
    # created_at = Column(
    #     DateTime(timezone=True),
    #     default=datetime.now(tz=utc).replace(second=0, microsecond=0),
    #     nullable=False,
    # )

    @classmethod
    def find(cls, db_session: Session, id: str) -> Self | None:
        """
        :param db_session:
        :param id:
        :return:
        """
        statement = select(cls).where(cls.id == id)
        result = db_session.execute(statement)
        instance = result.scalars().first()
        return instance

    def save(self, db_session: Session) -> None:
        db_session.add(self)

    def save_and_return_id(self, db_session: Session) -> int:
        try:
            self.save(db_session=db_session)
            db_session.flush()
            return self.id
        except Exception as e:
            logging.error(f"Error in save_and_return_id: {e}")

    def update(self, db_session: Session, **kwargs) -> None:
        for k, v in kwargs.items():
            setattr(self, k, v)
        self.save(db_session)

    def to_dict(self) -> Dict[str, Any]:
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
