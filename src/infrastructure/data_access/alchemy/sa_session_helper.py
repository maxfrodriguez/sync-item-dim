from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker


class SASessionMaker:
    @staticmethod
    def get_sync_session_maker(connection_string: str) -> sessionmaker[Session]:
        engine: Engine = create_engine(url=connection_string, echo=True)
        return sessionmaker(bind=engine, expire_on_commit=False)
