from sqlalchemy import text, insert
from database import sync_engine, async_engine, session_factory, Base
from models import WorkersOrm, metadata_obj


class SyncORM:
    @staticmethod
    def create_tables():
        sync_engine.echo = True
        print(Base.metadata.tables.values())
        Base.metadata.drop_all(sync_engine)
        Base.metadata.create_all(sync_engine)
        sync_engine.echo = True
