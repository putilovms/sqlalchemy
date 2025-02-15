from sqlalchemy import text, insert
from src.database import sync_engine, async_engine, session_factory
from models import metadata_obj, WorkersOrm


def create_tables():    
    sync_engine.echo = False
    metadata_obj.drop_all(sync_engine)
    metadata_obj.create_all(sync_engine)
    sync_engine.echo = True


def insert_data():
    worker_bobr = WorkersOrm(username="Bobr")
    worker_volk = WorkersOrm(username="Volk")
    with session_factory() as session:
        # session.add(worker_bobr)
        session.add_all([worker_bobr, worker_volk])
        session.commit()
