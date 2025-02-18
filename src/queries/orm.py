from sqlalchemy import text, insert, select
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

    @staticmethod
    def insert_workers():
        with session_factory() as session:
            worker_jack = WorkersOrm(username="Jack")
            worker_michael = WorkersOrm(username="Michael")
            session.add_all([worker_jack, worker_michael])
            session.flush() # отрпавить изменения в базу но не завершать запрос
            session.commit()

    @staticmethod
    def select_workers():
        with session_factory() as session:
            query = select(WorkersOrm)
            result = session.execute(query)
            workers = result.scalars().all()
            print(workers)

    @staticmethod
    def update_worker(worker_id: int = 2, new_username: str = "Misha"):
        with session_factory() as session:
            # этот способ делает 2 запроса, 1 для получения объекта
            # 2й для изменения записи
            worker_michael = session.get(WorkersOrm, worker_id)
            worker_michael.username = new_username
            session.expire_all() # сброс всех предыдущих изменений
            session.commit()


class AsyncORM:
    pass
