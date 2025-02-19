from sqlalchemy import Integer, and_, text, insert, select, func, cast
from database import sync_engine, async_engine, session_factory, Base
from models import WorkersOrm, ResumesOrm, Workload, metadata_obj


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
            session.flush()  # отправить изменения в базу но не завершать запрос
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
            session.commit()

    @staticmethod
    def insert_resumes():
        with session_factory() as session:
            resume_jack_1 = ResumesOrm(
                title="Python Junior Developer",
                compensation=50000,
                workload=Workload.fulltime,
                worker_id=1
            )
            resume_jack_2 = ResumesOrm(
                title="Python Разработчик",
                compensation=150000,
                workload=Workload.fulltime,
                worker_id=1
            )
            resume_michael_1 = ResumesOrm(
                title="Python Data Engineer",
                compensation=250000,
                workload=Workload.parttime,
                worker_id=2
            )
            resume_michael_2 = ResumesOrm(
                title="Data Scientist",
                compensation=300000,
                workload=Workload.fulltime,
                worker_id=2
            )
            session.add_all([resume_jack_1, resume_jack_2,
                            resume_michael_1, resume_michael_2])
            session.commit()
            sync_engine.echo = True

    @staticmethod
    def select_resumes_avg_compensation(like_language: str = "Python"):
        with session_factory() as session:
            """
            select workload, avg(compensation)::int as avg_compensation
            from resumes
            where title like '%Python%' and compensation > 40000
            group by workload
            """
            query = (
                select(
                    ResumesOrm.workload,
                    cast(func.avg(ResumesOrm.compensation), Integer).label('avg_compensation'),
                ).select_from(
                    ResumesOrm
                ).filter(
                    and_(
                        ResumesOrm.title.contains(like_language),
                        ResumesOrm.compensation > 40000
                    )
                ).group_by(
                    ResumesOrm.workload
                ).having(
                    cast(func.avg(ResumesOrm.compensation), Integer) > 70000
                )
            )
            # print(query.compile(compile_kwargs={"literal_binds": True}))
            result = session.execute(query)
            print(result.all())
            


class AsyncORM:
    pass
