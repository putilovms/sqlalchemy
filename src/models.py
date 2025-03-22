import datetime
from typing import Optional, Annotated
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, func, text
from sqlalchemy import CheckConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column, relationship
from database import Base, str_256
import enum


intpk = Annotated[int, mapped_column(primary_key=True)]
created_at = Annotated[datetime.datetime, mapped_column(
    server_default=text("TIMEZONE('utc', now())"))]
updated_at = Annotated[datetime.datetime, mapped_column(
    server_default=text("TIMEZONE('utc', now())"),
    onupdate=datetime.datetime.now,
)]

# деклоративный стиль


class WorkersOrm(Base):
    __tablename__ = "workers"

    id: Mapped[intpk]
    username: Mapped[str]

    resumes: Mapped[list["ResumesOrm"]] = relationship(
        back_populates="worker",
    )

    resumes_parttime: Mapped[list["ResumesOrm"]] = relationship(
        back_populates="worker",
        primaryjoin="and_(WorkersOrm.id == ResumesOrm.worker_id, ResumesOrm.workload == 'parttime')",
        order_by="ResumesOrm.id.desc()",
        lazy="selectin"
    )


class Workload(enum.Enum):
    parttime = "parttime"
    fulltime = "fulltime"


class ResumesOrm(Base):
    __tablename__ = "resumes"

    id: Mapped[intpk]
    title: Mapped[str_256]
    compensation: Mapped[int | None]
    workload: Mapped[Workload]
    worker_id: Mapped[int] = mapped_column(
        ForeignKey("workers.id", ondelete="CASCADE"))
    created_at: Mapped[created_at]
    updated_at: Mapped[updated_at]

    worker: Mapped["WorkersOrm"] = relationship(
        back_populates="resumes",
    )

    vacancies_replied: Mapped[list["VacanciesOrm"]] = relationship(
        back_populates="resumes_replied",
        secondary="vacancies_replies",
    )

    __table_args__ = (
        Index("title_index", "title"),
        CheckConstraint("compensation > 0",
                        name="constraint_compensation_positive")
    )


class VacanciesOrm(Base):
    __tablename__ = "vacancies"

    id: Mapped[intpk]
    title: Mapped[str_256]
    compensation: Mapped[int | None]

    resumes_replied: Mapped[list["ResumesOrm"]] = relationship(
        back_populates="vacancies_replied",
        secondary="vacancies_replies",
    )


class VacanciesRepliesOrm(Base):
    __tablename__ = "vacancies_replies"

    resume_id: Mapped[int] = mapped_column(
        ForeignKey("resumes.id", ondelete="CASCADE"),
        primary_key=True,
    )
    vacancy_id: Mapped[int] = mapped_column(
        ForeignKey("vacancies.id", ondelete="CASCADE"),
        primary_key=True,
    )
    cover_latter: Mapped[str | None]

# императивный стиль


metadata_obj = MetaData()


workers_table = Table(
    "workers",
    metadata_obj,
    Column("id", Integer, primary_key=True),
    Column("username", String),
)
