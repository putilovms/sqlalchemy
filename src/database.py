import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import URL, create_engine, text
from config import settings

sync_engine = create_engine(
    url=settings.DATABASE_URL_psycopg,
    echo=True,
    pool_size=5, # количество соединений в пуле
    max_overflow=10, # количество дополнительных соединений в пуле
)


# with sync_engine.connect() as conn:
#     result = conn.execute(text('SELECT VERSION()'))
#     print(result.first())


async_engine = create_async_engine(
    url=settings.DATABASE_URL_asyncpg,
    echo=False,
)


async def get_async_conn():
    async with async_engine.connect() as conn:
        result = await conn.execute(text('SELECT VERSION()'))
        print(result.first())

asyncio.run(get_async_conn())
