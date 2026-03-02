import os
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

SQL_DATABASE_URL = os.getenv("SQL_DATABASE_URL")

engine = create_async_engine(
    SQL_DATABASE_URL,
    echo=True,
    future=True,
    connect_args={},
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
)

AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)
