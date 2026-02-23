from utils.ServerLogger import ServerLogger
import os
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

logger = ServerLogger()

SQL_DATABASE_URL = os.getenv("SQL_DATABASE_URL")

if not SQL_DATABASE_URL:
    logger.error("🛑 SQL_DATABASE_URL is MISSING from environment!")
else:
    # Safely log host info without password
    from urllib.parse import urlparse
    parsed = urlparse(SQL_DATABASE_URL)
    logger.info(f"🔗 Connecting to SQL at {parsed.hostname}")

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
