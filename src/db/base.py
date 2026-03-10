from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from src.core.config import settings

source_engine = create_engine(
    settings.SOURCE_DATABASE_URL if hasattr(settings, "SOURCE_DATABASE_URL") else settings.DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=1800,  # Recycle connections every 30 minutes
    pool_size=10,
    max_overflow=20,
    future=True,
    connect_args={"connect_timeout": 60}
)

target_engine = create_engine(
    settings.TARGET_DATABASE_URL if hasattr(settings, "TARGET_DATABASE_URL") else settings.DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=1800,
    pool_size=10,
    max_overflow=20,
    future=True,
    connect_args={"connect_timeout": 60}
)

SourceSessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=source_engine,
    future=True
)

TargetSessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=target_engine,
    future=True
)

# Backward-compatible aliases
engine = target_engine
SessionLocal = TargetSessionLocal

Base = declarative_base()
