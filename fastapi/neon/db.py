from sqlalchemy import create_engine
import os

DATABASE_URL = os.getenv("DATABASE_URL", "")
# Railway/Neon supply postgres:// or postgresql:// — rewrite to use psycopg v3
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)

engine = create_engine(
    DATABASE_URL,
    connect_args={"sslmode": "require"},
    pool_size=5,
    max_overflow=2,
    pool_timeout=30,
    pool_recycle=300,
    pool_pre_ping=True,
)
