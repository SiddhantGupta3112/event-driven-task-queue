import os
import psycopg2
from dotenv import load_dotenv
from psycopg2.pool import SimpleConnectionPool
from contextlib import contextmanager
import logging

load_dotenv()

_db_pool = None

def get_pool():
    """Helper function to lazily initialize the pool only when needed."""
    global _db_pool
    if _db_pool is None:
        USER = os.getenv("POSTGRES_USER")
        PASSWORD = os.getenv("POSTGRES_PASSWORD")
        DB = os.getenv("POSTGRES_DB")

        is_local = os.getenv("IS_LOCAL", "true").lower() == "true"
        db_host = "localhost" if is_local else "postgres"

        DATABASE_URL = f"postgres://{USER}:{PASSWORD}@{db_host}:5432/{DB}"
        logging.info(f"Database setup: Database URL is -{DATABASE_URL}")
        _db_pool = SimpleConnectionPool(1, 10, dsn=DATABASE_URL)
    return _db_pool

@contextmanager
def get_db():
    pool = get_pool()
    conn = pool.getconn()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)
        