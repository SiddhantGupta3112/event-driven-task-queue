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
        is_local = os.getenv("IS_LOCAL", "true").lower() == "true"
        logging.info(f"IS_LOCAL raw: {os.getenv('IS_LOCAL')}")
        logging.info(f"is_local: {is_local}")

        if is_local:
            USER = os.getenv("POSTGRES_USER")
            PASSWORD = os.getenv("POSTGRES_PASSWORD")
            DB = os.getenv("POSTGRES_DB")
            db_host = "localhost"
            port = "5432"
            DATABASE_URL = f"postgresql://{USER}:{PASSWORD}@{db_host}:{port}/{DB}"
        else:
            DATABASE_URL = os.getenv("DATABASE_URL")
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
        