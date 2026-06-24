import os
import psycopg2
from dotenv import load_dotenv
from psycopg2.pool import SimpleConnectionPool
from contextlib import contextmanager

load_dotenv()

USER = os.getenv("POSTGRES_USER")
PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB = os.getenv("POSTGRES_DB")

is_local = os.getenv("IS_LOCAL", "true").lower() == "true"
db_host = "localhost" if is_local else "postgres"

DATABASE_URL = f"postgres://{USER}:{PASSWORD}@{db_host}:5432/{DB}"


db_pool = SimpleConnectionPool(1, 10, dsn=DATABASE_URL)

@contextmanager
def get_db():
    conn = db_pool.getconn()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)
        