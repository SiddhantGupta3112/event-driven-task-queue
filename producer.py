import os
import redis
from dotenv import load_dotenv
import logging
from db import get_db
from psycopg2.extras import Json
import uuid

load_dotenv()

is_local = os.getenv("IS_LOCAL", "true").lower() == "true"

if is_local:
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", 6379))
    redis_db = int(os.getenv("REDIS_DB", 0))
    redis_password = None
else:
    redis_host = os.getenv("REDISHOST")
    redis_port = int(os.getenv("REDISPORT", 6379))
    redis_db = 0
    redis_password = os.getenv("REDIS_PASSWORD")

pool = redis.ConnectionPool(
    host=redis_host,
    port=redis_port,
    db=redis_db,
    password=redis_password,
    decode_responses=True
)


redis_client = redis.Redis(connection_pool=pool)

def test_redis_connection() -> bool:
    logging.info("Testing Redis connection...")
    try:
        if redis_client.ping():
            logging.info("Redis working.")
            return True
        else:
            logging.warning("Redis not working.")
            return False
    except Exception as e:
        logging.error(f"Encountered error: {e}")
        return False
    
def enqueue_job(payload: dict) -> None | str:
    logging.info(f"Producer: Enqueing job...")
    
    job_uuid = str(uuid.uuid4())
    redis_payload = {**payload, "job_uuid": job_uuid}
    
    stream_id = None
    with get_db() as conn:
        with conn.cursor() as cur:
            try:
                logging.info("Producer: Appending event to Redis Stream...")
                stream_id = str(
                    redis_client.xadd(
                        name="jobs",
                            fields=redis_payload,
                            id="*"
                    )
                )
                
                logging.info("Producer: Inserting atomic record into Postgres...")
                cur.execute(
                    '''INSERT INTO JOBS (id, stream_id, status, payload)
                       VALUES (%s, %s, %s, %s);''',
                    (job_uuid, stream_id, "pending", Json(redis_payload))
                )
                
                conn.commit()
                logging.info("Producer: Job successfully enqueued and committed.")
            except Exception as err:
                logging.error(f"Producer: Pipeline failed. Error: {err}. Rolling back DB.")
                conn.rollback()
                raise err
    
    return stream_id
    