import os
import pytest
from db import get_db
from producer import enqueue_job, redis_client

@pytest.fixture(scope="module", autouse=True)
def setup_and_teardown_db():
    """
    Safely runs against the isolated test database instance.
    """
    # Check the component configuration keys instead of DATABASE_URL
    postgres_db = os.getenv("POSTGRES_DB")
    redis_db = os.getenv("REDIS_DB")
    
    if not postgres_db or not redis_db:
        raise RuntimeError(
            "CRITICAL: Test environment variables are missing! Ensure tests/.env.test is loaded."
        )
        
    # Ensure both Postgres and Redis are locked onto test instances
    assert "test" in postgres_db, f"CRITICAL: Postgres is pointing to an unsafe DB: {postgres_db}!"
    assert redis_db == "1", f"CRITICAL: Redis test database index should be 1, got: {redis_db}!"
    
    # 1. Purge the test sandbox tables before running
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE JOBS RESTART IDENTITY CASCADE;")
    yield
    
    # 2. Clean up after the test completes
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE JOBS RESTART IDENTITY CASCADE;")
            
def test_real_pipeline_execution():
    """
    INTEGRATION TEST: Verifies real connection credentials, database 
    schema constraints, and live Redis Stream appends.
    """
    payload = {"task_type": "video_transcode", "resolution": "1080p"}
    worker_id = "live_worker_node_01"
    
    stream_id = enqueue_job(payload)
    
    assert stream_id is not None
    assert "-" in stream_id 
    
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, worker_id, stream_id, status FROM JOBS WHERE stream_id = %s", 
                (stream_id,)
            )
            row = cur.fetchone()
            
            assert row is not None
            db_uuid, db_worker_id, db_stream_id, db_status = row
        
            assert db_stream_id == stream_id
            assert db_status == "pending"


    stream_entries = redis_client.xrange("jobs", min=stream_id, max=stream_id)
    assert stream_entries is not None
    assert len(stream_entries) == 1
    
    returned_stream_id, returned_payload = stream_entries[0]
    assert returned_stream_id == stream_id
    assert returned_payload is not None
    assert returned_payload["job_uuid"] == str(db_uuid)
    assert returned_payload["task_type"] == "video_transcode"