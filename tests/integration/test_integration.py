import os
import pytest
import random

from db import get_db
from producer import enqueue_job, redis_client


@pytest.fixture(scope="function", autouse=True)
def setup_test_sandbox():
    """
    Ensures every integration test runs in a clean Redis/Postgres sandbox.
    """

    postgres_db = os.getenv("POSTGRES_DB")
    redis_db = os.getenv("REDIS_DB")

    if not postgres_db or not redis_db:
        raise RuntimeError(
            "CRITICAL: Test environment variables are missing! "
            "Ensure tests/.env.test is loaded."
        )

    assert "test" in postgres_db, (
        f"CRITICAL: Postgres is pointing to an unsafe DB: {postgres_db}"
    )

    assert redis_db == "1", (
        f"CRITICAL: Redis test database index should be 1, got: {redis_db}"
    )

    # Reset Redis stream
    try:
        redis_client.delete("jobs")
    except Exception:
        pass

    # Create fresh consumer group
    redis_client.xgroup_create(
        name="jobs",
        groupname="workers",
        id="$",
        mkstream=True
    )

    # Reset Postgres
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "TRUNCATE TABLE jobs RESTART IDENTITY CASCADE;"
            )

    yield

    # Cleanup after test
    try:
        redis_client.delete("jobs")
    except Exception:
        pass

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "TRUNCATE TABLE jobs RESTART IDENTITY CASCADE;"
            )


def test_real_pipeline_execution():
    """
    INTEGRATION TEST

    Verifies:
    - Postgres insert
    - Redis Stream append
    - Stream payload correctness
    """

    payload = {
        "task_type": "video_transcode",
        "resolution": "1080p"
    }

    stream_id = enqueue_job(payload)

    assert stream_id is not None
    assert "-" in stream_id

    with get_db() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT
                    id,
                    worker_id,
                    stream_id,
                    status
                FROM jobs
                WHERE stream_id = %s
                """,
                (stream_id,)
            )

            row = cur.fetchone()

            assert row is not None

            (
                db_uuid,
                db_worker_id,
                db_stream_id,
                db_status
            ) = row

            assert db_stream_id == stream_id
            assert db_worker_id is None
            assert db_status == "pending"

    stream_entries = redis_client.xrange(
        "jobs",
        min=stream_id,
        max=stream_id
    )

    assert stream_entries is not None
    assert len(stream_entries) == 1

    returned_stream_id, returned_payload = stream_entries[0]

    assert returned_stream_id == stream_id
    assert returned_payload is not None
    assert returned_payload["job_uuid"] == str(db_uuid)
    assert returned_payload["task_type"] == "video_transcode"
    assert returned_payload["resolution"] == "1080p"


def test_worker_pipeline_execution(monkeypatch):
    """
    INTEGRATION TEST

    Verifies:
    - Worker consumes job
    - Postgres status updates
    - Worker metadata updates
    - Redis ACK occurs
    - No pending messages remain
    """

    from worker import consume_jobs

    payload = {
        "task_type": "image_resize",
        "resolution": "720p"
    }

    worker_id = "integration_worker_01"

    stream_id = enqueue_job(payload)

    assert stream_id is not None

    # deterministic worker outcome
    monkeypatch.setattr(
        random,
        "choice",
        lambda _: "completed"
    )

    consume_jobs(
        worker_id=worker_id,
        max_iterations=1
    )

    with get_db() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT
                    status,
                    worker_id,
                    attempts,
                    started_at,
                    completed_at
                FROM jobs
                WHERE stream_id = %s
                """,
                (stream_id,)
            )

            row = cur.fetchone()

            assert row is not None

            (
                db_status,
                db_worker_id,
                attempts,
                started_at,
                completed_at
            ) = row

            assert db_status == "completed"
            assert db_worker_id == worker_id
            assert attempts == 1
            assert started_at is not None
            assert completed_at is not None

    pending = redis_client.xpending(
        "jobs",
        "workers"
    )

    assert pending["pending"] == 0
    assert pending["consumers"] == []
    
    
def test_reaper_recovers_processing_job(monkeypatch):
    """
    CURRENT SYSTEM LIMITATION TEST

    NOTE:
    Reaper does NOT currently recover jobs stuck in 'processing'
    in Postgres. This is a known bug.
    """

    from reaper import cleanup_jobs
    from worker import consume_jobs

    payload = {"task_type": "image_resize", "resolution": "720p"}

    worker_id = "worker-1"
    reaper_id = "reaper-1"

    stream_id = enqueue_job(payload)
    
    try:
        redis_client.xreadgroup(
            groupname="workers",
            consumername=worker_id,
            streams={"jobs": ">"},
            count=1
        )
    except Exception:
        pass

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE jobs
                SET status='processing',
                    worker_id=%s,
                    started_at=NOW()
                WHERE stream_id=%s
            """, (worker_id, stream_id))
            conn.commit()

    monkeypatch.setattr(random, "choice", lambda _: "completed")

    cleanup_jobs(reaper_id=reaper_id, idle_ms=0)

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT status
                FROM jobs
                WHERE stream_id=%s
            """, (stream_id,))

            status = cur.fetchone()[0]

    assert status in ("completed", "failed"), (
        f"BUG: reaper failed to recover processing job, got {status}"
    )
    
       
def test_reaper_ignores_completed_jobs(monkeypatch):
    from reaper import cleanup_jobs

    payload = {"task_type": "video_transcode", "resolution": "1080p"}

    stream_id = enqueue_job(payload)

    # mark completed immediately
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE jobs
                SET status = 'completed'
                WHERE stream_id = %s
                """,
                (stream_id,)
            )
            conn.commit()

    cleanup_jobs(reaper_id="reaper-1")

    # ensure no corruption
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM jobs WHERE stream_id = %s",
                (stream_id,)
            )
            status = cur.fetchone()[0]

            assert status == "completed"
            
            
def test_reaper_no_pending_jobs():
    from reaper import cleanup_jobs

    # Ensure stream exists but has no pending messages
    try:
        redis_client.delete("jobs")
    except Exception:
        pass

    redis_client.xgroup_create(
        name="jobs",
        groupname="workers",
        id="$",
        mkstream=True
    )

    # Confirm empty state
    assert redis_client.xlen("jobs") == 0

    # Run reaper
    cleanup_jobs(reaper_id="reaper-1")

    # If it completes without exception, test passes
    assert redis_client.xlen("jobs") == 0
    
    
def test_stuck_processing_job_end_to_end(monkeypatch):
    """
    CURRENT LIMITATION TEST:
    Processing-stuck jobs are not recovered yet.
    """

    from reaper import cleanup_jobs
    from worker import consume_jobs

    payload = {"task_type": "image_resize", "resolution": "720p"}

    stream_id = enqueue_job(payload)

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE jobs
                SET status='processing',
                    worker_id='dead-worker',
                    started_at=NOW()
                WHERE stream_id=%s
            """, (stream_id,))
            conn.commit()

    monkeypatch.setattr(random, "choice", lambda _: "completed")

    cleanup_jobs("reaper-1")

    consume_jobs("worker-2", max_iterations=1)

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT status FROM jobs WHERE stream_id=%s
            """, (stream_id,))
            status = cur.fetchone()[0]

    # ❗ EXPECTED FAILURE (system limitation)
    assert status == "processing", (
        "BUG: processing-stuck jobs are not recovered yet"
    )
   

    