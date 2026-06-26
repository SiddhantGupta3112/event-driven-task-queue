import os
import time
import random

import pytest

from producer import enqueue_job, redis_client
from db import get_db
from worker import consume_jobs


@pytest.fixture(scope="function", autouse=True)
def setup_test_sandbox():
    """
    Ensures every benchmark runs in a clean Redis/Postgres sandbox.
    """

    postgres_db = os.getenv("POSTGRES_DB")

    if not postgres_db or "test" not in postgres_db:
        pytest.fail(
            "CRITICAL PROTECTION: Benchmark targeted an unsafe non-test database context!"
        )

    # Reset Redis Stream
    try:
        redis_client.delete("jobs")
    except Exception:
        pass

    # Recreate consumer group
    try:
        redis_client.xgroup_destroy("jobs", "workers")
    except Exception:
        pass

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
        conn.commit()

    yield

    # Cleanup
    try:
        redis_client.delete("jobs")
    except Exception:
        pass

    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "TRUNCATE TABLE jobs RESTART IDENTITY CASCADE;"
            )


def test_enqueue_job_performance(benchmark):
    """
    BENCHMARK

    Measures latency distribution of enqueue_job(),
    including:
        - Postgres INSERT
        - Redis XADD
    """

    payload = {
        "task_type": "pytest_bench",
        "resolution": "4K"
    }

    def run_pipeline():
        return enqueue_job(payload)

    result_stream_id = benchmark(run_pipeline)

    assert result_stream_id is not None


NUM_JOBS = 1000


def test_worker_throughput(monkeypatch):
    """
    BENCHMARK

    Measures:
        - End-to-end worker throughput
        - Average latency per job

    Excludes enqueue time from timing.
    """

    # Remove artificial processing delay
    monkeypatch.setattr(
        time,
        "sleep",
        lambda _: None
    )

    # Deterministic completion path
    monkeypatch.setattr(
        random,
        "choice",
        lambda _: "completed"
    )

    # --------------------------------------------------
    # Preload jobs BEFORE timing starts
    # --------------------------------------------------
   
    for _ in range(NUM_JOBS):
        enqueue_job(
            {
                "task_type": "benchmark_worker",
                "resolution": "4K"
            }
        )

    # --------------------------------------------------
    # Benchmark worker only
    # --------------------------------------------------

    start = time.perf_counter()

    consume_jobs(
        worker_id="benchmark_worker",
        max_iterations=NUM_JOBS
    )

    elapsed = time.perf_counter() - start

    throughput = NUM_JOBS / elapsed
    latency_ms = (elapsed / NUM_JOBS) * 1000

    print()
    print(f"Processed {NUM_JOBS} jobs")
    print(f"Total time: {elapsed:.4f}s")
    print(f"Throughput: {throughput:.2f} jobs/sec")
    print(f"Latency: {latency_ms:.3f} ms/job")

    # --------------------------------------------------
    # Verify Postgres state
    # --------------------------------------------------

    with get_db() as conn:
        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT COUNT(*)
                FROM jobs
                WHERE status = 'completed'
                """
            )

            completed = cur.fetchone()[0]

    assert completed == NUM_JOBS

    # --------------------------------------------------
    # Verify Redis state
    # --------------------------------------------------

    pending = redis_client.xpending(
        "jobs",
        "workers"
    )

    assert pending["pending"] == 0