import os
import pytest
from producer import enqueue_job
from db import get_db

@pytest.fixture(scope="module", autouse=True)
def setup_test_db_sandbox():
    """Ensure our isolated test tables are wiped clean before performance tracking runs."""
    postgres_db = os.getenv("POSTGRES_DB")
    if not postgres_db or "test" not in postgres_db:
        pytest.fail("CRITICAL PROTECTION: Benchmark targeted an unsafe non-test database context!")
        
    with get_db() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE JOBS CASCADE;")

def test_enqueue_job_performance(benchmark):
    """
    BENCHMARK: Measures execution latency distribution of single-row injections
    through the combined Postgres write and Redis Stream append pipeline.
    """
    payload = {"task_type": "pytest_bench", "resolution": "4K"}

    # Define the core operation we want to time
    def run_pipeline():
        stream_id = enqueue_job(payload)
        return stream_id

    # Tell pytest-benchmark to repeatedly sample and track this execution loop
    result_stream_id = benchmark(run_pipeline)
    
    # Sanity verification to ensure the timed function executed correctly
    assert result_stream_id is not None