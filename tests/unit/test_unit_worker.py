import pytest
from unittest.mock import MagicMock, patch

from worker import worker_group_exists, consume_jobs


# ==========================================================
# Consumer Group Tests
# ==========================================================

@patch("worker.redis_client")
def test_worker_group_exists_already_present(mock_redis):

    mock_redis.xinfo_groups.return_value = [
        {"name": "workers"}
    ]

    assert worker_group_exists() is True

    mock_redis.xinfo_groups.assert_called_once_with(
        "jobs"
    )


@patch("worker.redis_client")
def test_worker_group_exists_creates_group(mock_redis):

    mock_redis.xinfo_groups.return_value = []

    assert worker_group_exists() is True

    mock_redis.xgroup_create.assert_called_once_with(
        name="jobs",
        groupname="workers",
        id="$",
        mkstream=True
    )


@patch("worker.redis_client")
def test_worker_group_exists_failure(mock_redis):

    mock_redis.xinfo_groups.side_effect = Exception()

    mock_redis.xgroup_create.side_effect = Exception()

    assert worker_group_exists() is False


# ==========================================================
# Worker Processing Tests
# ==========================================================

@patch("worker.random.choice", return_value="completed")
@patch("worker.time.sleep")
@patch("worker.get_db")
@patch("worker.redis_client")
def test_consume_job_success(
    mock_redis,
    mock_get_db,
    mock_sleep,
    mock_choice
):

    mock_redis.xreadgroup.return_value = [
        (
            "jobs",
            [
                (
                    "123-0",
                    {"task": "resize"}
                )
            ]
        )
    ]

    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.rowcount = 1

    consume_jobs(
        worker_id="worker-1",
        max_iterations=1
    )

    assert mock_cursor.execute.call_count >= 2

    mock_redis.xack.assert_called_once_with(
        "jobs",
        "workers",
        "123-0"
    )


@patch("worker.get_db")
@patch("worker.redis_client")
def test_job_already_processed(
    mock_redis,
    mock_get_db
):

    mock_redis.xreadgroup.return_value = [
        (
            "jobs",
            [
                (
                    "123-0",
                    {"task": "resize"}
                )
            ]
        )
    ]

    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    # UPDATE ... WHERE status='pending'
    # matched nothing
    mock_cursor.rowcount = 0

    consume_jobs(
        worker_id="worker-1",
        max_iterations=1
    )

    mock_redis.xack.assert_called_once_with(
        "jobs",
        "workers",
        "123-0"
    )


@patch("worker.get_db")
@patch("worker.redis_client")
def test_processing_update_failure(
    mock_redis,
    mock_get_db
):

    mock_redis.xreadgroup.return_value = [
        (
            "jobs",
            [
                (
                    "123-0",
                    {"task": "resize"}
                )
            ]
        )
    ]

    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.execute.side_effect = Exception(
        "DB error"
    )

    consume_jobs(
        worker_id="worker-1",
        max_iterations=1
    )

    mock_redis.xack.assert_not_called()


@patch("worker.random.choice", return_value="completed")
@patch("worker.time.sleep")
@patch("worker.get_db")
@patch("worker.redis_client")
def test_final_status_update_failure(
    mock_redis,
    mock_get_db,
    mock_sleep,
    mock_choice
):

    mock_redis.xreadgroup.return_value = [
        (
            "jobs",
            [
                (
                    "123-0",
                    {"task": "resize"}
                )
            ]
        )
    ]

    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.rowcount = 1

    call_counter = 0

    def execute_side_effect(*args, **kwargs):
        nonlocal call_counter
        call_counter += 1

        if call_counter == 2:
            raise Exception("Final update failed")

    mock_cursor.execute.side_effect = execute_side_effect

    consume_jobs(
        worker_id="worker-1",
        max_iterations=1
    )

    mock_redis.xack.assert_not_called()


@patch("worker.redis_client")
def test_worker_exits_after_idle_polls(
    mock_redis
):

    mock_redis.xreadgroup.return_value = []

    consume_jobs(
        worker_id="worker-1",
        max_idle_polls=3
    )

    assert mock_redis.xreadgroup.call_count == 3