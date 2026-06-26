import pytest
from unittest.mock import MagicMock, patch

from reaper import cleanup_jobs


# ==========================================================
# No pending jobs
# ==========================================================

@patch("reaper.redis_client")
def test_cleanup_jobs_no_pending(mock_redis):

    mock_redis.xpending_range.return_value = []

    cleanup_jobs("reaper-1")

    mock_redis.xpending_range.assert_called_once()


# ==========================================================
# xpending_range failure
# ==========================================================

@patch("reaper.redis_client")
def test_cleanup_jobs_pending_query_failure(mock_redis):

    mock_redis.xpending_range.side_effect = Exception("redis error")

    cleanup_jobs("reaper-1")


# ==========================================================
# xclaim failure
# ==========================================================

@patch("reaper.redis_client")
def test_cleanup_jobs_claim_failure(mock_redis):

    mock_redis.xpending_range.return_value = [
        {"message_id": "1-0"}
    ]

    mock_redis.xclaim.side_effect = Exception("claim failed")

    cleanup_jobs("reaper-1")


# ==========================================================
# xclaim returns empty
# ==========================================================

@patch("reaper.redis_client")
def test_cleanup_jobs_empty_claim(mock_redis):

    mock_redis.xpending_range.return_value = [
        {"message_id": "1-0"}
    ]

    mock_redis.xclaim.return_value = []

    cleanup_jobs("reaper-1")


# ==========================================================
# Job already completed
# ==========================================================

@patch("reaper.redis_client")
@patch("reaper.get_db")
def test_cleanup_jobs_already_completed(
    mock_get_db,
    mock_redis
):

    mock_redis.xpending_range.return_value = [
        {"message_id": "1-0"}
    ]

    mock_redis.xclaim.return_value = [
        ("1-0", {"task": "x"})
    ]

    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.rowcount = 0

    cleanup_jobs("reaper-1")

    mock_redis.xack.assert_called_once_with(
        "jobs",
        "workers",
        "1-0"
    )


# ==========================================================
# Successful recovery -> completed
# ==========================================================

@patch("reaper.redis_client")
@patch("reaper.get_db")
@patch("reaper.time.sleep")
@patch("reaper.random.choice")
def test_cleanup_jobs_completed(
    mock_choice,
    mock_sleep,
    mock_get_db,
    mock_redis
):

    mock_choice.return_value = "completed"

    mock_redis.xpending_range.return_value = [
        {"message_id": "1-0"}
    ]

    mock_redis.xclaim.return_value = [
        ("1-0", {"task": "x"})
    ]

    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.rowcount = 1

    cleanup_jobs("reaper-1")

    assert mock_redis.xack.called


# ==========================================================
# Successful recovery -> failed
# ==========================================================

@patch("reaper.redis_client")
@patch("reaper.get_db")
@patch("reaper.time.sleep")
@patch("reaper.random.choice")
def test_cleanup_jobs_failed(
    mock_choice,
    mock_sleep,
    mock_get_db,
    mock_redis
):

    mock_choice.return_value = "failed"

    mock_redis.xpending_range.return_value = [
        {"message_id": "1-0"}
    ]

    mock_redis.xclaim.return_value = [
        ("1-0", {"task": "x"})
    ]

    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.rowcount = 1

    cleanup_jobs("reaper-1")

    assert mock_redis.xack.called


# ==========================================================
# Final status update fails
# ==========================================================

@patch("reaper.redis_client")
@patch("reaper.get_db")
@patch("reaper.time.sleep")
def test_cleanup_jobs_final_update_failure(
    mock_sleep,
    mock_get_db,
    mock_redis
):

    mock_redis.xpending_range.return_value = [
        {"message_id": "1-0"}
    ]

    mock_redis.xclaim.return_value = [
        ("1-0", {"task": "x"})
    ]

    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.rowcount = 1

    execute_counter = {"count": 0}

    def execute_side_effect(*args, **kwargs):
        execute_counter["count"] += 1

        if execute_counter["count"] == 2:
            raise Exception("update failed")

    mock_cursor.execute.side_effect = execute_side_effect

    cleanup_jobs("reaper-1")


# ==========================================================
# ACK failure
# ==========================================================

@patch("reaper.redis_client")
@patch("reaper.get_db")
@patch("reaper.time.sleep")
def test_cleanup_jobs_ack_failure(
    mock_sleep,
    mock_get_db,
    mock_redis
):

    mock_redis.xpending_range.return_value = [
        {"message_id": "1-0"}
    ]

    mock_redis.xclaim.return_value = [
        ("1-0", {"task": "x"})
    ]

    mock_redis.xack.side_effect = Exception("ack failed")

    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    mock_cursor.rowcount = 1

    cleanup_jobs("reaper-1")