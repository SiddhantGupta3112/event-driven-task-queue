from unittest.mock import MagicMock, patch
import pytest
from producer import enqueue_job 

@patch("producer.redis_client") 
@patch("producer.get_db")       
@patch("producer.uuid.uuid4")
def test_enqueue_job_success(mock_uuid, mock_get_db, mock_redis_client):
    """
    Validates that a successful database insert and Redis stream push
    returns the correct Stream ID.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_uuid.return_value = "mocked-uuid-1111"
    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    
    mock_redis_client.xadd.return_value = "1719234567890-0"
    
    payload = {"task": "process_data"}
    result_stream_id = enqueue_job(payload)
    
    assert result_stream_id == "1719234567890-0"
    assert "job_uuid" not in payload
    mock_redis_client.xadd.assert_called_once_with(
        name="jobs",
        fields={
            "task": "process_data",
            "job_uuid": "mocked-uuid-1111"
        },
        id="*"
    )


@patch("producer.redis_client")
@patch("producer.get_db")
def test_enqueue_job_redis_failure_rolls_back(mock_get_db, mock_redis_client):
    """
    Validates that if Redis breaks down mid-execution, the exception bubbles up
    to trigger a clean database rollback.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    
    mock_get_db.return_value.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    
    mock_redis_client.xadd.side_effect = Exception("Redis connection timed out!")
    
    with pytest.raises(Exception) as exc_info:
        enqueue_job({"task": "process_data"})
        
    assert "Redis connection timed out!" in str(exc_info.value)