import json
import pytest
import psycopg2
from unittest.mock import patch, MagicMock
from pipeline.worker.main import process_message
from pipeline.sources.hsd_tables.runner import ValidationFailed
from pipeline.worker.dispatch import NotImplementedFileType
from pipeline.config import Settings


def _make_message(file_id: str) -> dict:
    return {"Body": json.dumps({"fileId": file_id}), "ReceiptHandle": "handle-123"}


def _make_settings():
    return Settings(
        database_url="postgresql://localhost/test",
        sqs_queue_url="http://localhost:4566/test",
    )


def test_process_message_not_pending_returns(tmp_path):
    """claim_job returns None → function returns normally (caller deletes message)."""
    settings = _make_settings()
    with patch("pipeline.worker.main.get_connection"), \
         patch("pipeline.worker.main.claim_job", return_value=None):
        process_message(_make_message("test-id"), settings)  # No exception


def test_process_message_validation_failure_marks_failed(tmp_path):
    settings = _make_settings()
    org_file = MagicMock(s3_bucket="b", s3_key="k/file.csv", file_type="PROVIDER", id="test-id")
    fake_path = tmp_path / "file.csv"
    fake_path.write_text("data")

    with patch("pipeline.worker.main.get_connection"), \
         patch("pipeline.worker.main.claim_job", return_value=org_file), \
         patch("pipeline.worker.main.make_s3_client"), \
         patch("pipeline.worker.main.download_to_tempfile", return_value=fake_path), \
         patch("pipeline.worker.main.dispatch", side_effect=ValidationFailed("bad data")), \
         patch("pipeline.worker.main.mark_failed") as mock_fail, \
         patch("pipeline.worker.main.mark_completed") as mock_ok:
        process_message(_make_message("test-id"), settings)
        mock_fail.assert_called_once()
        mock_ok.assert_not_called()


def test_process_message_success_marks_completed(tmp_path):
    settings = _make_settings()
    org_file = MagicMock(s3_bucket="b", s3_key="k/file.csv", file_type="PROVIDER", id="test-id")
    fake_path = tmp_path / "file.csv"
    fake_path.write_text("data")

    with patch("pipeline.worker.main.get_connection"), \
         patch("pipeline.worker.main.claim_job", return_value=org_file), \
         patch("pipeline.worker.main.make_s3_client"), \
         patch("pipeline.worker.main.download_to_tempfile", return_value=fake_path), \
         patch("pipeline.worker.main.dispatch"), \
         patch("pipeline.worker.main.mark_completed") as mock_ok:
        process_message(_make_message("test-id"), settings)
        mock_ok.assert_called_once()


def test_process_message_db_error_propagates():
    settings = _make_settings()
    with patch("pipeline.worker.main.get_connection", side_effect=psycopg2.OperationalError("down")):
        with pytest.raises(psycopg2.OperationalError):
            process_message(_make_message("test-id"), settings)


def test_process_message_s3_error_propagates(tmp_path):
    import botocore.exceptions
    settings = _make_settings()
    org_file = MagicMock(s3_bucket="b", s3_key="k/file.csv", file_type="PROVIDER", id="test-id")

    with patch("pipeline.worker.main.get_connection"), \
         patch("pipeline.worker.main.claim_job", return_value=org_file), \
         patch("pipeline.worker.main.make_s3_client"), \
         patch("pipeline.worker.main.download_to_tempfile",
               side_effect=botocore.exceptions.ClientError({"Error": {"Code": "NoSuchKey", "Message": ""}}, "GetObject")):
        with pytest.raises(botocore.exceptions.ClientError):
            process_message(_make_message("test-id"), settings)
