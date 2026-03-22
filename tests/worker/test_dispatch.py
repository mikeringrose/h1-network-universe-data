import pytest
from pathlib import Path
from unittest.mock import patch
from pipeline.worker.dispatch import dispatch, NotImplementedFileType
from pipeline.sources.hsd_tables.runner import ValidationFailed


def test_dispatch_beneficiary_raises(tmp_path):
    f = tmp_path / "members.csv"
    f.write_text("a,b\n1,2\n")
    with pytest.raises(NotImplementedFileType):
        dispatch("BENEFICIARY", f, "postgresql://localhost/test")


def test_dispatch_unknown_type_raises(tmp_path):
    f = tmp_path / "file.csv"
    f.write_text("a\n1\n")
    with pytest.raises(ValueError, match="Unknown file_type"):
        dispatch("UNKNOWN", f, "postgresql://localhost/test")


def test_dispatch_provider_calls_run_safe(tmp_path):
    f = tmp_path / "provider.csv"
    f.write_text("a\n1\n")
    with patch("pipeline.worker.dispatch._process_provider") as mock:
        dispatch("PROVIDER", f, "postgresql://localhost/test")
        mock.assert_called_once_with(f, "postgresql://localhost/test")


def test_dispatch_facility_calls_run_safe(tmp_path):
    f = tmp_path / "facility.csv"
    f.write_text("a\n1\n")
    with patch("pipeline.worker.dispatch._process_facility") as mock:
        dispatch("FACILITY", f, "postgresql://localhost/test")
        mock.assert_called_once_with(f, "postgresql://localhost/test")
