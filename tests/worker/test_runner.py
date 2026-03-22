import sys
import pytest
from unittest.mock import patch
from pipeline.sources.hsd_tables.runner import run_safe, ValidationFailed


def test_run_safe_validation_failure_raises():
    with patch("pipeline.sources.hsd_tables.pipeline.run", side_effect=SystemExit(1)):
        with pytest.raises(ValidationFailed):
            run_safe("dummy.csv", 2025, "provider")


def test_run_safe_other_exit_code_reraises():
    with patch("pipeline.sources.hsd_tables.pipeline.run", side_effect=SystemExit(2)):
        with pytest.raises(SystemExit) as exc_info:
            run_safe("dummy.csv", 2025, "provider")
        assert exc_info.value.code == 2


def test_run_safe_success():
    with patch("pipeline.sources.hsd_tables.pipeline.run", return_value=None):
        run_safe("dummy.csv", 2025, "provider")  # No exception
