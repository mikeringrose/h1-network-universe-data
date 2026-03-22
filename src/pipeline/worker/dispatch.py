import logging
from pathlib import Path

from pipeline.sources.hsd_tables.runner import ValidationFailed

logger = logging.getLogger(__name__)


class NotImplementedFileType(Exception):
    """Raised for file types with no pipeline implementation."""


def _process_provider(file_path: Path, database_url: str) -> None:
    import datetime
    from pipeline.sources.hsd_tables.runner import run_safe
    run_safe(file_path, datetime.date.today().year, "provider")


def _process_facility(file_path: Path, database_url: str) -> None:
    import datetime
    from pipeline.sources.hsd_tables.runner import run_safe
    run_safe(file_path, datetime.date.today().year, "facility")


def _process_beneficiary(file_path: Path, database_url: str) -> None:
    raise NotImplementedFileType("BENEFICIARY file processing is not yet implemented")


_DISPATCH = {
    "PROVIDER": "_process_provider",
    "FACILITY": "_process_facility",
    "BENEFICIARY": "_process_beneficiary",
}

_current_module = __import__(__name__, fromlist=[""])


def dispatch(file_type: str, file_path: Path, database_url: str) -> None:
    handler_name = _DISPATCH.get(file_type)
    if handler_name is None:
        raise ValueError(f"Unknown file_type: {file_type!r}")
    handler = getattr(_current_module, handler_name)
    handler(file_path, database_url)
