import logging
from pathlib import Path

from pipeline.sources.hsd_tables.runner import ValidationFailed

logger = logging.getLogger(__name__)


class NotImplementedFileType(Exception):
    """Raised for file types with no pipeline implementation."""


def _process_provider(file_path: Path, database_url: str, *, org_file=None, api_database_url: str | None = None) -> None:
    from pipeline.sources.org_providers.pipeline import run
    if org_file is None:
        raise ValueError("org_file is required for PROVIDER processing")
    if api_database_url is None:
        raise ValueError("api_database_url is required for PROVIDER processing")
    run(
        file_path,
        organization_id=org_file.organization_id,
        source_file_id=org_file.id,
        api_database_url=api_database_url,
    )


def _process_facility(file_path: Path, database_url: str, *, org_file=None, api_database_url: str | None = None) -> None:
    from pipeline.sources.org_facilities.pipeline import run
    if org_file is None:
        raise ValueError("org_file is required for FACILITY processing")
    if api_database_url is None:
        raise ValueError("api_database_url is required for FACILITY processing")
    run(
        file_path,
        organization_id=org_file.organization_id,
        source_file_id=org_file.id,
        api_database_url=api_database_url,
    )


def _process_beneficiary(file_path: Path, database_url: str, *, org_file=None, api_database_url: str | None = None) -> None:
    raise NotImplementedFileType("BENEFICIARY file processing is not yet implemented")


_DISPATCH = {
    "PROVIDER": "_process_provider",
    "FACILITY": "_process_facility",
    "BENEFICIARY": "_process_beneficiary",
}

_current_module = __import__(__name__, fromlist=[""])


def dispatch(file_type: str, file_path: Path, database_url: str, *, org_file=None, api_database_url: str | None = None) -> None:
    handler_name = _DISPATCH.get(file_type)
    if handler_name is None:
        raise ValueError(f"Unknown file_type: {file_type!r}")
    handler = getattr(_current_module, handler_name)
    kwargs = {}
    if org_file is not None:
        kwargs["org_file"] = org_file
    if api_database_url is not None:
        kwargs["api_database_url"] = api_database_url
    handler(file_path, database_url, **kwargs)
