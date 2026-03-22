import datetime
from dataclasses import dataclass


@dataclass(frozen=True)
class OrgFile:
    id: str
    file_type: str
    organization_id: str
    uploaded_by_id: str
    original_name: str
    mime_type: str | None
    size_bytes: int | None
    s3_bucket: str
    s3_key: str
    status: str
    error_message: str | None
    created_at: datetime.datetime
    updated_at: datetime.datetime
