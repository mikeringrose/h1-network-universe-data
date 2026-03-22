"""Pipeline configuration via pydantic-settings (.env)."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = "postgresql://localhost:5432/pipeline"
    api_database_url: str = "postgresql://localhost:5432/api"
    data_dir: str = "data"

    # SQS
    sqs_queue_url: str = "http://localhost:4566/000000000000/org-files"
    sqs_wait_time_seconds: int = 20
    sqs_max_messages: int = 1
    # AWS / S3
    aws_region: str = "us-east-1"
    aws_endpoint_url: str | None = None
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None


def get_settings() -> Settings:
    return Settings()
