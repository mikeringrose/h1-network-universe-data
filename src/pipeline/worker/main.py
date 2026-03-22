import json
import logging

import psycopg2

from pipeline.config import get_settings
from pipeline.worker.db import get_connection, claim_job, mark_completed, mark_failed
from pipeline.worker.dispatch import dispatch, NotImplementedFileType
from pipeline.sources.hsd_tables.runner import ValidationFailed
from pipeline.worker.s3 import make_s3_client, download_to_tempfile

logger = logging.getLogger(__name__)


def _make_sqs_client(settings):
    import boto3
    kwargs: dict = {"region_name": settings.aws_region}
    if settings.aws_endpoint_url:
        kwargs["endpoint_url"] = settings.aws_endpoint_url
    if settings.aws_access_key_id:
        kwargs["aws_access_key_id"] = settings.aws_access_key_id
        kwargs["aws_secret_access_key"] = settings.aws_secret_access_key
    return boto3.client("sqs", **kwargs)


def process_message(message: dict, settings) -> None:
    """
    Process one SQS message. Returns normally → caller should delete message.
    Raises psycopg2.OperationalError or botocore ClientError → caller must NOT delete message.
    """
    import botocore.exceptions

    body = json.loads(message["Body"])
    file_id = body["fileId"]

    conn = get_connection(settings.api_database_url)
    try:
        org_file = claim_job(conn, file_id)
        if org_file is None:
            logger.warning("File %s not found or not PENDING; skipping", file_id)
            return

        s3 = make_s3_client(settings)
        # S3 ClientError propagates — caller will NOT delete message
        tmp_path = download_to_tempfile(s3, org_file.s3_bucket, org_file.s3_key)

        try:
            try:
                dispatch(
                    org_file.file_type,
                    tmp_path,
                    settings.database_url,
                    org_file=org_file,
                    api_database_url=settings.api_database_url,
                )
            except (ValidationFailed, NotImplementedFileType, ValueError) as exc:
                mark_failed(conn, file_id, str(exc))
                return
            except Exception as exc:
                logger.exception("Unexpected error processing file %s", file_id)
                mark_failed(conn, file_id, f"Unexpected processing error: {exc}")
                return

            mark_completed(conn, file_id)
        finally:
            tmp_path.unlink(missing_ok=True)
    finally:
        conn.close()


def run_forever() -> None:
    import botocore.exceptions

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = get_settings()
    sqs = _make_sqs_client(settings)

    logger.info("Worker starting. Queue: %s", settings.sqs_queue_url)

    while True:
        response = sqs.receive_message(
            QueueUrl=settings.sqs_queue_url,
            MaxNumberOfMessages=settings.sqs_max_messages,
            WaitTimeSeconds=settings.sqs_wait_time_seconds,
            AttributeNames=["All"],
        )
        messages = response.get("Messages", [])
        if not messages:
            continue

        for message in messages:
            receipt_handle = message["ReceiptHandle"]
            should_delete = False
            try:
                process_message(message, settings)
                should_delete = True
            except psycopg2.OperationalError:
                logger.exception("DB connection lost for message; leaving in queue")
            except botocore.exceptions.ClientError:
                logger.exception("S3 error for message; leaving in queue for retry")
            except Exception:
                logger.exception("Unexpected error for message; leaving in queue")

            if should_delete:
                sqs.delete_message(
                    QueueUrl=settings.sqs_queue_url,
                    ReceiptHandle=receipt_handle,
                )
                logger.info("Deleted SQS message %s", receipt_handle[:20])
