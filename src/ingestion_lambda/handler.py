"""AWS Lambda handler for consuming Calendly webhook events from SQS and landing to S3 Bronze."""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def compact_timestamp(iso_ts: str) -> str:
    """Convert an ISO-8601 timestamp string to compact UTC format (e.g., 20250709T060434Z)."""
    cleaned = iso_ts.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    dt = datetime.fromisoformat(cleaned).astimezone(timezone.utc)
    return dt.strftime("%Y%m%dT%H%M%SZ")


def extract_partition_date(iso_ts: str) -> str:
    """Extract YYYY-MM-DD partition date in UTC from an ISO-8601 timestamp."""
    cleaned = iso_ts.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    dt = datetime.fromisoformat(cleaned).astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%d")


def extract_invitee_uuid(payload_uri: str) -> str:
    """Extract the trailing UUID segment from payload.uri."""
    stripped = payload_uri.rstrip("/")
    if not stripped:
        raise ValueError("Empty payload.uri")
    uuid_segment = stripped.split("/")[-1]
    if not uuid_segment:
        raise ValueError(f"Could not extract invitee_uuid from URI: {payload_uri}")
    return uuid_segment


def format_bronze_s3_key(raw_body: str, fallback_date: str | None = None) -> tuple[str, bool]:
    """Build deterministic S3 Bronze object key from raw webhook payload body.

    Returns:
        tuple[str, bool]: (s3_key, is_valid_transport)
        - For valid JSON containing payload.uri, event, and created_at:
          ("webhooks/dt=YYYY-MM-DD/{invitee_uuid}_{event}_{created_at}.json", True)
        - Fallback if raw_body is unparseable JSON or missing required routing fields:
          ("webhooks/dt=YYYY-MM-DD/{sha256_hash}.json", False)
    """
    default_dt = fallback_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    body_hash = hashlib.sha256(raw_body.encode("utf-8")).hexdigest()

    try:
        parsed = json.loads(raw_body)
        if not isinstance(parsed, dict):
            raise ValueError("Top-level JSON payload must be an object")

        created_at_raw = parsed.get("created_at")
        event_name = parsed.get("event")
        payload_obj = parsed.get("payload")
        if not isinstance(payload_obj, dict):
            raise ValueError("Missing or non-dict 'payload'")

        payload_uri = payload_obj.get("uri")
        if not created_at_raw or not event_name or not payload_uri:
            raise ValueError("Missing required routing keys: created_at, event, or payload.uri")

        invitee_uuid = extract_invitee_uuid(str(payload_uri))
        compact_ts = compact_timestamp(str(created_at_raw))
        dt_partition = extract_partition_date(str(created_at_raw))

        s3_key = f"webhooks/dt={dt_partition}/{invitee_uuid}_{event_name}_{compact_ts}.json"
        return s3_key, True
    except Exception as exc:
        logger.warning("Falling back to SHA-256 key due to malformed payload: %s", exc)
        s3_key = f"webhooks/dt={default_dt}/{body_hash}.json"
        return s3_key, False


def route_to_dlq(
    sqs_client: Any,
    dlq_url: str,
    raw_body: str,
    error_reason: str,
    message_id: str | None = None,
) -> None:
    """Send malformed payload and error context to the SQS Dead-Letter Queue."""
    dlq_envelope = {
        "source": "ingestion_lambda",
        "message_id": message_id,
        "error": error_reason,
        "raw_body": raw_body,
        "quarantined_at": datetime.now(timezone.utc).isoformat(),
    }
    sqs_client.send_message(
        QueueUrl=dlq_url,
        MessageBody=json.dumps(dlq_envelope),
    )


def lambda_handler(
    event: dict[str, Any],
    context: Any = None,
    s3_client: Any = None,
    sqs_client: Any = None,
) -> dict[str, Any]:
    """Process batched SQS records from API Gateway and write raw payloads to S3 Bronze."""
    bronze_bucket = os.environ.get("BRONZE_BUCKET", "")
    dlq_url = os.environ.get("DLQ_URL", "")

    if not bronze_bucket:
        raise RuntimeError("BRONZE_BUCKET environment variable is required")

    s3 = s3_client or boto3.client("s3")
    sqs = sqs_client or boto3.client("sqs")

    records = event.get("Records", [])
    batch_item_failures: list[dict[str, str]] = []
    processed_keys: list[str] = []
    dlq_count = 0

    for record in records:
        message_id = record.get("messageId", "unknown")
        raw_body = record.get("body", "")

        try:
            s3_key, is_valid_transport = format_bronze_s3_key(raw_body)

            # Write raw payload to S3 Bronze (either deterministic key or SHA-256 fallback key)
            s3.put_object(
                Bucket=bronze_bucket,
                Key=s3_key,
                Body=raw_body.encode("utf-8"),
                ContentType="application/json",
            )
            processed_keys.append(s3_key)

            # If fallback SHA-256 was triggered, route to DLQ for quarantine auditing
            if not is_valid_transport:
                if dlq_url:
                    route_to_dlq(
                        sqs_client=sqs,
                        dlq_url=dlq_url,
                        raw_body=raw_body,
                        error_reason="Malformed JSON or missing routing fields (used SHA-256 fallback)",
                        message_id=message_id,
                    )
                dlq_count += 1

        except Exception as exc:
            logger.error("Unrecoverable error processing message %s: %s", message_id, exc)
            if dlq_url:
                try:
                    route_to_dlq(
                        sqs_client=sqs,
                        dlq_url=dlq_url,
                        raw_body=raw_body,
                        error_reason=f"Unrecoverable ingestion failure: {exc}",
                        message_id=message_id,
                    )
                    dlq_count += 1
                except Exception as dlq_exc:
                    logger.error("Failed to send message %s to DLQ: %s", message_id, dlq_exc)
                    batch_item_failures.append({"itemIdentifier": message_id})
            else:
                batch_item_failures.append({"itemIdentifier": message_id})

    return {
        "batchItemFailures": batch_item_failures,
        "processedKeys": processed_keys,
        "dlqCount": dlq_count,
    }
