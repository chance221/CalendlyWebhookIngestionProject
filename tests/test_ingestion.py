"""Unit tests for the Calendly Ingestion Lambda handler and deterministic S3 Bronze naming."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ingestion_lambda.handler import (  # noqa: E402
    compact_timestamp,
    extract_invitee_uuid,
    extract_partition_date,
    format_bronze_s3_key,
    lambda_handler,
)

SAMPLE_DATA_DIR = Path(__file__).parent / "sample_data"


def test_compact_timestamp_and_partition_date() -> None:
    iso_ts = "2025-07-09T06:04:34.000000Z"
    assert compact_timestamp(iso_ts) == "20250709T060434Z"
    assert extract_partition_date(iso_ts) == "2025-07-09"


def test_extract_invitee_uuid() -> None:
    uri = (
        "https://api.calendly.com/scheduled_events/"
        "1ac9e88e-eae3-4e4b-b979-d770cff02d72/invitees/"
        "22a0f2d6-1bde-4fc1-95c1-d969df1da21d"
    )
    assert extract_invitee_uuid(uri) == "22a0f2d6-1bde-4fc1-95c1-d969df1da21d"


def test_format_bronze_s3_key_created_payload() -> None:
    raw_body = (SAMPLE_DATA_DIR / "calendly_invitee_created.json").read_text(encoding="utf-8")
    s3_key, is_valid = format_bronze_s3_key(raw_body)
    assert is_valid is True
    assert s3_key == (
        "webhooks/dt=2025-07-09/"
        "22a0f2d6-1bde-4fc1-95c1-d969df1da21d_invitee.created_20250709T060434Z.json"
    )


def test_format_bronze_s3_key_canceled_payload() -> None:
    raw_body = (SAMPLE_DATA_DIR / "calendly_invitee_canceled.json").read_text(encoding="utf-8")
    s3_key, is_valid = format_bronze_s3_key(raw_body)
    assert is_valid is True
    assert s3_key == (
        "webhooks/dt=2025-07-09/"
        "22a0f2d6-1bde-4fc1-95c1-d969df1da21d_invitee.canceled_20250709T081510Z.json"
    )


def test_format_bronze_s3_key_fallback_on_corrupt_json() -> None:
    corrupt_body = "{not-valid-json: true"
    expected_hash = hashlib.sha256(corrupt_body.encode("utf-8")).hexdigest()
    s3_key, is_valid = format_bronze_s3_key(corrupt_body, fallback_date="2025-07-09")
    assert is_valid is False
    assert s3_key == f"webhooks/dt=2025-07-09/{expected_hash}.json"


def test_lambda_handler_processes_valid_and_malformed_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BRONZE_BUCKET", "test-bronze-bucket")
    monkeypatch.setenv("DLQ_URL", "https://sqs.us-east-1.amazonaws.com/123456789012/test-dlq")

    valid_body = (SAMPLE_DATA_DIR / "calendly_invitee_created.json").read_text(encoding="utf-8")
    corrupt_body = "<<<corrupted_payload>>>"

    sqs_event = {
        "Records": [
            {"messageId": "msg-valid-1", "body": valid_body},
            {"messageId": "msg-corrupt-2", "body": corrupt_body},
        ]
    }

    mock_s3 = MagicMock()
    mock_sqs = MagicMock()

    response = lambda_handler(sqs_event, s3_client=mock_s3, sqs_client=mock_sqs)

    assert response["batchItemFailures"] == []
    assert len(response["processedKeys"]) == 2
    assert response["dlqCount"] == 1
    assert mock_s3.put_object.call_count == 2
    assert mock_sqs.send_message.call_count == 1

    dlq_call_kwargs = mock_sqs.send_message.call_args.kwargs
    assert dlq_call_kwargs["QueueUrl"] == "https://sqs.us-east-1.amazonaws.com/123456789012/test-dlq"
    dlq_payload = json.loads(dlq_call_kwargs["MessageBody"])
    assert dlq_payload["message_id"] == "msg-corrupt-2"
    assert dlq_payload["raw_body"] == corrupt_body
