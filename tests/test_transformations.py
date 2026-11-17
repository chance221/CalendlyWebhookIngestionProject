"""Unit tests for Pydantic schemas, URL channel mapping, Day-1 spend fetching,
Iceberg MERGE INTO row_hash upserts, and Gold attribution summary calculations.
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from glue_jobs.bronze_to_silver import (  # noqa: E402
    build_bronze_spend_archive_key,
    build_external_spend_url,
    fetch_and_archive_campaign_spend,
    get_calendly_events_ddl,
    get_calendly_events_merge_sql,
    get_campaign_spend_ddl,
    get_campaign_spend_merge_sql,
    simulate_iceberg_merge_calendly_events,
    simulate_iceberg_merge_campaign_spend,
    validate_campaign_spend_batch,
    validate_webhook_batch,
)
from glue_jobs.schemas import (  # noqa: E402
    DEFAULT_UNMAPPED_CHANNEL,
    CalendlyWebhookContract,
    map_event_type_to_channel,
)
from glue_jobs.silver_to_gold import (  # noqa: E402
    compute_gold_attribution_summary,
    get_gold_attribution_ddl,
    get_gold_attribution_merge_sql,
)

SAMPLE_DATA_DIR = Path(__file__).parent / "sample_data"


def test_event_type_url_channel_mapping() -> None:
    """Verify exact mapping for the 3 paid ad channels and fallback to organic_or_other."""
    assert (
        map_event_type_to_channel(
            "https://api.calendly.com/event_types/79652b8a-1688-4d0b-ba8f-733c70856de4"
        )
        == "facebook_paid_ads"
    )
    assert (
        map_event_type_to_channel(
            "https://api.calendly.com/event_types/2b5580c2-13e9-4283-8b4d-56401cf31554"
        )
        == "youtube_paid_ads"
    )
    assert (
        map_event_type_to_channel(
            "https://api.calendly.com/event_types/70022cde-e64f-49a0-9362-a815186005dc"
        )
        == "tiktok_paid_ads"
    )
    assert (
        map_event_type_to_channel("https://api.calendly.com/event_types/unknown-uuid")
        == DEFAULT_UNMAPPED_CHANNEL
    )
    assert map_event_type_to_channel(None) == DEFAULT_UNMAPPED_CHANNEL


def test_day_minus_1_spend_url_and_archive_key_builder() -> None:
    """Verify Day - 1 URL resolution and Bronze archive path formatting."""
    ref_date = date(2025, 6, 25)
    url = build_external_spend_url(reference_date=ref_date)
    assert (
        url
        == "https://dea-data-bucket.s3.us-east-1.amazonaws.com/calendly_spend_data/spend_data_2025-06-24.json"
    )

    explicit_url = build_external_spend_url(target_date_str="2025-07-09")
    assert (
        explicit_url
        == "https://dea-data-bucket.s3.us-east-1.amazonaws.com/calendly_spend_data/spend_data_2025-07-09.json"
    )

    archive_key = build_bronze_spend_archive_key("2025-06-24")
    assert archive_key == "campaign_spend/dt=2025-06-24/spend_data_2025-06-24.json"


def test_fetch_and_archive_campaign_spend_to_bronze() -> None:
    """Verify external spend HTTP fetch archives raw JSON to S3 Bronze before transformation."""
    sample_spend_text = (SAMPLE_DATA_DIR / "spend_data_2025-06-24.json").read_text(encoding="utf-8")

    mock_http = MagicMock()
    mock_response = MagicMock()
    mock_response.text = sample_spend_text
    mock_http.get.return_value = mock_response

    mock_s3 = MagicMock()

    records, archive_key = fetch_and_archive_campaign_spend(
        bronze_bucket="test-bronze",
        target_date_str="2025-06-24",
        s3_client=mock_s3,
        http_session=mock_http,
    )

    assert len(records) == 3
    assert archive_key == "campaign_spend/dt=2025-06-24/spend_data_2025-06-24.json"
    mock_s3.put_object.assert_called_once()


def test_pydantic_webhook_validation_and_dlq_routing() -> None:
    """Verify strict Pydantic validation rejects missing payload/uri/scheduled_event and routes to DLQ."""
    valid_payload = json.loads(
        (SAMPLE_DATA_DIR / "calendly_invitee_created.json").read_text(encoding="utf-8")
    )

    missing_payload = {"created_at": "2025-07-09T06:04:34.000000Z", "event": "invitee.created"}
    missing_uri = {
        "created_at": "2025-07-09T06:04:34.000000Z",
        "event": "invitee.created",
        "payload": {
            "uri": None,
            "scheduled_event": {
                "event_type": "https://api.calendly.com/event_types/79652b8a-1688-4d0b-ba8f-733c70856de4",
                "start_time": "2025-07-09T17:45:00.000000Z",
            },
        },
    }
    missing_scheduled_event = {
        "created_at": "2025-07-09T06:04:34.000000Z",
        "event": "invitee.created",
        "payload": {
            "uri": "https://api.calendly.com/scheduled_events/1/invitees/2",
            "scheduled_event": None,
        },
    }

    mock_sqs = MagicMock()
    dlq_url = "https://sqs.us-east-1.amazonaws.com/123456789012/test-dlq"

    valid_rows, quarantined = validate_webhook_batch(
        [valid_payload, missing_payload, missing_uri, missing_scheduled_event],
        sqs_client=mock_sqs,
        dlq_url=dlq_url,
    )

    assert len(valid_rows) == 1
    assert len(quarantined) == 3
    assert mock_sqs.send_message.call_count == 3
    assert valid_rows[0]["channel"] == "facebook_paid_ads"
    assert valid_rows[0]["invitee_uuid"] == "22a0f2d6-1bde-4fc1-95c1-d969df1da21d"


def test_pydantic_campaign_spend_validation_and_dlq_routing() -> None:
    """Verify strict Pydantic validation rejects invalid YYYY-MM-DD date, null channel, or null spend."""
    valid_records = json.loads(
        (SAMPLE_DATA_DIR / "spend_data_2025-06-24.json").read_text(encoding="utf-8")
    )
    invalid_records = [
        {"date": "06/24/2025", "channel": "facebook_paid_ads", "spend": 100.0},
        {"date": None, "channel": "facebook_paid_ads", "spend": 100.0},
        {"date": "2025-06-24", "channel": None, "spend": 100.0},
        {"date": "2025-06-24", "channel": "tiktok_paid_ads", "spend": None},
    ]

    mock_sqs = MagicMock()
    dlq_url = "https://sqs.us-east-1.amazonaws.com/123456789012/test-dlq"

    valid_rows, quarantined = validate_campaign_spend_batch(
        valid_records + invalid_records,
        sqs_client=mock_sqs,
        dlq_url=dlq_url,
    )

    assert len(valid_rows) == 3
    assert len(quarantined) == 4
    assert mock_sqs.send_message.call_count == 4


def test_calendly_lifecycle_state_transition_via_row_hash() -> None:
    """Verify invitee.created followed by invitee.canceled updates the same payload.uri row via row_hash."""
    created_raw = json.loads(
        (SAMPLE_DATA_DIR / "calendly_invitee_created.json").read_text(encoding="utf-8")
    )
    canceled_raw = json.loads(
        (SAMPLE_DATA_DIR / "calendly_invitee_canceled.json").read_text(encoding="utf-8")
    )

    created_row = CalendlyWebhookContract.model_validate(created_raw).to_silver_record()
    canceled_row = CalendlyWebhookContract.model_validate(canceled_raw).to_silver_record()

    assert created_row["invitee_uri"] == canceled_row["invitee_uri"]
    assert created_row["status"] == "active"
    assert canceled_row["status"] == "canceled"
    assert created_row["row_hash"] != canceled_row["row_hash"]

    target_table: dict[str, dict] = {}

    # 1. Initial insert on invitee.created
    stats_insert = simulate_iceberg_merge_calendly_events(target_table, [created_row])
    assert stats_insert == {"inserted": 1, "updated": 0, "skipped": 0}
    assert target_table[created_row["invitee_uri"]]["status"] == "active"

    # 2. Duplicate replay of invitee.created is skipped (row_hash matches)
    stats_replay = simulate_iceberg_merge_calendly_events(target_table, [created_row])
    assert stats_replay == {"inserted": 0, "updated": 0, "skipped": 1}

    # 3. Lifecycle transition to invitee.canceled updates the existing row in place
    stats_cancel = simulate_iceberg_merge_calendly_events(target_table, [canceled_row])
    assert stats_cancel == {"inserted": 0, "updated": 1, "skipped": 0}
    assert len(target_table) == 1
    assert target_table[created_row["invitee_uri"]]["status"] == "canceled"
    assert target_table[created_row["invitee_uri"]]["row_hash"] == canceled_row["row_hash"]


def test_90_record_rolling_campaign_spend_restatements() -> None:
    """Verify 90-record rolling spend snapshot: 87 unchanged skipped, 2 restated updated, 1 new inserted."""
    initial_raw = json.loads(
        (SAMPLE_DATA_DIR / "spend_data_rolling_90_initial.json").read_text(encoding="utf-8")
    )
    restated_raw = json.loads(
        (SAMPLE_DATA_DIR / "spend_data_rolling_90_restated.json").read_text(encoding="utf-8")
    )

    initial_rows, q1 = validate_campaign_spend_batch(initial_raw)
    restated_rows, q2 = validate_campaign_spend_batch(restated_raw)

    assert len(q1) == 0 and len(q2) == 0
    assert len(initial_rows) == 89
    assert len(restated_rows) == 90

    target_spend_table: dict[tuple[str, str], dict] = {}

    # Seed the Silver table with the 89 initial records
    seed_stats = simulate_iceberg_merge_campaign_spend(target_spend_table, initial_rows)
    assert seed_stats == {"inserted": 89, "updated": 0, "skipped": 0}

    # Apply the 90-record rolling snapshot containing 87 unchanged, 2 restated, and 1 new row
    merge_stats = simulate_iceberg_merge_campaign_spend(target_spend_table, restated_rows)
    assert merge_stats == {"inserted": 1, "updated": 2, "skipped": 87}
    assert len(target_spend_table) == 90
    assert target_spend_table[("facebook_paid_ads", "2025-06-01")]["spend"] == 999.99
    assert target_spend_table[("youtube_paid_ads", "2025-06-01")]["spend"] == 888.88


def test_gold_attribution_summary_and_iceberg_sql() -> None:
    """Verify Gold attribution summary join, cost_per_booking math, and Iceberg MERGE SQL."""
    spend_rows = [
        {"spend_date": "2025-07-09", "channel": "facebook_paid_ads", "spend": 600.0},
        {"spend_date": "2025-07-09", "channel": "youtube_paid_ads", "spend": 450.0},
        {"spend_date": "2025-07-09", "channel": "tiktok_paid_ads", "spend": 300.0},
    ]
    calendly_rows = [
        {"booking_date": "2025-07-09", "channel": "facebook_paid_ads", "status": "active"},
        {"booking_date": "2025-07-09", "channel": "facebook_paid_ads", "status": "active"},
        {"booking_date": "2025-07-09", "channel": "facebook_paid_ads", "status": "canceled"},
        {"booking_date": "2025-07-09", "channel": "youtube_paid_ads", "status": "canceled"},
        {"booking_date": "2025-07-09", "channel": "organic_or_other", "status": "active"},
    ]

    gold_summary = compute_gold_attribution_summary(calendly_rows, spend_rows)
    by_channel = {r["channel"]: r for r in gold_summary}

    assert len(gold_summary) == 3

    fb = by_channel["facebook_paid_ads"]
    assert fb["total_spend"] == 600.0
    assert fb["total_bookings"] == 3
    assert fb["active_bookings"] == 2
    assert fb["canceled_bookings"] == 1
    assert fb["cost_per_booking"] == 300.0

    yt = by_channel["youtube_paid_ads"]
    assert yt["total_bookings"] == 1
    assert yt["active_bookings"] == 0
    assert yt["canceled_bookings"] == 1
    assert yt["cost_per_booking"] is None

    tt = by_channel["tiktok_paid_ads"]
    assert tt["total_bookings"] == 0
    assert tt["cost_per_booking"] is None

    # Verify Iceberg SQL statements include merge-on-read and row_hash conditions
    assert "merge-on-read" in get_calendly_events_ddl()
    assert "merge-on-read" in get_campaign_spend_ddl()
    assert "target.row_hash <> source.row_hash" in get_calendly_events_merge_sql()
    assert "target.row_hash <> source.row_hash" in get_campaign_spend_merge_sql()
    assert "PARTITIONED BY (month(spend_date))" in get_gold_attribution_ddl()
    assert "NULLIF" in get_gold_attribution_merge_sql()
