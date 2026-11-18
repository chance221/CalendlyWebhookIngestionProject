"""AWS Glue PySpark Bronze-to-Silver job:
- Fetches rolling 30-day campaign spend from external S3 endpoint (Day - 1) and archives to Bronze.
- Validates Calendly webhooks and campaign spend records using Pydantic contracts, routing invalid records to SQS DLQ.
- Executes low-churn Apache Iceberg MERGE INTO upserts (merge-on-read) gated by SHA-256 row_hash comparisons.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any

import boto3
import requests
from pydantic import ValidationError

from glue_jobs.schemas import CalendlyWebhookContract, CampaignSpendContract

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

EXTERNAL_SPEND_BASE_URL = "https://dea-data-bucket.s3.us-east-1.amazonaws.com/calendly_spend_data"


def resolve_target_date(target_date_str: str | None = None, reference_date: date | None = None) -> str:
    """Resolve the Day - 1 target date (YYYY-MM-DD) for external spend fetching."""
    if target_date_str and target_date_str.strip() and target_date_str.strip().lower() != "yesterday":
        cleaned = target_date_str.strip()
        datetime.strptime(cleaned, "%Y-%m-%d")
        return cleaned
    ref = reference_date or datetime.now(timezone.utc).date()
    return (ref - timedelta(days=1)).strftime("%Y-%m-%d")


def build_external_spend_url(
    target_date_str: str | None = None,
    reference_date: date | None = None,
    base_url: str = EXTERNAL_SPEND_BASE_URL,
) -> str:
    """Construct the external S3 HTTP endpoint URL for Day - 1 rolling campaign spend."""
    resolved_date = resolve_target_date(target_date_str, reference_date)
    return f"{base_url.rstrip('/')}/spend_data_{resolved_date}.json"


def build_bronze_spend_archive_key(target_date_str: str) -> str:
    """Build the S3 Bronze archive key for the fetched rolling campaign spend file."""
    return f"campaign_spend/dt={target_date_str}/spend_data_{target_date_str}.json"


def fetch_and_archive_campaign_spend(
    bronze_bucket: str,
    target_date_str: str | None = None,
    s3_client: Any = None,
    http_session: Any = None,
) -> tuple[list[dict[str, Any]], str]:
    """Fetch Day - 1 campaign spend JSON from the external endpoint and archive a raw copy to S3 Bronze."""
    resolved_date = resolve_target_date(target_date_str)
    spend_url = build_external_spend_url(resolved_date)
    archive_key = build_bronze_spend_archive_key(resolved_date)

    session = http_session or requests
    response = session.get(spend_url, timeout=30)
    response.raise_for_status()
    raw_text = response.text
    records = json.loads(raw_text)

    if not isinstance(records, list):
        raise ValueError(f"Expected JSON array from {spend_url}, got {type(records).__name__}")

    s3 = s3_client or boto3.client("s3")
    s3.put_object(
        Bucket=bronze_bucket,
        Key=archive_key,
        Body=raw_text.encode("utf-8"),
        ContentType="application/json",
    )

    return records, archive_key


def send_validation_failure_to_dlq(
    sqs_client: Any,
    dlq_url: str,
    record_source: str,
    raw_record: Any,
    validation_errors: Any,
) -> dict[str, Any]:
    """Serialize a failed record with its Pydantic validation error details to the SQS DLQ."""
    dlq_message = {
        "source": record_source,
        "quarantined_at": datetime.now(timezone.utc).isoformat(),
        "raw_record": raw_record,
        "validation_errors": validation_errors,
    }
    if sqs_client and dlq_url:
        sqs_client.send_message(
            QueueUrl=dlq_url,
            MessageBody=json.dumps(dlq_message, default=str),
        )
    return dlq_message


def validate_webhook_batch(
    raw_webhooks: list[dict[str, Any] | str],
    sqs_client: Any = None,
    dlq_url: str = "",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Validate raw Calendly webhook records with Pydantic and quarantine failures to SQS DLQ.

     Also deduplicates multiple events for the same invitee_uri within the same batch by keeping
    the latest webhook_created_at so MERGE INTO has a deterministic source view.
    """
    valid_by_uri: dict[str, dict[str, Any]] = {}
    quarantined: list[dict[str, Any]] = []

    for item in raw_webhooks:
        try:
            parsed_dict = json.loads(item) if isinstance(item, str) else item
            if not isinstance(parsed_dict, dict):
                raise ValueError("Webhook item must be a JSON object")
            contract = CalendlyWebhookContract.model_validate(parsed_dict)
            silver_row = contract.to_silver_record()
            uri = silver_row["invitee_uri"]
            existing = valid_by_uri.get(uri)
            if existing is None or silver_row["webhook_created_at"] >= existing["webhook_created_at"]:
                valid_by_uri[uri] = silver_row
        except (ValidationError, Exception) as exc:
            err_details = exc.errors() if isinstance(exc, ValidationError) else [{"msg": str(exc)}]
            dlq_msg = send_validation_failure_to_dlq(
                sqs_client=sqs_client,
                dlq_url=dlq_url,
                record_source="bronze_to_silver.calendly_webhooks",
                raw_record=item,
                validation_errors=err_details,
            )
            quarantined.append(dlq_msg)

    return list(valid_by_uri.values()), quarantined


def validate_campaign_spend_batch(
    raw_spend_records: list[dict[str, Any]],
    sqs_client: Any = None,
    dlq_url: str = "",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Validate external campaign spend records with Pydantic and quarantine failures to SQS DLQ."""
    valid_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    quarantined: list[dict[str, Any]] = []

    for item in raw_spend_records:
        try:
            if not isinstance(item, dict):
                raise ValueError("Campaign spend record must be a JSON object")
            contract = CampaignSpendContract.model_validate(item)
            silver_row = contract.to_silver_record()
            composite_key = (silver_row["channel"], silver_row["spend_date"])
            valid_by_key[composite_key] = silver_row
        except (ValidationError, Exception) as exc:
            err_details = exc.errors() if isinstance(exc, ValidationError) else [{"msg": str(exc)}]
            dlq_msg = send_validation_failure_to_dlq(
                sqs_client=sqs_client,
                dlq_url=dlq_url,
                record_source="bronze_to_silver.campaign_spend",
                raw_record=item,
                validation_errors=err_details,
            )
            quarantined.append(dlq_msg)

    return list(valid_by_key.values()), quarantined


def simulate_iceberg_merge_calendly_events(
    target_table: dict[str, dict[str, Any]],
    source_rows: list[dict[str, Any]],
) -> dict[str, int]:
    """In-memory simulation of Iceberg MERGE INTO on silver.calendly_events keyed by invitee_uri.

    Executes:
      WHEN MATCHED AND target.row_hash <> source.row_hash THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """
    inserted = 0
    updated = 0
    skipped = 0

    for src in source_rows:
        pk = src["invitee_uri"]
        existing = target_table.get(pk)
        if existing is None:
            target_table[pk] = dict(src)
            inserted += 1
        elif existing["row_hash"] != src["row_hash"]:
            target_table[pk] = dict(src)
            updated += 1
        else:
            skipped += 1

    return {"inserted": inserted, "updated": updated, "skipped": skipped}


def simulate_iceberg_merge_campaign_spend(
    target_table: dict[tuple[str, str], dict[str, Any]],
    source_rows: list[dict[str, Any]],
) -> dict[str, int]:
    """In-memory simulation of Iceberg MERGE INTO on silver.campaign_spend keyed by (channel, spend_date).

    Executes:
      WHEN MATCHED AND target.row_hash <> source.row_hash THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """
    inserted = 0
    updated = 0
    skipped = 0

    for src in source_rows:
        pk = (src["channel"], src["spend_date"])
        existing = target_table.get(pk)
        if existing is None:
            target_table[pk] = dict(src)
            inserted += 1
        elif existing["row_hash"] != src["row_hash"]:
            target_table[pk] = dict(src)
            updated += 1
        else:
            skipped += 1

    return {"inserted": inserted, "updated": updated, "skipped": skipped}


def get_calendly_events_ddl(catalog: str = "glue_catalog", database: str = "silver") -> str:
    """Return Apache Iceberg DDL for silver.calendly_events with merge-on-read configuration."""
    return f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{database}.calendly_events (
        invitee_uri STRING,
        invitee_uuid STRING,
        scheduled_event_uri STRING,
        event_type_uri STRING,
        channel STRING,
        event_type_name STRING,
        invitee_name STRING,
        invitee_email STRING,
        status STRING,
        rescheduled BOOLEAN,
        timezone STRING,
        event_start_time STRING,
        event_end_time STRING,
        booking_date DATE,
        questions_and_answers STRING,
        webhook_created_at STRING,
        row_hash STRING
    )
    USING iceberg
    PARTITIONED BY (booking_date)
    TBLPROPERTIES (
        'format-version' = '2',
        'write.merge.mode' = 'merge-on-read',
        'write.update.mode' = 'merge-on-read',
        'write.delete.mode' = 'merge-on-read'
    )
    """.strip()


def get_campaign_spend_ddl(catalog: str = "glue_catalog", database: str = "silver") -> str:
    """Return Apache Iceberg DDL for silver.campaign_spend with merge-on-read configuration."""
    return f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{database}.campaign_spend (
        channel STRING,
        spend_date DATE,
        spend DOUBLE,
        row_hash STRING
    )
    USING iceberg
    PARTITIONED BY (months(spend_date))
    TBLPROPERTIES (
        'format-version' = '2',
        'write.merge.mode' = 'merge-on-read',
        'write.update.mode' = 'merge-on-read',
        'write.delete.mode' = 'merge-on-read'
    )
    """.strip()


def get_calendly_events_merge_sql(
    source_view: str = "src_calendly_events",
    catalog: str = "glue_catalog",
    database: str = "silver",
) -> str:
    """Return the Apache Iceberg MERGE INTO SQL statement for silver.calendly_events."""
    return f"""
    MERGE INTO {catalog}.{database}.calendly_events AS target
    USING {source_view} AS source
    ON target.invitee_uri = source.invitee_uri
    WHEN MATCHED AND target.row_hash <> source.row_hash THEN
        UPDATE SET
            target.invitee_uuid = source.invitee_uuid,
            target.scheduled_event_uri = source.scheduled_event_uri,
            target.event_type_uri = source.event_type_uri,
            target.channel = source.channel,
            target.event_type_name = source.event_type_name,
            target.invitee_name = source.invitee_name,
            target.invitee_email = source.invitee_email,
            target.status = source.status,
            target.rescheduled = source.rescheduled,
            target.timezone = source.timezone,
            target.event_start_time = source.event_start_time,
            target.event_end_time = source.event_end_time,
            target.booking_date = CAST(source.booking_date AS DATE),
            target.questions_and_answers = source.questions_and_answers,
            target.webhook_created_at = source.webhook_created_at,
            target.row_hash = source.row_hash
    WHEN NOT MATCHED THEN
        INSERT (
            invitee_uri,
            invitee_uuid,
            scheduled_event_uri,
            event_type_uri,
            channel,
            event_type_name,
            invitee_name,
            invitee_email,
            status,
            rescheduled,
            timezone,
            event_start_time,
            event_end_time,
            booking_date,
            questions_and_answers,
            webhook_created_at,
            row_hash
        )
        VALUES (
            source.invitee_uri,
            source.invitee_uuid,
            source.scheduled_event_uri,
            source.event_type_uri,
            source.channel,
            source.event_type_name,
            source.invitee_name,
            source.invitee_email,
            source.status,
            source.rescheduled,
            source.timezone,
            source.event_start_time,
            source.event_end_time,
            CAST(source.booking_date AS DATE),
            source.questions_and_answers,
            source.webhook_created_at,
            source.row_hash
        )
    """.strip()


def get_campaign_spend_merge_sql(
    source_view: str = "src_campaign_spend",
    catalog: str = "glue_catalog",
    database: str = "silver",
) -> str:
    """Return the Apache Iceberg MERGE INTO SQL statement for silver.campaign_spend."""
    return f"""
    MERGE INTO {catalog}.{database}.campaign_spend AS target
    USING {source_view} AS source
    ON target.channel = source.channel
       AND target.spend_date = CAST(source.spend_date AS DATE)
    WHEN MATCHED AND target.row_hash <> source.row_hash THEN
        UPDATE SET
            target.spend = source.spend,
            target.row_hash = source.row_hash
    WHEN NOT MATCHED THEN
        INSERT (
            channel,
            spend_date,
            spend,
            row_hash
        )
        VALUES (
            source.channel,
            CAST(source.spend_date AS DATE),
            source.spend,
            source.row_hash
        )
    """.strip()


def run_spark_bronze_to_silver(
    spark: Any,
    bronze_bucket: str,
    dlq_url: str,
    target_date_str: str | None = None,
    catalog: str = "glue_catalog",
    silver_db: str = "silver",
) -> dict[str, Any]:
    """Execute the full PySpark Bronze-to-Silver pipeline on AWS Glue."""
    resolved_date = resolve_target_date(target_date_str)
    s3_client = boto3.client("s3")
    sqs_client = boto3.client("sqs")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{silver_db}")
    spark.sql(get_calendly_events_ddl(catalog=catalog, database=silver_db))
    spark.sql(get_campaign_spend_ddl(catalog=catalog, database=silver_db))

    # 1. Fetch & archive external rolling 30-day campaign spend (Day - 1)
    raw_spend_list, archive_key = fetch_and_archive_campaign_spend(
        bronze_bucket=bronze_bucket,
        target_date_str=resolved_date,
        s3_client=s3_client,
    )
    valid_spend, quarantined_spend = validate_campaign_spend_batch(
        raw_spend_records=raw_spend_list,
        sqs_client=sqs_client,
        dlq_url=dlq_url,
    )

    if valid_spend:
        spend_df = spark.createDataFrame(valid_spend)
        spend_df.createOrReplaceTempView("src_campaign_spend")
        spark.sql(
            get_campaign_spend_merge_sql(
                source_view="src_campaign_spend",
                catalog=catalog,
                database=silver_db,
            )
        )

    # 2. Read raw Calendly webhooks from S3 Bronze and validate via Pydantic
    webhooks_prefix = f"s3://{bronze_bucket}/webhooks/"
    raw_webhook_Objects: list[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bronze_bucket, Prefix="webhooks/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                body = s3_client.get_object(Bucket=bronze_bucket, Key=key)["Body"].read().decode("utf-8")
                raw_webhook_Objects.append(body)

    valid_webhooks, quarantined_webhooks = validate_webhook_batch(
        raw_webhooks=raw_webhook_Objects,
        sqs_client=sqs_client,
        dlq_url=dlq_url,
    )

    if valid_webhooks:
        webhooks_df = spark.createDataFrame(valid_webhooks)
        webhooks_df.createOrReplaceTempView("src_calendly_events")
        spark.sql(
            get_calendly_events_merge_sql(
                source_view="src_calendly_events",
                catalog=catalog,
                database=silver_db,
            )
        )

    return {
        "target_date": resolved_date,
        "archived_spend_key": archive_key,
        " webhooks_prefix": webhooks_prefix,
        "valid_spend_count": len(valid_spend),
        "quarantined_spend_count": len(quarantined_spend),
        "valid_webhooks_count": len(valid_webhooks),
        "quarantined_webhooks_count": len(quarantined_webhooks),
    }


def main() -> None:
    """AWS Glue entry point for Bronze-to-Silver transformation."""
    from awsglue.utils import getResolvedOptions
    from pyspark.sql import SparkSession

    args = getResolvedOptions(
        sys.argv,
        ["BRONZE_BUCKET", "SILVER_BUCKET", "DLQ_URL", "target_date"],
    )
    silver_db = "calendly_silver"
    if "--SILVER_DATABASE" in sys.argv:
        silver_db = getResolvedOptions(sys.argv, ["SILVER_DATABASE"])["SILVER_DATABASE"]

    spark = (
        SparkSession.builder.appName("calendly-bronze-to-silver")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{args['SILVER_BUCKET']}/warehouse")
        .config(
            "spark.sql.catalog.glue_catalog.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .getOrCreate()
    )

    summary = run_spark_bronze_to_silver(
        spark=spark,
        bronze_bucket=args["BRONZE_BUCKET"],
        dlq_url=args["DLQ_URL"],
        target_date_str=args.get("target_date"),
        silver_db=silver_db,
    )
    logger.info("Bronze-to-Silver job completed: %s", json.dumps(summary))


if __name__ == "__main__":
    main()
