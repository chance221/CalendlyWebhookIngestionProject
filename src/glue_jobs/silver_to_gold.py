"""AWS Glue PySpark Silver-to-Gold job:
- Joins silver.calendly_events (filtered to paid ad channels) with silver.campaign_spend on
  channel and booking_date = spend_date.
- Computes daily and channel-level metrics: spend_date, channel, total_spend, total_bookings,
  active_bookings, canceled_bookings, and cost_per_booking (total_spend / NULLIF(active_bookings, 0)).
- Writes/merges into the Apache Iceberg table gold.campaign_attribution_summary partitioned by month(spend_date).
"""

from __future__ import annotations

import logging
import sys
from collections import defaultdict
from typing import Any

from glue_jobs.schemas import PAID_CHANNELS

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_gold_attribution_ddl(catalog: str = "glue_catalog", database: str = "gold") -> str:
    """Return Apache Iceberg DDL for gold.campaign_attribution_summary partitioned by months(spend_date)."""
    return f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{database}.campaign_attribution_summary (
        spend_date DATE,
        channel STRING,
        total_spend DOUBLE,
        total_bookings BIGINT,
        active_bookings BIGINT,
        canceled_bookings BIGINT,
        cost_per_booking DOUBLE
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


def get_gold_attribution_merge_sql(
    catalog: str = "glue_catalog",
    silver_db: str = "silver",
    gold_db: str = "gold",
) -> str:
    """Return the Apache Iceberg MERGE INTO SQL statement for gold.campaign_attribution_summary."""
    paid_channels_sql = ", ".join(f"'{ch}'" for ch in PAID_CHANNELS)
    return f"""
    MERGE INTO {catalog}.{gold_db}.campaign_attribution_summary AS target
    USING (
        WITH paid_events AS (
            SELECT
                booking_date,
                channel,
                COUNT(1) AS total_bookings,
                SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS active_bookings,
                SUM(CASE WHEN status = 'canceled' THEN 1 ELSE 0 END) AS canceled_bookings
            FROM {catalog}.{silver_db}.calendly_events
            WHERE channel IN ({paid_channels_sql})
            GROUP BY booking_date, channel
        ),
        spend_agg AS (
            SELECT
                spend_date,
                channel,
                SUM(spend) AS total_spend
            FROM {catalog}.{silver_db}.campaign_spend
            WHERE channel IN ({paid_channels_sql})
            GROUP BY spend_date, channel
        )
        SELECT
            s.spend_date AS spend_date,
            s.channel AS channel,
            ROUND(s.total_spend, 2) AS total_spend,
            COALESCE(e.total_bookings, 0) AS total_bookings,
            COALESCE(e.active_bookings, 0) AS active_bookings,
            COALESCE(e.canceled_bookings, 0) AS canceled_bookings,
            ROUND(
                s.total_spend / NULLIF(COALESCE(e.active_bookings, 0), 0),
                2
            ) AS cost_per_booking
        FROM spend_agg s
        LEFT JOIN paid_events e
            ON s.channel = e.channel
           AND s.spend_date = e.booking_date
    ) AS source
    ON target.spend_date = source.spend_date
       AND target.channel = source.channel
    WHEN MATCHED THEN
        UPDATE SET
            target.total_spend = source.total_spend,
            target.total_bookings = source.total_bookings,
            target.active_bookings = source.active_bookings,
            target.canceled_bookings = source.canceled_bookings,
            target.cost_per_booking = source.cost_per_booking
    WHEN NOT MATCHED THEN
        INSERT (
            spend_date,
            channel,
            total_spend,
            total_bookings,
            active_bookings,
            canceled_bookings,
            cost_per_booking
        )
        VALUES (
            source.spend_date,
            source.channel,
            source.total_spend,
            source.total_bookings,
            source.active_bookings,
            source.canceled_bookings,
            source.cost_per_booking
        )
    """.strip()


def compute_gold_attribution_summary(
    calendly_events: list[dict[str, Any]],
    campaign_spend: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Pure-Python computation of gold.campaign_attribution_summary for local unit verification."""
    bookings_by_key: dict[tuple[str, str], dict[str, int]] = defaultdict(
        lambda: {"total_bookings": 0, "active_bookings": 0, "canceled_bookings": 0}
    )

    for ev in calendly_events:
        ch = ev.get("channel")
        if ch not in PAID_CHANNELS:
            continue
        b_date = str(ev.get("booking_date"))
        key = (b_date, ch)
        bookings_by_key[key]["total_bookings"] += 1
        if ev.get("status") == "active":
            bookings_by_key[key]["active_bookings"] += 1
        elif ev.get("status") == "canceled":
            bookings_by_key[key]["canceled_bookings"] += 1

    spend_by_key: dict[tuple[str, str], float] = defaultdict(float)
    for sp in campaign_spend:
        ch = sp.get("channel")
        if ch not in PAID_CHANNELS:
            continue
        s_date = str(sp.get("spend_date"))
        spend_by_key[(s_date, ch)] += float(sp.get("spend", 0.0))

    results: list[dict[str, Any]] = []
    for (spend_date, channel), total_spend in sorted(spend_by_key.items()):
        counts = bookings_by_key.get(
            (spend_date, channel),
            {"total_bookings": 0, "active_bookings": 0, "canceled_bookings": 0},
        )
        active = counts["active_bookings"]
        total_spend_rounded = round(total_spend, 2)
        cpb = round(total_spend_rounded / active, 2) if active > 0 else None

        results.append(
            {
                "spend_date": spend_date,
                "channel": channel,
                "total_spend": total_spend_rounded,
                "total_bookings": counts["total_bookings"],
                "active_bookings": active,
                "canceled_bookings": counts["canceled_bookings"],
                "cost_per_booking": cpb,
            }
        )

    return results


def run_spark_silver_to_gold(
    spark: Any,
    catalog: str = "glue_catalog",
    silver_db: str = "silver",
    gold_db: str = "gold",
) -> None:
    """Execute the Silver-to-Gold Iceberg aggregation in PySpark."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{gold_db}")
    spark.sql(get_gold_attribution_ddl(catalog=catalog, database=gold_db))
    spark.sql(
        get_gold_attribution_merge_sql(
            catalog=catalog,
            silver_db=silver_db,
            gold_db=gold_db,
        )
    )


def main() -> None:
    """AWS Glue entry point for Silver-to-Gold attribution summary."""
    from awsglue.utils import getResolvedOptions
    from pyspark.sql import SparkSession

    args = getResolvedOptions(sys.argv, ["GOLD_BUCKET"])
    silver_db = "calendly_silver"
    gold_db = "calendly_gold"
    if "--SILVER_DATABASE" in sys.argv:
        silver_db = getResolvedOptions(sys.argv, ["SILVER_DATABASE"])["SILVER_DATABASE"]
    if "--GOLD_DATABASE" in sys.argv:
        gold_db = getResolvedOptions(sys.argv, ["GOLD_DATABASE"])["GOLD_DATABASE"]

    spark = (
        SparkSession.builder.appName("calendly-silver-to-gold")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.warehouse", f"s3://{args['GOLD_BUCKET']}/warehouse")
        .config(
            "spark.sql.catalog.glue_catalog.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog",
        )
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .getOrCreate()
    )

    run_spark_silver_to_gold(spark=spark, silver_db=silver_db, gold_db=gold_db)
    logger.info("Silver-to-Gold job completed successfully.")


if __name__ == "__main__":
    main()
