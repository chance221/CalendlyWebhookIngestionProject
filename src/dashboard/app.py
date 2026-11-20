"""Streamlit analytics dashboard querying Gold & Silver Apache Iceberg tables via PyIceberg."""

from __future__ import annotations

import os
from datetime import date, timedelta

import pandas as pd
import streamlit as st

PAID_CHANNELS = ("facebook_paid_ads", "youtube_paid_ads", "tiktok_paid_ads")


def load_iceberg_table_df(database: str, table_name: str, region: str) -> pd.DataFrame | None:
    """Query an AWS Glue Data Catalog-backed Apache Iceberg table directly via PyIceberg."""
    try:
        from pyiceberg.catalog import load_catalog

        catalog = load_catalog(
            "glue",
            **{
                "type": "glue",
                "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
                "s3.region": region,
                "client.region": region,
            },
        )
        table = catalog.load_table(f"{database}.{table_name}")
        arrow_table = table.scan().to_arrow()
        return arrow_table.to_pandas()
    except Exception:
        return None


def generate_demo_gold_df() -> pd.DataFrame:
    """Generate realistic demo records for gold.campaign_attribution_summary when running offline."""
    rows = []
    start_dt = date(2025, 6, 1)
    channel_profiles = {
        "facebook_paid_ads": {"base_spend": 620.0, "active": 6, "canceled": 1},
        "youtube_paid_ads": {"base_spend": 540.0, "active": 4, "canceled": 1},
        "tiktok_paid_ads": {"base_spend": 340.0, "active": 3, "canceled": 1},
    }
    for d in range(30):
        curr_date = start_dt + timedelta(days=d)
        for ch, prof in channel_profiles.items():
            spend = round(prof["base_spend"] + (d % 7) * 28.5, 2)
            active = prof["active"] + (d % 3)
            canceled = prof["canceled"] + (1 if d % 5 == 0 else 0)
            total = active + canceled
            cpb = round(spend / active, 2) if active > 0 else None
            rows.append(
                {
                    "spend_date": pd.to_datetime(curr_date),
                    "channel": ch,
                    "total_spend": spend,
                    "total_bookings": total,
                    "active_bookings": active,
                    "canceled_bookings": canceled,
                    "cost_per_booking": cpb,
                }
            )
    return pd.DataFrame(rows)


def generate_demo_silver_events_df() -> pd.DataFrame:
    """Generate realistic demo records for silver.calendly_events when running offline."""
    return pd.DataFrame(
        [
            {
                "booking_date": "2025-06-30",
                "channel": "facebook_paid_ads",
                "event_type_name": "Data Engineer Academy Info Session",
                "invitee_name": "Ninad Magdum",
                "invitee_email": "chrisgarzon19@gmail.com",
                "status": "active",
                "rescheduled": False,
                "timezone": "Asia/Calcutta",
                "event_start_time": "2025-07-09T17:45:00Z",
            },
            {
                "booking_date": "2025-06-30",
                "channel": "youtube_paid_ads",
                "event_type_name": "Data Engineer Academy Info Session",
                "invitee_name": "Alex Rivera",
                "invitee_email": "alex.rivera@example.com",
                "status": "active",
                "rescheduled": False,
                "timezone": "America/New_York",
                "event_start_time": "2025-07-10T15:00:00Z",
            },
            {
                "booking_date": "2025-06-29",
                "channel": "tiktok_paid_ads",
                "event_type_name": "Data Engineer Academy Info Session",
                "invitee_name": "Jordan Lee",
                "invitee_email": "jordan.lee@example.com",
                "status": "canceled",
                "rescheduled": False,
                "timezone": "America/Los_Angeles",
                "event_start_time": "2025-07-11T18:30:00Z",
            },
        ]
    )


def render_dashboard() -> None:
    st.set_page_config(
        page_title="Calendly Marketing Attribution Lakehouse",
        page_icon="📅",
        layout="wide",
    )

    st.title("📅 Calendly Paid Channel Attribution Dashboard")
    st.caption(
        "Powered by AWS Serverless Medallion Lakehouse "
        "(API Gateway → SQS → S3 Bronze → Glue PySpark → Apache Iceberg → PyIceberg)"
    )

    aws_region = os.environ.get("AWS_REGION", "us-east-1")
    gold_db = os.environ.get("GOLD_DATABASE", "calendly_gold")
    silver_db = os.environ.get("SILVER_DATABASE", "calendly_silver")

    with st.sidebar:
        st.header("Iceberg Catalog Settings")
        data_source = st.radio(
            "Data Source Mode",
            options=["PyIceberg (AWS Glue Catalog)", "Local Demo Snapshot"],
            index=0,
        )
        selected_channels = st.multiselect(
            "Filter Paid Channels",
            options=list(PAID_CHANNELS),
            default=list(PAID_CHANNELS),
        )

    gold_df = None
    silver_df = None
    using_fallback = False

    if data_source == "PyIceberg (AWS Glue Catalog)":
        gold_df = load_iceberg_table_df(gold_db, "campaign_attribution_summary", aws_region)
        silver_df = load_iceberg_table_df(silver_db, "calendly_events", aws_region)

    if gold_df is None or gold_df.empty:
        using_fallback = True
        gold_df = generate_demo_gold_df()
    if silver_df is None or silver_df.empty:
        silver_df = generate_demo_silver_events_df()

    if using_fallback and data_source == "PyIceberg (AWS Glue Catalog)":
        st.info(
            "AWS Glue Iceberg catalog not reachable in current environment; displaying representative sample dataset."
        )

    gold_df["spend_date"] = pd.to_datetime(gold_df["spend_date"])
    filtered_df = gold_df[gold_df["channel"].isin(selected_channels)].copy()

    # Top-level KPI metrics
    total_spend = float(filtered_df["total_spend"].sum())
    total_bookings = int(filtered_df["total_bookings"].sum())
    active_bookings = int(filtered_df["active_bookings"].sum())
    blended_cpb = total_spend / active_bookings if active_bookings > 0 else 0.0
    retention_rate = (active_bookings / total_bookings * 100.0) if total_bookings > 0 else 0.0

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Total Ad Spend", f"${total_spend:,.2f}")
    col2.metric("Total Bookings", f"{total_bookings:,}")
    col3.metric("Active Bookings", f"{active_bookings:,}")
    col4.metric("Active Conversion Rate", f"{retention_rate:.1f}%")
    col5.metric("Cost per Active Booking", f"${blended_cpb:,.2f}")

    st.divider()

    # Channel-level Attribution Summary Table & Charts
    left_col, right_col = st.columns(2)

    with left_col:
        st.subheader("Daily Ad Spend by Paid Channel")
        spend_pivot = filtered_df.pivot_table(
            index="spend_date",
            columns="channel",
            values="total_spend",
            aggfunc="sum",
        ).fillna(0.0)
        st.line_chart(spend_pivot)

    with right_col:
        st.subheader("Active vs. Canceled Bookings by Channel")
        channel_bookings = (
            filtered_df.groupby("channel", as_index=True)[["active_bookings", "canceled_bookings"]]
            .sum()
        )
        st.bar_chart(channel_bookings)

    st.subheader("Channel Performance & Cost-per-Booking Breakdown")
    channel_summary = (
        filtered_df.groupby("channel", as_index=False)
        .agg(
            total_spend=("total_spend", "sum"),
            total_bookings=("total_bookings", "sum"),
            active_bookings=("active_bookings", "sum"),
            canceled_bookings=("canceled_bookings", "sum"),
        )
    )
    channel_summary["cost_per_booking"] = channel_summary.apply(
        lambda r: round(r["total_spend"] / r["active_bookings"], 2)
        if r["active_bookings"] > 0
        else None,
        axis=1,
    )
    channel_summary["cancellation_rate_%"] = channel_summary.apply(
        lambda r: round(r["canceled_bookings"] / r["total_bookings"] * 100.0, 2)
        if r["total_bookings"] > 0
        else 0.0,
        axis=1,
    )
    st.dataframe(channel_summary, use_container_width=True, hide_index=True)

    st.subheader("Recent Calendly Invitee Events (`silver.calendly_events`)")
    paid_only_toggle = st.checkbox("Show Paid Ad Channels Only in Invitee Events Table", value=True)
    if paid_only_toggle and "channel" in silver_df.columns:
        display_silver_df = silver_df[silver_df["channel"].isin(selected_channels)]
    else:
        display_silver_df = silver_df
    st.dataframe(display_silver_df, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    render_dashboard()  
