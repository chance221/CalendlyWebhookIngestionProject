"""Pydantic v2 data contracts, channel attribution mapping, and SHA-256 row_hash helpers."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

# Exact lookup map from Calendly event_type URLs to paid marketing channels
EVENT_TYPE_CHANNEL_MAP: dict[str, str] = {
    "https://api.calendly.com/event_types/79652b8a-1688-4d0b-ba8f-733c70856de4": "facebook_paid_ads",
    "https://api.calendly.com/event_types/2b5580c2-13e9-4283-8b4d-56401cf31554": "youtube_paid_ads",
    "https://api.calendly.com/event_types/70022cde-e64f-49a0-9362-a815186005dc": "tiktok_paid_ads",
}

PAID_CHANNELS: tuple[str, ...] = (
    "facebook_paid_ads",
    "youtube_paid_ads",
    "tiktok_paid_ads",
)

DEFAULT_UNMAPPED_CHANNEL = "organic_or_other"
DATE_REGEX = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def map_event_type_to_channel(event_type_uri: str | None) -> str:
    """Map payload.scheduled_event.event_type URL to a marketing channel."""
    if not event_type_uri:
        return DEFAULT_UNMAPPED_CHANNEL
    return EVENT_TYPE_CHANNEL_MAP.get(event_type_uri.strip(), DEFAULT_UNMAPPED_CHANNEL)


def compute_calendly_row_hash(
    status: str | None,
    rescheduled: bool | None,
    event_start_time: str | None,
    event_end_time: str | None,
    invitee_email: str | None,
    invitee_name: str | None,
    event_type_uri: str | None,
    channel: str | None,
) -> str:
    """Compute deterministic SHA-256 row_hash for silver.calendly_events."""
    components = [
        str(status or ""),
        str(bool(rescheduled)).lower(),
        str(event_start_time or ""),
        str(event_end_time or ""),
        str(invitee_email or ""),
        str(invitee_name or ""),
        str(event_type_uri or ""),
        str(channel or ""),
    ]
    canonical = "|".join(components)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def compute_spend_row_hash(spend: float) -> str:
    """Compute deterministic SHA-256 row_hash for silver.campaign_spend based on spend."""
    canonical = f"{float(spend):.2f}"
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class ScheduledEventModel(BaseModel):
    """Pydantic contract for payload.scheduled_event."""

    model_config = ConfigDict(extra="allow")

    uri: str | None = None
    name: str | None = None
    status: str | None = None
    event_type: str = Field(..., min_length=1)
    start_time: str = Field(..., min_length=1)
    end_time: str | None = None


class CalendlyPayloadModel(BaseModel):
    """Pydantic contract for Calendly top-level payload object."""

    model_config = ConfigDict(extra="allow")

    uri: str = Field(..., min_length=1)
    name: str | None = None
    email: str | None = None
    status: str = Field(default="active")
    rescheduled: bool = Field(default=False)
    timezone: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    questions_and_answers: list[dict[str, Any]] | None = Field(default_factory=list)
    scheduled_event: ScheduledEventModel


class CalendlyWebhookContract(BaseModel):
    """Strict Pydantic contract for incoming Calendly webhook JSON envelopes."""

    model_config = ConfigDict(extra="allow")

    event: str = Field(default="invitee.created", min_length=1)
    created_at: str = Field(..., min_length=1)
    payload: CalendlyPayloadModel

    def to_silver_record(self) -> dict[str, Any]:
        """Flatten validated webhook payload into the silver.calendly_events schema."""
        invitee_uri = self.payload.uri.strip()
        invitee_uuid = invitee_uri.rstrip("/").split("/")[-1]
        event_type_uri = self.payload.scheduled_event.event_type.strip()
        channel = map_event_type_to_channel(event_type_uri)

        # Derive booking_date from payload.created_at (fallback to top-level created_at)
        raw_booking_ts = self.payload.created_at or self.created_at
        cleaned_ts = raw_booking_ts.strip()
        if cleaned_ts.endswith("Z"):
            cleaned_ts = cleaned_ts[:-1] + "+00:00"
        booking_date = (
            datetime.fromisoformat(cleaned_ts).astimezone(timezone.utc).strftime("%Y-%m-%d")
        )

        qna_json = json.dumps(self.payload.questions_and_answers or [], sort_keys=True)
        row_hash = compute_calendly_row_hash(
            status=self.payload.status,
            rescheduled=self.payload.rescheduled,
            event_start_time=self.payload.scheduled_event.start_time,
            event_end_time=self.payload.scheduled_event.end_time,
            invitee_email=self.payload.email,
            invitee_name=self.payload.name,
            event_type_uri=event_type_uri,
            channel=channel,
        )

        return {
            "invitee_uri": invitee_uri,
            "invitee_uuid": invitee_uuid,
            "scheduled_event_uri": self.payload.scheduled_event.uri,
            "event_type_uri": event_type_uri,
            "channel": channel,
            "event_type_name": self.payload.scheduled_event.name,
            "invitee_name": self.payload.name,
            "invitee_email": self.payload.email,
            "status": self.payload.status,
            "rescheduled": bool(self.payload.rescheduled),
            "timezone": self.payload.timezone,
            "event_start_time": self.payload.scheduled_event.start_time,
            "event_end_time": self.payload.scheduled_event.end_time,
            "booking_date": booking_date,
            "questions_and_answers": qna_json,
            "webhook_created_at": self.created_at,
            "row_hash": row_hash,
        }


class CampaignSpendContract(BaseModel):
    """Strict Pydantic contract for external campaign spend JSON records."""

    model_config = ConfigDict(extra="forbid")

    date: str
    channel: str = Field(..., min_length=1)
    spend: float

    @field_validator("date")
    @classmethod
    def validate_yyyy_mm_dd(cls, value: str) -> str:
        if not value or not isinstance(value, str) or not DATE_REGEX.match(value):
            raise ValueError(f"date must strictly match YYYY-MM-DD format, got: {value!r}")
        # Verify calendar validity (e.g. reject 2025-02-30)
        datetime.strptime(value, "%Y-%m-%d")
        return value

    @field_validator("channel")
    @classmethod
    def validate_channel_non_empty(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("channel must be a non-empty string")
        return cleaned

    @field_validator("spend", mode="before")
    @classmethod
    def validate_spend_non_null(cls, value: Any) -> float:
        if value is None or isinstance(value, bool):
            raise ValueError("spend must be a non-null float")
        val = float(value)
        if val < 0:
            raise ValueError("spend cannot be negative")
        return val

    def to_silver_record(self) -> dict[str, Any]:
        """Convert validated campaign spend item into silver.campaign_spend schema."""
        return {
            "channel": self.channel,
            "spend_date": self.date,
            "spend": round(float(self.spend), 2),
            "row_hash": compute_spend_row_hash(self.spend),
        }
