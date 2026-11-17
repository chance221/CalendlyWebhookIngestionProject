# Calendly Webhook & Paid Marketing Attribution Lakehouse (AWS + Apache Iceberg)

A production-grade, end-to-end serverless data lakehouse on AWS that ingests real-time **Calendly webhook events** (`invitee.created`, `invitee.canceled`) and rolling **external campaign spend records** into a **Medallion architecture (Bronze, Silver, Gold)** backed by **Apache Iceberg**, culminating in a **Streamlit + PyIceberg** analytics dashboard.

---

## 1. Architecture Overview

```mermaid
flowchart LR
    subgraph External["External Data Sources"]
        CW["Calendly Webhooks\n(POST JSON)"]
        S3Ext["External S3 Spend Endpoint\n(Rolling 30-Day JSON, Day - 1)"]
    end

    subgraph Ingestion["Serverless Ingestion Layer"]
        APIGW["Amazon API Gateway\n(HTTP API Direct Integration)"]
        SQS["Amazon SQS\nProcessing Queue"]
        DLQ["Amazon SQS\nDead-Letter Queue (DLQ)"]
        LMB["Ingestion AWS Lambda\n(Python 3.12)"]
    end

    subgraph Lakehouse["Medallion Lakehouse (S3 + AWS Glue + Apache Iceberg)"]
        EB["Amazon EventBridge\nBatch Schedule"]
        BZ["S3 Bronze\nwebhooks/dt=YYYY-MM-DD/...\ncampaign_spend/dt=YYYY-MM-DD/..."]
        GLUE1["AWS Glue PySpark\nbronze_to_silver.py\n(Pydantic + SHA-256 MERGE)"]
        SLV["S3 Silver (Apache Iceberg)\nsilver.calendly_events\nsilver.campaign_spend"]
        GLUE2["AWS Glue PySpark\nsilver_to_gold.py"]
        GLD["S3 Gold (Apache Iceberg)\ngold.campaign_attribution_summary"]
    end

    subgraph Consumption["Analytics & Presentation"]
        ST["Streamlit Dashboard\n(PyIceberg Direct Reader)"]
    end

    CW -->|POST /webhooks/calendly| APIGW
    APIGW -->|SQS-SendMessage| SQS
    SQS -->|Batch Poll| LMB
    LMB -->|Deterministic Key| BZ
    LMB -.->|Malformed JSON| DLQ
    EB -->|Scheduled Trigger| GLUE1
    S3Ext -->|HTTP GET Day-1| GLUE1
    GLUE1 -->|Archive Raw Spend| BZ
    BZ --> GLUE1
    GLUE1 -.->|Pydantic Quarantine| DLQ
    GLUE1 -->|MERGE INTO (MoR)| SLV
    SLV --> GLUE2
    GLUE2 -->|MERGE INTO| GLD
    SLV --> ST
    GLD --> ST
```

### End-to-End Data Flow
1. **Webhook Buffering & Ingestion (`API Gateway → SQS → Lambda → S3 Bronze`):**
   - **Amazon API Gateway (HTTP API)** integrates directly with the **SQS Processing Queue** (`SQS-SendMessage`), eliminating cold-start latency and throttling risk on incoming Calendly webhooks.
   - **Ingestion Lambda (`src/ingestion_lambda/handler.py`)** consumes SQS batches, extracts `invitee_uuid` (trailing UUID from `payload.uri`), `event`, and compact UTC `created_at` (`YYYYMMDDTHHMMSSZ`), and writes raw payloads to S3 Bronze using Hive-style daily partitioning:
     `s3://<bronze-bucket>/webhooks/dt=YYYY-MM-DD/{invitee_uuid}_{event}_{created_at}.json`
   - If a transport payload is completely unparseable JSON, the handler writes a fallback object named by its SHA-256 digest (`{sha256_hash}.json`) and routes the error envelope to the **SQS Dead-Letter Queue (DLQ)**.
2. **Bronze-to-Silver Transformation (`src/glue_jobs/bronze_to_silver.py`):**
   - Triggered on an **EventBridge Schedule**, the AWS Glue PySpark job fetches the `Day - 1` (`YYYY-MM-DD`) 30-day rolling campaign spend JSON from `https://dea-data-bucket.s3.us-east-1.amazonaws.com/calendly_spend_data/spend_data_YYYY-MM-DD.json` and archives the raw file to `s3://<bronze-bucket>/campaign_spend/dt=YYYY-MM-DD/spend_data_YYYY-MM-DD.json`.
   - Enforces **strict Pydantic v2 validation** (`src/glue_jobs/schemas.py`) across both webhooks and campaign spend records, quarantining invalid records to the **SQS DLQ** without halting the batch.
   - Maps `payload.scheduled_event.event_type` URLs to paid channels (`facebook_paid_ads`, `youtube_paid_ads`, `tiktok_paid_ads`) and executes Apache Iceberg `MERGE INTO` upserts (`write.merge.mode = 'merge-on-read'`) into `silver.calendly_events` and `silver.campaign_spend` gated by SHA-256 `row_hash` comparisons.
3. **Silver-to-Gold Attribution (`src/glue_jobs/silver_to_gold.py`):**
   - Filters `silver.calendly_events` to the three paid channels and joins with `silver.campaign_spend` on `channel` and `booking_date = spend_date`.
   - Computes daily channel KPIs (`total_spend`, `total_bookings`, `active_bookings`, `canceled_bookings`, and `cost_per_booking = total_spend / NULLIF(active_bookings, 0)`) in the Apache Iceberg table `gold.campaign_attribution_summary` partitioned by `month(spend_date)`.
4. **Presentation Layer (`src/dashboard/app.py`):**
   - A **Streamlit** application that queries `gold.campaign_attribution_summary` and `silver.calendly_events` directly via **PyIceberg** against the AWS Glue Data Catalog.

---

## 2. Project Directory Structure

```text
CalendlyWebhookIngestionProject/
├── .github/
│   └── workflows/
│       └── deploy.yml                  # CI/CD: Ruff, Pytest, Terraform checks, S3 Glue script sync
├── infra/                              # Terraform IaC modules
│   ├── main.tf                         # Providers & AWS caller/region data sources
│   ├── variables.tf                    # Configurable variables (no hardcoded secrets/account IDs)
│   ├── outputs.tf                      # API Gateway URL, SQS/DLQ URLs, S3 bucket outputs
│   ├── iam.tf                          # Least-privilege IAM roles & policies
│   ├── ingestion.tf                    # HTTP API Gateway, SQS, DLQ, Ingestion Lambda
│   ├── storage.tf                      # S3 buckets (Bronze, Silver, Gold, Scripts) & Glue Catalog
│   └── processing.tf                   # AWS Glue PySpark jobs & EventBridge Batch Schedule
├── src/
│   ├── ingestion_lambda/
│   │   ├── handler.py                  # SQS consumer -> S3 Bronze deterministic writer + DLQ
│   │   └── requirements.txt
│   ├── glue_jobs/
│   │   ├── schemas.py                  # Pydantic v2 contracts, URL-to-channel map, SHA-256 row_hash
│   │   ├── bronze_to_silver.py         # PySpark Day-1 spend fetch + Pydantic validation + Iceberg MERGE
│   │   └── silver_to_gold.py           # PySpark Gold attribution summary & cost-per-booking join
│   └── dashboard/
│       ├── app.py                      # Streamlit analytics app reading Iceberg tables via PyIceberg
│       └── requirements.txt
├── tests/
│   ├── sample_data/                    # Mock webhooks (created/canceled) & 90-record rolling spend JSONs
│   ├── test_ingestion.py               # Unit tests for deterministic Bronze keys & DLQ fallback
│   └── test_transformations.py         # Unit tests for Pydantic, channel mapping, & Iceberg upserts
├── README.md
└── pyproject.toml
```

---

## 3. Architecture Decision Records (ADRs)

### ADR-001: Why SQS + Scheduled AWS Glue Batch Was Chosen Over Apache Flink / Kinesis
- **Context:** Calendly webhooks arrive sporadically when prospects book or cancel info sessions, whereas external marketing spend files are published once daily (`Day - 1` rolling 30-day snapshots).
- **Decision:** Buffer real-time webhook bursts in **Amazon SQS** via direct API Gateway integration, land raw JSON objects to S3 Bronze via a lightweight Python 3.12 Lambda, and run **AWS Glue PySpark** on an **EventBridge Schedule** (rather than per-file S3 triggers or 24/7 Flink/Kinesis streams).
- **Consequences:**
  - Eliminates ~$150–$400+/month in idle compute from always-on Kinesis shards or Managed Service for Apache Flink (KDA) clusters.
  - Prevents the Apache Iceberg small-file and metadata-churn problem that occurs when S3 object-created events trigger micro-batches for individual webhooks.

### ADR-002: Why DynamoDB Was Omitted in Favor of Deterministic Bronze Keys + SHA-256 `row_hash` Iceberg `MERGE INTO`
- **Context:** Traditional serverless pipelines often maintain an external DynamoDB state table to deduplicate webhook deliveries and track restatements in rolling 30-day spend exports (90 records per file: 3 channels × 30 days).
- **Decision:** Omit DynamoDB entirely. Enforce idempotency at two native Lakehouse boundaries:
  1. **Bronze Layer:** Deterministic S3 object keys (`webhooks/dt=YYYY-MM-DD/{invitee_uuid}_{event}_{created_at}.json`) make duplicate SQS deliveries idempotent `PutObject` overwrites.
  2. **Silver Layer (Apache Iceberg):** Configure `write.merge.mode = 'merge-on-read'` and gate updates with a deterministic SHA-256 `row_hash` comparison (`WHEN MATCHED AND target.row_hash <> source.row_hash THEN UPDATE SET ...`).
- **Consequences:**
  - Zero dual-write consistency bugs between DynamoDB and S3/Iceberg.
  - When `invitee.created` is followed later by `invitee.canceled` for the same `payload.uri`, the change in `status` produces a new `row_hash` and cleanly transitions the existing Silver record in place.
  - When a 90-record rolling campaign spend file arrives with 87 unchanged historical records, 2 restated historical records, and 1 new daily record, Iceberg skips the 87 unchanged rows without writing unnecessary positional delete files or rewriting unchanged Parquet data files.

### ADR-003: Why Campaign Attribution Maps `scheduled_event.event_type` URLs Instead of `payload.tracking` UTM Fields
- **Context:** Incoming Calendly webhook payloads have `null` values across `payload.tracking` (`utm_source`, `utm_medium`, `utm_campaign`, etc.) because each paid advertising platform directs traffic to a dedicated Calendly scheduling link (`event_type`).
- **Decision:** Derive the marketing `channel` deterministically in Silver from `payload.scheduled_event.event_type` using the canonical lookup table in `src/glue_jobs/schemas.py`:
  - `.../event_types/79652b8a-1688-4d0b-ba8f-733c70856de4` → `"facebook_paid_ads"`
  - `.../event_types/2b5580c2-13e9-4283-8b4d-56401cf31554` → `"youtube_paid_ads"`
  - `.../event_types/70022cde-e64f-49a0-9362-a815186005dc` → `"tiktok_paid_ads"`
  - Any other `event_type` URI → `"organic_or_other"`
- **Consequences:** Guarantees 100% deterministic join compatibility with `silver.campaign_spend` on `(channel, booking_date = spend_date)` even when client-side cookies or UTM parameters are stripped.

### ADR-004: How Pydantic v2 + Amazon SQS DLQ Isolates Bad Data Without Halting Batch Pipelines
- **Context:** Schema drift or upstream API anomalies (such as missing `payload.uri`, missing `scheduled_event.start_time`, or malformed `date`/`spend` values in external JSON files) can crash a PySpark batch job and block executive reporting for all valid records.
- **Decision:** Validate every Bronze record in Python using strict **Pydantic v2** contracts (`CalendlyWebhookContract` and `CampaignSpendContract`) before constructing the Spark source view for Iceberg `MERGE INTO`. Any record failing validation is serialized alongside its structured Pydantic error details and published to the **SQS Dead-Letter Queue (DLQ)**.
- **Consequences:** Valid bookings and spend records flow uninterrupted into Silver and Gold tables, while quarantined payloads remain inspectable and replayable from the SQS DLQ.

---

## 4. Local Setup, Testing & Deployment Instructions

### 1. Install Dependencies & Run Unit Tests Locally
```bash
pip install -e ".[dev,dashboard]"
ruff check src/ tests/
pytest -v
```

### 2. Run the Streamlit Analytics Dashboard
```bash
streamlit run src/dashboard/app.py
```

### 3. Validate & Deploy AWS Infrastructure via Terraform
```bash
cd infra
terraform fmt -check
terraform init
terraform validate
terraform plan -var="environment=prod" -var="aws_region=us-east-1"
terraform apply -var="environment=prod" -var="aws_region=us-east-1"
```
