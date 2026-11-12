# Production YouTube ELT Pipeline

An automated, containerized EtLT (Extract, transform-Load, Transform) pipeline designed to ingest, process, and validate channel and video metrics from the YouTube Data API v3 into a multi-tier PostgreSQL data warehouse. Orchestrated via Apache Airflow with CeleryExecutor and backed by Soda Core for data contract and quality enforcement.

---

## Architecture Overview

The system processes data across three decoupled, event-driven DAGs:

```
[ YouTube Data API v3 ]
           │
           ▼
┌──────────────────────┐
│  DAG 1: produce_json │  Extracts channel uploads, batches video statistics,
└──────────┬───────────┘  and outputs date-partitioned JSON (YT_data_<ds>.json).
           │ (TriggerDagRunOperator)
           ▼
┌──────────────────────┐
│  DAG 2: update_db    │  Ingests JSON into staging.yt_api, transforms metrics,
└──────────┬───────────┘  and upserts into core.yt_api with audit tracking.
           │ (TriggerDagRunOperator)
           ▼
┌──────────────────────┐
│ DAG 3: data_quality  │  Executes declarative Soda Core scans against staging
└──────────────────────┘  and core schemas before downstream analytical exposure.
```

### DAG Workflow Details

1. **`produce_json` (Extraction & Ingestion)**
   - Resolves channel upload playlist and traverses paginated video records.
   - Chunks video IDs into 50-item batches to comply with YouTube API query limits.
   - Serializes extracted payload into `data/YT_data_<ds>.json` using Airflow execution date (`ds`) for deterministic backfill idempotency.
   - Triggers `update_db` passing payload file path via task execution context.

2. **`update_db` (Warehouse Processing & Transformation)**
   - **Staging Tier (`staging.yt_api`):** Creates schema/table if not present. Performs upsert and deletion reconciliation against current payload keys.
   - **Core Tier (`core.yt_api`):** Reads staging records, applies ISO 8601 duration parsing (converting `PT#M#S` to `TIME`), categorizes video formats (`Shorts` vs `Normal`), defensibly casts metric values to integer defaults, and updates record timestamps (`updated_at`).
   - Triggers `data_quality` upon successful transaction commit.

3. **`data_quality` (Data Contract Testing)**
   - Executes programmatic Soda Core CLI scans via Airflow `BashOperator`.
   - Validates missing count, duplicate keys, and business integrity rules (ensuring like and comment counts do not exceed view totals).

---

## Tech Stack

- **Orchestration:** Apache Airflow 2.9.2 (CeleryExecutor)
- **Containerization:** Docker Compose
- **Database Engine:** PostgreSQL 13 (Independent databases for Airflow Metadata, Celery Backend, and ELT Warehouse)
- **Message Broker:** Redis 7.2 (Celery task queue broker)
- **Data Quality:** Soda Core 3.3.14 (`soda-core-postgres`)
- **Testing:** Pytest 8.3.3
- **CI/CD:** GitHub Actions

---

## Repository Structure

```
data-engineering-1-airflow/
├── dags/
│   ├── api/
│   │   └── video_stats.py          # YouTube API ingestion tasks
│   ├── dataquality/
│   │   └── soda.py                 # Soda Core BashOperator task generator
│   ├── datawarehouse/
│   │   ├── data_loading.py         # JSON payload parser
│   │   ├── data_modification.py    # Database DML handlers (insert, update, delete)
│   │   ├── data_transformation.py  # Metric parsing, duration calculation, schema mapping
│   │   ├── data_utils.py           # Connection management and DDL operations
│   │   └── dw.py                   # Staging and core Airflow tasks
│   ├── produce_json_dag.py         # Pipeline entrypoint DAG
│   ├── update_db_dag.py            # Warehouse loading DAG
│   └── data_quality_dag.py         # Data quality scan DAG
├── docker/
│   └── postgres/
│       └── init-multiple-databases.sh # Multi-tenant database initialization script
├── include/
│   └── soda/
│       ├── checks.yml              # Declarative SodaCL test suite
│       └── configuration.yml       # Soda data source definition
├── tests/
│   ├── conftest.py                 # Pytest fixtures and mock contexts
│   ├── integration_test.py         # External connection verification
│   └── unit_test.py                # DAG structure, count, and integrity assertions
├── .env.example                    # Environment variable template
├── docker-compose.yaml             # Multi-container orchestration definition
├── dockerfile                      # Airflow extended image with custom dependencies
└── requirements.txt                # Python package requirements
```

---

## Prerequisites

- Docker Engine 24.0+ and Docker Compose v2 installed.
- Minimum 4 GB of free RAM dedicated to Docker daemon.
- A valid Google Cloud YouTube Data API v3 Key.

---

## How to Run Locally

### 1. Configure Environment Variables

Create your local `.env` file from the provided template:

```bash
cp .env.example .env
```

Open `.env` and configure your credentials:

```ini
API_KEY=your_actual_youtube_api_key
CHANNEL_HANDLE=@YourTargetChannelHandle
```

### 2. Build and Launch Containers

Initialize the multi-database setup, message broker, and Airflow cluster:

```bash
docker compose up --build -d
```

Verify all services reach healthy status:

```bash
docker compose ps
```

Expected running containers:
- `postgres` (Healthy on port `5432`)
- `redis` (Healthy on port `6379`)
- `airflow-init` (Exited with code 0)
- `airflow-webserver` (Healthy on port `8080`)
- `airflow-scheduler` (Healthy)
- `airflow-worker` (Healthy)

### 3. Access Airflow Web Interface

Navigate to `http://localhost:8080` in your browser.
- **Username:** `airflow` (or defined in `AIRFLOW_WWW_USER_USERNAME`)
- **Password:** `airflow` (or defined in `AIRFLOW_WWW_USER_PASSWORD`)

Unpause the DAGs or trigger `produce_json` to run the end-to-end pipeline.

### 4. Run Test Suites

Execute test suites directly inside the running Airflow worker container:

```bash
docker exec -it airflow-worker pytest tests/ -v
```

### 5. Inspect Warehouse Tables

Connect to the PostgreSQL warehouse database:

```bash
docker exec -it postgres psql -U elt_user -d elt_db
```

Query staging and core tables:

```sql
SELECT * FROM staging.yt_api LIMIT 5;
SELECT "Video_ID", "Video_Title", "Duration", "Video_Type", "Video_Views", "updated_at" FROM core.yt_api LIMIT 5;
```

### 6. Tear Down Services

To stop containers while preserving database volume data:

```bash
docker compose down
```

To stop containers and reset all database volumes:

```bash
docker compose down -v
```

---

## Author & Maintainer

Developed by Ahmed Rizk Gawish
