# DSS150P Laboratory 3 Starter Repository

This repository supports Module 2: Pipeline Construction, Storage, and Orchestration.
It is intentionally incomplete. Students must implement the marked TODOs and document their decisions.

## Main progression
- Goal 1: reproducible environment, modularization, Git, Docker, configuration
- Goal 2: raw -> staging -> curated transformations; audit/error handling; rerun-safe loading
- Goal 3: CSV/JSON/Parquet/PostgreSQL comparison; partitioning; selected-partition load
- Goal 4: Apache Airflow DAG for extract -> transform -> load -> validate

Start with `DSS150P_Laboratory_Activity_3.pdf`.

## Recommended commands
```bash
cp .env.example .env
python -m venv .venv
# activate .venv then:
pip install -r requirements.txt
python -m src.cli validate-env
```
The provided `.env.example` uses `POSTGRES_HOST=localhost` for host-side commands. Docker Compose overrides the application containers to use the service hostname `postgres`.

Docker/PostgreSQL:
```bash
docker compose up -d postgres
docker compose run --rm pipeline python -m src.cli validate-env
```

Airflow in Goal 4:
```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d airflow-webserver airflow-scheduler
```
Airflow UI: http://localhost:8080 (training credentials: admin/admin; change if reused outside the lab).

## Changelog & Progress Tracking

### Phase 0: Prerequisites and Initial Setup
- **Changes:** Initialized Git repository in the correct project root, created local `.env` file from template, updated local database credentials.
- **Security:** Verified `.env` is successfully ignored by `.gitignore` to prevent secret leakage.

### Phase 1: Goal 1.2 - Reproducible Environment
- **Changes:** Created `.venv`, installed dependencies, and started PostgreSQL via Docker.
- **External Configuration Explanation:** Configuration is separated from code. Secrets and local credentials are stored securely in `.env` (ignored by Git), while non-sensitive pipeline parameters are stored in `config/settings.yml`. This modularity allows the pipeline to adapt to different runtime contexts (local host vs. Docker container) without modifying the Python source code.

### Phase 2: Goal 2 - ETL/ELT Pipeline Development
- **Task A (Raw Extraction):** Implemented `extract_sources` to copy unmodified source files (`customers.csv`, `orders.csv`, `products.json`) into run-specific snapshots (`data/raw/run_id=...`).
- **Task B (Staging Transformations):** Cleaned and typed datasets using configurations from `settings.yml`. Deduplicated records keeping the latest `updated_at`, enforced UTC timestamps, normalized strings, and wrote Parquet outputs to `data/staging/`.
- **Task C (Curated Transformations):** Joined orders with customers and products. Calculated monetary measures (`gross_amount`, `discount_amount`, `net_amount`). Generated a deterministic `record_hash` using MD5 for future rerun-safe loading. Wrote Parquet outputs to `data/curated/`.
- **Task D (Error Handling & Quarantine):** Wrapped the CLI in a strict `try-except` block for pipeline failures. Enforced data-quality rules (e.g., rejecting invalid quantities, negative prices, and orphaned foreign keys), routing all invalid rows to `data/quarantine/` with a traceable reason.
- **Execution Strategy:** The pipeline is executed inside a Linux container via `docker compose` to ensure cross-platform reproducibility and deliberately bypass local Windows file-locking and silent Pandas/C-engine memory crashes.

### Phase 3: Goal 3 - Storage Systems and Benchmarking
- **Task A (Multi-Format Materialization):** Materialized the curated dataset into four representations: CSV, JSON Lines, compressed Parquet (`snappy`), and PostgreSQL.
- **Task B (Performance Benchmarking):** Measured write time, full read median (5 repetitions), and filtered read median (`status='DELIVERED'`) in compliance with the laboratory specifications.
- **Benchmark Summary Table:**
  | Format | Size (Bytes) | Write Time (s) | Full Read Median (s) | Filtered Read Median (s) |
  | :--- | :--- | :--- | :--- | :--- |
  | **CSV** | 13,630,073 | 1.9415 | 0.3016 | 0.2810 |
  | **JSON Lines** | 27,813,442 | 1.1007 | 0.6556 | 1.1403 |
  | **Parquet** | 3,692,812 | 0.2868 | 0.0726 | 0.1111 |
  | **PostgreSQL** | Server Table | N/A | 0.5055 | 0.0977 |
- **Key Findings:** Parquet achieved the highest compression ratio and fastest full-scan performance because of its columnar storage layout. PostgreSQL excelled at filtered retrieval via server-side indexing. CSV and JSON formats incurred higher text-parsing penalties and storage footprints.

# DSS150P Laboratory Activity #3: Productionizing a Modular Data Pipeline

**Course:** DSS150P - Fundamentals of Data Engineering  
**Student/Team Repository:** [memorandavid-dotcom/dss150p-lab03-starter-main](https://github.com/memorandavid-dotcom/dss150p-lab03-starter-main)

---

## Overview
This repository contains the complete production-ready implementation of a modular, rerun-safe, and orchestrated data pipeline for an e-commerce analytics platform. The pipeline ingests raw customer, product, and order records, performs technical cleaning and strict quality quarantining, executes cross-source business joins, materializes performance-benchmarked storage formats, supports partitioned data loading, persists records securely into PostgreSQL via idempotent UPSERTs, and automates end-to-end execution using Apache Airflow.

---

## Architecture & Module Structure

The codebase is strictly separated into single-responsibility modules following modern data engineering standards:

```text
project/
├── config/settings.yml       # Non-secret pipeline configurations
├── data/
│   ├── source/               # Original unedited source files (CSV, JSON)
│   ├── raw/                  # Run-specific reproducible raw snapshots
│   ├── staging/              # Type-casted, cleaned Parquet staging files
│   ├── curated/              # Business-joined, monetary-calculated Parquet files
│   ├── quarantine/           # Explicitly rejected invalid/orphan records
│   ├── benchmarks/           # Format size comparisons and timing reports
│   └── partitioned/          # Year/month partitioned Parquet storage
├── src/
│   ├── extract/              # Source acquisition and raw snapshotting
│   ├── transform/            # Staging cleanup and curated business join logic
│   ├── load/                 # PostgreSQL UPSERT persistence and partition loads
│   ├── validate/             # Schema contract and data quality assertions
│   ├── benchmark/            # Format materialization and speed runner
│   └── cli.py                # Thin command-line interface entrypoint
├── dags/dss150p_pipeline.py  # Production Apache Airflow orchestration DAG
├── sql/init/                 # Database bootstrap and warehouse schema SQL
└── docker-compose.yml        # Multi-container orchestration (Python + Postgres)