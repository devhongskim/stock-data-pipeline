# 📐 Architecture Decision Log

This document traces the chronological evolution, technical trade-offs, and architectural pivots made throughout development of the Cloud-Native Stock Market ETL Pipeline — how the system matured from a basic local script into a production-grade, cloud-native data pipeline.

> For the granular bug list and discovery process behind these decisions, see [`ENGINEERING_LOG.md`](ENGINEERING_LOG.md).

## At a Glance

| Layer | Where it started | Where it landed |
|---|---|---|
| **Storage** | Local disk | AWS S3 + embedded DuckDB warehouses |
| **Orchestration** | Manual `python main.py` | Apache Airflow via Astro CLI |
| **Idempotency** | Unconditional overwrites | Per-layer existence checks + self-healing |
| **Observability** | `print()` statements | Structured logging + Slack alerting |

---

## 1. Storage & Persistence Layer

The storage tier evolved from local disk I/O to a cloud-native, serverless-friendly Medallion architecture.

| Phase | Design |
|---|---|
| **1 — Local File System** | Raw JSON extracts and processed Parquet files saved directly to disk. |
| **2 — Introduction of PostgreSQL** | Postgres added to house aggregate data in a queryable state, while raw files stayed local. |
| **3 — Cloud Migration to AWS S3** | File storage decoupled from local execution and migrated entirely to S3; Postgres continued managing structured/aggregate layers. |
| **4 — Retiring Postgres for Embedded DuckDB on S3** | Postgres fully deprecated in favor of embedded DuckDB warehouses backed by S3. |

**Rationale:** For a single-node, daily batch pipeline processing manageable volumes of financial data, maintaining an external Postgres container added unnecessary operational overhead and connection-management complexity. DuckDB provides high-performance columnar analytical execution locally, while S3 delivers durable, portable storage via file-based state synchronization.

---

## 2. Orchestration & Execution

The execution model progressed from manual invocation to scheduled cloud automation, and finally to robust containerized workflow management.

| Phase | Design |
|---|---|
| **1 — Manual Script Execution** | Run entirely via manual invocations of a standalone `main.py`. |
| **2 — GitHub Actions & Cron** | Automated remote execution via GitHub Actions + cron, with custom calendar logic validating market open/closed status before running. |
| **3 — Apache Airflow (via Astro CLI)** | Fully orchestrated Airflow environment (`stock_market_data_daily` DAG). |

**Rationale:** Airflow introduced native retry policies, visual dependency graphs, task-level isolation, and dynamic scheduling capabilities that cron-based GitHub Actions cannot support at scale.

---

## 3. Idempotency & Overwrite Controls

Data consistency and execution safety matured through iterative guardrails.

| Phase | Design |
|---|---|
| **1 — Unconditional Overwrites** | Existing data overwritten blindly on every execution — risked data corruption and redundant API calls. |
| **2 — Guardrail Checks** | `check_exists` existence checks added across every pipeline stage, complemented by an optional `force_overwrite` parameter for controlled backfills. |

**Result:** Re-running a completed date is now an instantaneous no-op, and partial runs safely self-heal on subsequent executions.

---

## 4. Error Handling & Observability

Observability transitioned from raw standard output to structured logging and proactive alerting.

| Phase | Design |
|---|---|
| **1 — Basic Standard Output** | Error visibility relied solely on `print()` statements — minimal context during failures. |
| **2 — Structured Logging** | Standardized on Python's `logging` module, categorizing and capturing distinct exception types across modules. |
| **3 — Explicit Exception Raising** | Refined error propagation with explicit control-flow exceptions (e.g. `AirflowSkipException` for market closures). |
| **4 — Proactive Slack Alerting** | Automated failure notifications via an Airflow `on_failure_callback` tied to a Slack Incoming Webhook. |

**Defensive design:** The alerting client wraps its Slack call in an isolated `try/except`, so a network disruption or misconfigured webhook can never crash the underlying pipeline task — a broken alert should never become a second failure.