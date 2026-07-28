📐 Architecture Decision Log & Evolution
This document outlines the chronological evolution, technical trade-offs, and architectural pivots made throughout the development of the Cloud-Native Stock Market ETL Pipeline. It highlights how the system matured from a basic local script to a production-grade, cloud-native data pipeline.

1. Storage & Persistence Layer
The storage tier evolved from local disk I/O to a cloud-native, serverless-friendly Medallion architecture.

Phase 1 — Local File System: The pipeline initially relied entirely on local disk storage, saving raw JSON extracts and processed Parquet files directly to the local machine.

Phase 2 — Introduction of PostgreSQL: To transition analytical data into a queryable state, PostgreSQL was introduced to house aggregate data, while raw files remained on local storage.

Phase 3 — Cloud Migration to AWS S3: To decouple storage from local execution environments, file storage was migrated entirely to AWS S3, while PostgreSQL continued to manage structured/aggregate layers.

Phase 4 — Retiring Postgres for Embedded DuckDB on S3: PostgreSQL was fully deprecated in favor of embedded DuckDB warehouses backed by AWS S3.

Rationale: For a single-node, daily batch pipeline processing manageable volumes of financial data, maintaining an external Postgres container added unnecessary operational overhead and connection management complexity. DuckDB provides high-performance columnar analytical execution locally, while leveraging S3 for durable, portable storage via file-based state synchronization.

2. Orchestration & Execution
The execution model progressed from manual invocation to scheduled cloud automation and, finally, robust containerized workflow management.

Phase 1 — Manual Script Execution: The pipeline was executed entirely via manual runs of a standalone main.py script.

Phase 2 — GitHub Actions & Cron: Transitioned to automated remote execution using GitHub Actions paired with a cron scheduler, incorporating custom calendar logic to validate market open/closed status before execution.

Phase 3 — Apache Airflow (via Astro CLI): Upgraded to a fully orchestrated Airflow environment using the Astro CLI (stock_market_data_daily DAG).

Rationale: Airflow introduced native retry policies, visual dependency graphs, task-level isolation, and dynamic scheduling capabilities that cron-based GitHub Actions cannot support at scale.

3. Idempotency & Overwrite Controls
Data consistency and execution safety matured through iterative guardrails.

Phase 1 — Unconditional Overwrites: Early iterations overwrote existing data blindly on every execution, risking data corruption or redundant API calls.

Phase 2 — Guardrail Checks: Implemented existence checks (check_exists) across pipeline stages to prevent redundant writes, complemented by an optional force_overwrite parameter for controlled backfills.

Result: Re-running a completed date becomes an instantaneous no-op, and partial runs safely self-heal on subsequent executions.

4. Error Handling & Observability
Observability transitioned from raw standard output to structured logging and proactive alerting.

Phase 1 — Basic Standard Output: Initial error visibility relied solely on raw print() statements, providing minimal context during failures.

Phase 2 — Structured Logging: Standardized error tracking using Python's built-in logging module, categorizing and capturing distinct exception types across modules.

Phase 3 — Explicit Exception Raising: Refined error propagation by explicitly raising control-flow and failure exceptions (such as AirflowSkipException for market closures).

Phase 4 — Proactive Slack Alerting: Integrated an automated failure notification system via an Airflow on_failure_callback tied to a Slack Incoming Webhook.

Defensive Design: The alerting client incorporates graceful degradation and isolated try/except blocks to ensure a network disruption or misconfigured webhook can never crash the underlying pipeline task.