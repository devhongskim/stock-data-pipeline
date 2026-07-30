# 📐 Architecture Evolution & Design Decisions

This document traces the chronological evolution, technical trade-offs, and architectural pivots made throughout development of the Cloud-Native Stock Market ETL Pipeline — how the system matured from a basic local script into a production-grade, cloud-native data pipeline.

## At a Glance

| Layer | Where it started | Where it landed |
|---|---|---|
| **Storage** | Local disk | AWS S3 + embedded DuckDB databases |
| **Orchestration** | Manual `python main.py` | Apache Airflow via Astro CLI |
| **Idempotency** | Unconditional overwrites | Per-layer existence checks + self-healing |
| **Observability** | `print()` statements | Structured logging + Slack alerting |
| **Infrastructure** | Manual provisioning (console / `boto3`) | Terraform-managed, imported from existing state |

---

## 1. Storage & Persistence Layer

The storage tier evolved from local disk I/O to a cloud-native, serverless-friendly Medallion architecture, with each iteration reducing operational overhead while improving durability, portability, and analytical capability.

| Phase | Design |
|---|---|
| **1 — Local File System** | Raw JSON extracts and processed Parquet datasets stored directly on the local filesystem. |
| **2 — PostgreSQL Integration** | PostgreSQL introduced to maintain a persistent, queryable analytical store, while raw files remained on local disk. |
| **3 — Cloud Migration to AWS S3** | Bronze, Silver, and Gold datasets migrated to AWS S3 using date-partitioned storage, decoupling persistent data from the execution environment. PostgreSQL continued serving as the analytical engine. |
| **4 — Embedded DuckDB + S3** | PostgreSQL replaced with embedded DuckDB databases (`stock_raw.duckdb` and `stock_metrics.duckdb`). DuckDB performs relational processing and incremental upserts, while partitioned Parquet datasets and DuckDB database files are persisted to S3 as durable cloud storage. |

**Rationale:** The original PostgreSQL design introduced operational overhead that wasn't justified for a single-node, daily batch workload. Migrating to DuckDB eliminated the need to manage a separate database server while retaining SQL-based transformations and efficient analytical queries. AWS S3 remains the system's durable storage layer, with DuckDB serving as a lightweight embedded analytical engine that synchronizes state between pipeline runs.

---

## 2. Orchestration & Execution

The execution model progressed from manual invocation to scheduled cloud automation, and finally to robust containerized workflow management.

| Phase | Design |
|---|---|
| **1 — Manual Script Execution** | Run entirely via manual invocations of a standalone `main.py`. |
| **2 — GitHub Actions & Cron** | Automated remote execution via GitHub Actions + cron, with custom calendar logic validating market open/closed status before running. |
| **3 — Apache Airflow (via Astro CLI)** | Fully orchestrated Airflow environment (`stock_market_data_daily` DAG). |

**Rationale:** Airflow introduced task-level retries, dependency management, execution isolation, DAG visualization, and scheduling capabilities that cron-based GitHub Actions cannot provide.

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

---

## 5. Infrastructure as Code

Infrastructure provisioning progressed from manual, undocumented setup to a declarative, version-controlled configuration.

| Phase | Design |
|---|---|
| **1 — Manual Provisioning** | The S3 bucket was created directly through the AWS Console / `boto3`, with no record of its configuration outside of the AWS account itself. |
| **2 — Terraform Adoption via Import** | The existing bucket was brought under Terraform management using `terraform import`, rather than being recreated — adopting real, already-running infrastructure into code without downtime or data loss. |

**Rationale:** Since the bucket already existed and held real pipeline data, a greenfield `terraform apply` was never an option — creating a same-named bucket from scratch would have failed outright, since S3 bucket names are globally unique. `terraform import` bridges this gap: it links an existing AWS resource to a new Terraform-managed resource block, after which `terraform plan`/`apply` behave exactly as if Terraform had provisioned it from day one.

**A concrete outcome of this migration:** importing the bucket surfaced a latent bug — the pipeline's AWS region had been hardcoded as `us-east-1` in the application code, while the bucket itself was actually provisioned in `us-east-2`. This had gone unnoticed because S3's client-side region handling is comparatively lenient for basic object operations, whereas Terraform's AWS provider is stricter and surfaced the mismatch immediately. Fixed by aligning `S3_REGION` across the codebase and `providers.tf` to the bucket's real region.

**Least-privilege IAM, discovered iteratively:** The pipeline's IAM user was originally scoped to only what the application code needed (`GetObject`, `PutObject`, `ListBucket`). Bringing Terraform into the picture required additional permissions the application never needed directly — reading bucket policy, tags, and public-access-block configuration, and writing tags. Rather than granting a broad managed policy (e.g. `AmazonS3FullAccess`) to sidestep this, each additional permission was added one at a time as Terraform's `import`/`plan`/`apply` surfaced exactly which `AccessDenied` error it hit next — resulting in a scoped, bucket-specific policy containing only what Terraform genuinely requires.

**Result:** `terraform plan` now serves as a standing drift-detection tool — a way to verify at any time that the bucket's real-world configuration (tags, public access block) still matches what's declared in code, independent of whether changes were made through Terraform, the AWS Console, or another tool.