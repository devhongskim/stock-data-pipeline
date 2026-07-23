# Cloud-Native Stock Market ETL Pipeline

A professional-grade data engineering pipeline designed to ingest, process, and analyze stock market data. This project implements a Medallion Architecture to move data from raw ingestion to analytics-ready datasets, utilizing a cloud-first approach with AWS S3, DuckDB local data warehouses, and automated workflows.

## 🚀 Project Overview
This pipeline demonstrates a production-ready approach to ETL/ELT, moving data from the Polygon.io API into a structured, reliable format[cite: 15]. 

* **Bronze**: Raw, immutable JSON responses from the API stored in AWS S3[cite: 15].
* **Silver**: Data cleaned, schema-validated, upserted into local DuckDB storage (`stock_raw.duckdb`), and mirrored to S3 as optimized Parquet files[cite: 15].
* **Gold**: Business-ready metrics, including daily returns and intraday volatility, stored in `stock_metrics.duckdb` and pushed to S3[cite: 15].

## 🛠 Tech Stack
| Category | Technology |
| :--- | :--- |
| **Language** | Python[cite: 15] |
| **Cloud Storage** | AWS S3[cite: 15] |
| **Processing & Warehousing** | DuckDB |
| **Data Format** | JSON, Parquet[cite: 15] |
| **Data Analysis** | Pandas[cite: 15] |
| **Workflow Automation** | GitHub Actions / Apache Airflow[cite: 15] |
| **Data Source** | Polygon.io API[cite: 15] |

## 🏗 Pipeline Architecture
Polygon.io API -> AWS S3 (Bronze / Raw JSON) -> DuckDB (`stock_raw.duckdb` & Silver Parquet) -> DuckDB (`stock_metrics.duckdb` & Gold Parquet) -> BI & Analytics

## ⚙️ Configuration
The project is configured for cloud-native execution. Ensure the following environment variables are set in your GitHub Secrets or local environment:

| Variable | Description |
| :--- | :--- |
| `AWS_ACCESS_KEY_ID` | AWS Credentials[cite: 15] |
| `AWS_SECRET_ACCESS_KEY` | AWS Credentials[cite: 15] |
| `AWS_BUCKET` | Shared AWS S3 Storage Bucket |
| `POLYGON_API_KEY` | Polygon.io API Key[cite: 15] |

## 📊 Local Data Warehousing with DuckDB
The pipeline utilizes lightweight embedded **DuckDB** warehouses for efficient columnar storage and fast relational upserts:
* **`stock_raw.duckdb`**: Stores cleaned raw stock quotes for the Silver layer.
* **`stock_metrics.duckdb`**: Houses calculated financial metrics (daily returns, intraday volatility) for the Gold layer.

## 🚀 Future Roadmap
- [ ] **Scalability**: Implement Apache Spark for distributed processing[cite: 15].
- [ ] **Data Quality**: Integrate Great Expectations for automated testing[cite: 15].
- [ ] **Infrastructure**: Use Terraform to manage cloud resources (IaC)[cite: 15].

## 📦 How to Run
1. **Clone the repository**:
   `git https://github.com/devhongskim/stock-data-pipeline.git`[cite: 15]
2. **Install dependencies**:
   `pip install -r requirements.txt`[cite: 15]
3. **Configure environment**: Create a `.env` file with your API keys and AWS credentials[cite: 15].
4. **Execute**:
   `python main.py`[cite: 15]

### Storage Strategy
The Bronze layer utilizes date-based partitioning to optimize data retrieval and organization.
<img width="2537" height="895" alt="Screenshot 2026-07-18 212009" src="https://github.com/user-attachments/assets/4abfa8ac-2f6f-4ff3-9c84-c668acd358f7" />[cite: 15]

### Gold Layer: Analytics-Ready Data
The pipeline calculates key financial metrics, including daily returns and intraday volatility, which are loaded into the DuckDB warehouse and S3 for reporting and analysis.
<img width="890" height="1077" alt="Screenshot 2026-07-18 212301" src="https://github.com/user-attachments/assets/d80a31a3-f07f-47ba-8601-5ac95c0333b1" />[cite: 15]