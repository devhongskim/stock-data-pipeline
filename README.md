# Cloud-Native Stock Market ETL Pipeline

A professional-grade data engineering pipeline designed to ingest, process, and analyze stock market data. This project implements a Medallion Architecture to move data from raw ingestion to analytics-ready datasets, utilizing a cloud-first approach with AWS S3 and automated CI/CD workflows.

## 🚀 Project Overview
This pipeline demonstrates a production-ready approach to ETL/ELT, moving data from the Polygon.io API into a structured, reliable format. 

* **Bronze**: Raw, immutable JSON responses from the API stored in AWS S3.
* **Silver**: Data cleaned, schema-validated, and converted to optimized Parquet files via DuckDB.
* **Gold**: Business-ready metrics, including daily returns and intraday volatility.

## 🛠 Tech Stack
| Category | Technology |
| :--- | :--- |
| **Language** | Python |
| **Cloud Storage** | AWS S3 |
| **Processing Engine** | DuckDB |
| **Data Format** | JSON, Parquet |
| **Data Analysis** | Pandas |
| **Workflow Automation** | GitHub Actions |
| **Data Source** | Polygon.io API |

## 🏗 Pipeline Architecture
Polygon.io API -> AWS S3 (Bronze/Raw JSON) -> DuckDB (Silver/Cleaned Parquet) -> Gold Layer (Analytics Metrics) -> SQL Analysis / BI Tools

## ⚙️ Configuration
The project is configured for cloud-native execution. Ensure the following environment variables are set in your GitHub Secrets or local environment:

| Variable | Description |
| :--- | :--- |
| `AWS_ACCESS_KEY_ID` | AWS Credentials |
| `AWS_SECRET_ACCESS_KEY` | AWS Credentials |
| `AWS_BRONZE_BUCKET` | Destination S3 Bucket |
| `POLYGON_API_KEY` | Polygon.io API Key |

## 📊 Data Warehouse Integration
The pipeline is designed to optionally load the Gold layer into a PostgreSQL instance for SQL-based reporting. 

**Note on CI/CD:** PostgreSQL integration is enabled for **local development only**. The GitHub Actions workflow skips the database load step because the GitHub-hosted runners are ephemeral and cannot securely access private, local network instances. In a production environment, this would be solved by deploying the database to a service like Amazon RDS.

## 🚀 Future Roadmap
- [ ] **Scalability**: Implement Apache Spark for distributed processing.
- [ ] **Orchestration**: Transition from GitHub Actions to Apache Airflow.
- [ ] **Data Quality**: Integrate Great Expectations for automated testing.
- [ ] **Infrastructure**: Use Terraform to manage cloud resources (IaC).
- [ ] **Production DB**: Deploy PostgreSQL to Amazon RDS for full cloud-to-database automation.

## 📦 How to Run
1. **Clone the repository**:
   `git clone https://github.com/devhongskim/stock-data-pipeline.git`
2. **Install dependencies**:
   `pip install -r requirements.txt`
3. **Configure environment**: Create a `.env` file with your API keys and AWS credentials.
4. **Execute**:
   `python main.py`

### Storage Strategy
The Bronze layer utilizes date-based partitioning to optimize data retrieval and organization.
<img width="2537" height="895" alt="Screenshot 2026-07-18 212009" src="https://github.com/user-attachments/assets/4abfa8ac-2f6f-4ff3-9c84-c668acd358f7" />

### Gold Layer: Analytics-Ready Data
The pipeline calculates key financial metrics, including daily returns and intraday volatility, which are loaded into a PostgreSQL database for reporting and analysis.
<img width="890" height="1077" alt="Screenshot 2026-07-18 212301" src="https://github.com/user-attachments/assets/d80a31a3-f07f-47ba-8601-5ac95c0333b1" />

