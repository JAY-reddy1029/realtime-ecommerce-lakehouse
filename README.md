# Real-Time E-Commerce Analytics Lakehouse

An end-to-end production-grade data engineering project built with
industry-standard tools used at companies like Amazon, Flipkart, and Swiggy.

---

## Architecture
Event Sources (Python Producers)
↓
Apache Kafka (3 topics: orders, sessions, inventory)
↓
Spark Structured Streaming → Delta Lake Bronze (AWS S3)
↓
Apache Airflow DAGs → Spark Batch → Delta Lake Silver (AWS S3)
↓
dbt Core → Delta Lake Gold → AWS Redshift Serverless
↓
Great Expectations + Streamlit Dashboard


---

## Tech Stack

| Layer | Technology |
|---|---|
| Streaming Ingestion | Apache Kafka |
| Stream Processing | Apache Spark Structured Streaming |
| Batch Orchestration | Apache Airflow |
| Storage | AWS S3 + Delta Lake |
| Transformation | dbt Core |
| Data Warehouse | AWS Redshift Serverless |
| Data Quality | Great Expectations |
| Dashboard | Streamlit |
| Infrastructure | Terraform |
| CI/CD | GitHub Actions |
| Language | Python |

---

## Project Structure
realtime-ecommerce-lakehouse/
├── ingestion/
│ ├── kafka_producers/ # Python scripts that generate fake events
│ └── kafka_config/ # Kafka topic configuration
├── processing/
│ ├── spark_streaming/ # Spark Structured Streaming jobs
│ └── spark_batch/ # Spark batch jobs (Bronze → Silver)
├── orchestration/
│ └── dags/ # Apache Airflow DAGs
├── transformation/
│ └── dbt_project/ # dbt models (staging, intermediate, marts)
├── serving/
│ └── streamlit_app/ # Streamlit dashboard
├── quality/
│ └── expectations/ # Great Expectations suites
├── infrastructure/
│ └── terraform/ # AWS infrastructure as code
├── .github/
│ └── workflows/ # GitHub Actions CI/CD pipelines
└── docs/ # Architecture diagrams and documentation


---

## Data Domain

Three types of e-commerce events flow through this pipeline:

| Event | Fields |
|---|---|
| Orders | order_id, user_id, product_id, amount, status, timestamp |
| User Sessions | session_id, user_id, page_viewed, duration, device, timestamp |
| Inventory | product_id, warehouse_id, quantity, action, timestamp |

---

## Project Status

| Phase | Description | Status |
|---|---|---|
| Phase 1 | Kafka setup + Python producers | 🔄 In Progress |
| Phase 2 | Spark Streaming → Bronze layer | ⏳ Pending |
| Phase 3 | Airflow + Silver layer | ⏳ Pending |
| Phase 4 | dbt + Gold layer + Redshift | ⏳ Pending |
| Phase 5 | Data Quality + Streamlit | ⏳ Pending |
| Phase 6 | Terraform IaC + CI/CD | ⏳ Pending |

---

## Getting Started

> Detailed setup instructions will be added as each phase is completed.

Requirements:
- Docker Desktop
- Python 3.10+
- AWS Account
- Git

---

## Author

**Jayachandra Reddy**
- GitHub: [@JAY-reddy1029](https://github.com/JAY-reddy1029)
- LinkedIn: [jayreddy-datascientist](https://linkedin.com/in/jayreddy-datascientist)

---

## License

MIT License — see [LICENSE](LICENSE) for details.