# Rio E-Commerce DLT Framework: Azure Databricks

**Medallion Architecture | Azure Databricks | Unity Catalog | Delta Live Tables (DLT) | Serverless Compute | Databricks Asset Bundles (DABs)**

This repository implements a production-grade, configuration-driven data engineering framework utilizing the Medallion Architecture. It automates the `incremental and idempotent` end-to-end transformation of raw e-commerce operational data into analytical presentation layers (One Big Table/Star Schema) using Databricks Delta Live Tables (DLT) and Unity Catalog.
## 🛠️ Tech Stack

* **Platform:** Azure Databricks (Unity Catalog, Serverless Compute).
* **Storage:** Azure Data Lake Storage (ADLS) Gen2 (Raw Landing Zone via UC Volumes).
* **Compute/Engine:** Delta Live Tables (DLT), PySpark, Spark Structured Streaming.
* **Ingestion:** Databricks Auto Loader (`cloudFiles`).
* **CI/CD & Deployment:** Databricks Asset Bundles (DABs) with dynamic Dev/Prod parameterization.
* **Orchestration:** Databricks Lakeflow / Workflows (Executed via Service Principal).
* **State Management:** RocksDB (SCD Type 1 & 2 via `dlt.apply_changes`).

---
## 🏗️ Data Architecture (Medallion DLT)

The pipeline executes a deterministic, multi-layer transformation DAG ensuring structural integrity, financial accuracy, and incremental processing.

### Bronze Layer (Raw Ingestion)
* **Pattern:** Append-only streaming.
* **Mechanism:** Auto Loader (`cloudFiles`) dynamically ingests raw CSV payloads from the `ADLS Gen2 Landing Zone`.
* **Resilience:** Schema evolution set to `rescue` to trap malformed records and guarantee zero data loss.
* **Lineage:** Injects hardware-level file modification timestamps and SHA-256 row hashes for downstream deterministic sequencing.

### Silver Layer (Cleansed & Conformed)
* **Pattern:** Streaming updates and `Upserts` (SCD1 / SCD2).
* **Quality Gates:** Native DLT expectations (`@dlt.expect_or_drop`, `@dlt.expect`) enforce strict schema validation, geographical formatting, and financial boundaries.
* **State Tracking:**
  * **SCD Type 1:** Applied to `Orders`, `Items`, `Payments`, `Products`, and `Sellers` to update mutable attributes in-place.
  * **SCD Type 2:** Applied to the `Customers` domain to maintain historical geographic changes over time.
* **Optimization:** Change Data Feed (CDF) enabled to pass only net-new mutations to the Gold layer.

### Gold Layer (Analytical & Presentation)
* **Pattern:** Materialized Views and Incremental Facts.
* **Domain Models:**
  * `gold_dim_products`: Materialized dimension with late-bound translation logic.
  * `gold_fact_order_line_sales`: Granular line-item fact for `Gross Merchandise Value`(GMV)/`Average Order Value`(AOV) reporting.
  * `gold_fact_finops_reconciliation`: Order-grain, ledger balancing for `automated revenue anomaly detection`.
  * `gold_sales_performance_obt`: Fully denormalized `One Big Table` (OBT) optimized for sub-second BI rendering.
* **Optimization:** Liquid Clustering and Z-Ordering applied to critical access paths (e.g., temporal and categorical axes).

---
## 📂 Project Structure

The repository is modularized to support domain-driven schema definitions, decoupled DLT pipelines, and automated DABs deployments.

```text
.
├── assets/                       # Architectural diagrams and infrastructure visuals
├── orchestration_img/            # Execution proofs (Bootstrap, Job DAGs, SPN runs)
├── resources/
│   ├── jobs/                     # Master orchestration definitions (jobs.yml, orchestration_job.yml)
│   └── pipelines/                # DLT pipeline configurations (pipelines.yml)
├── src/
│   ├── bootstrap/                # UC setup and infra initialization scripts (SQL/Py)
│   ├── config/                   # Centralized ingestion control dictionaries (bronze_config.py)
│   ├── domains/                  # Domain-specific schema definitions (Customers, Orders, etc.)
│   ├── pipelines/                # Medallion DLT execution logic
│   │   ├── bronze/               # 01_bronze.py (Auto Loader factory)
│   │   ├── silver/               # Cleansing and SCD apply_changes logic
│   │   └── gold/                 # Materialized views, Facts, and OBTs
│   └── shared/                   # Cross-layer utilities (audit, spark_io, transformation)
├── databricks.yml                # DABs deployment configuration and environment targets
└── README.md
```

---
## 🛡️ Security & Governance
This project adheres to strict production security standards via Unity Catalog:

* **Service Principal (SP)**: Production jobs and DLT pipelines are run via a dedicated Service Principal to ensure strict environment and execution isolation.

* **Least Privilege**: The SP is granted specific READ FILES and MANAGE permissions strictly bound to required External Locations and execution catalogs.

* **External Locations**: Raw file access (ADLS Gen2) is exclusively managed through Unity Catalog External Locations and Storage Credentials.

* **Auditability**: Infrastructure setup and Unity Catalog initialization (src/bootstrap) are performed by a workspace admin, while daily operational pipeline executions are strictly restricted to the Service Principal.

---
## 📊 Visuals & Reporting (assets)
* **Lineage**: End-to-end data lineage is tracked and visualizable within the Databricks Catalog Explorer.

* **Power BI**: Visual reports are generated directly from the Gold layer (e.g., gold_sales_performance_obt) to provide executive insights into GMV, logistics performance, and automated revenue reconciliation.
