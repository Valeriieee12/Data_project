# Data Engineering Project: Delivery Data Pipeline

## Quick Start
```bash
docker-compose up --build
Services
Airflow UI: http://localhost:8080 (admin/admin)

PgAdmin: http://localhost:5051 (admin@delivery.com/admin)

PostgreSQL: localhost:5433

Project Structure
docker-compose.yml - Docker configuration

postgres/init.sql - DDL scripts for database

airflow/dags/ - Airflow DAGs for ETL pipeline

airflow/scripts/ - Python scripts for data processing

airflow/data/ - Source data files

documentation.md - Project documentation

ETL Pipeline
Data loading from Parquet files

Normalization to 3NF

Building analytical datamarts with PySpark

Data quality testing

DAGs
00_full_pipeline - Master pipeline

01_check_connections - Connection validation

02_load_full_data - Data loading

03_transform_to_normalized - Data normalization

04_build_datamarts - Datamart building

05_test_datamarts - Data testing
