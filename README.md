# Data Engineering Project: Delivery Data Pipeline


## Services
- **Airflow UI**: http://localhost:8080 (admin/admin)
- **PgAdmin**: http://localhost:5051 (admin@delivery.com/admin)
- **PostgreSQL**: localhost:5433

## Project Structure
- \`docker-compose.yml\` - Docker configuration
- \`postgres/init.sql\` - DDL scripts for database
- \`airflow/dags/\` - Airflow DAGs for ETL pipeline
- \`airflow/scripts/\` - Python scripts for data processing
- \`airflow/data/\` - Source data files
- \`documentation.md\` - Project documentation

## ETL Pipeline
1. Data loading from Parquet files
2. Normalization to 3NF
3. Building analytical datamarts with PySpark
4. Data quality testing

## DAGs
- \`00_full_pipeline\` - Master pipeline
- \`01_check_connections\` - Connection validation
- \`02_load_full_data\` - Data loading
- \`03_transform_to_normalized\` - Data normalization
- \`04_build_datamarts\` - Datamart building with PySpark
- \`05_test_datamarts\` - Data testing and validation

## Database Schema
### Normalized Tables (3NF)
- \`users\` - Customer information
- \`stores\` - Store details
- \`items\` - Product catalog
- \`drivers\` - Delivery drivers
- \`orders\` - Order headers
- \`order_items\` - Order line items
- \`delivery_history\` - Delivery tracking

### Analytical Datamarts
- \`datamart.orders_daily\` - Daily order metrics
- \`datamart.product_sales_daily\` - Daily product sales metrics

## Technologies Used
- **Apache Airflow** - Workflow orchestration
- **PostgreSQL** - Relational database
- **PySpark** - Data processing
- **Docker** - Containerization
- **PgAdmin** - Database administration

## Requirements
- Docker 20.10+
- Docker Compose 2.0+
- 8GB RAM minimum
- 10GB free disk space
