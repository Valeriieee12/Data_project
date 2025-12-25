"""
Конфигурация для подключения к PostgreSQL.
"""

POSTGRES_CONNECTION = {
    'host': 'postgres',
    'port': '5432',
    'database': 'delivery_db',
    'user': 'delivery_user',
    'password': 'delivery_password'
}

# Путь к данным
DATA_PATH = '/opt/airflow/data/deliveries.parquet'

# Схемы
STAGING_SCHEMA = 'staging'
MAIN_SCHEMA = 'public'
DATAMART_SCHEMA = 'datamart'
