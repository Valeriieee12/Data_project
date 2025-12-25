"""
Полная загрузка данных из Parquet в staging.deliveries_raw
"""
import os
import io
from datetime import datetime, timedelta
import logging

import pandas as pd
import pyarrow.parquet as pq
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner': 'delivery_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def ensure_staging_table():
    """Создание staging таблицы если не существует"""
    logger = logging.getLogger(__name__)
    hook = PostgresHook(postgres_conn_id='delivery_postgres')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    # Создаем схему staging если не существует
    cursor.execute("CREATE SCHEMA IF NOT EXISTS staging")
    
    # Создаем таблицу для сырых данных
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS staging.deliveries_raw (
        order_id BIGINT,
        user_id BIGINT,
        user_phone VARCHAR(50),
        address_text TEXT,
        created_at TIMESTAMP,
        paid_at TIMESTAMP,
        delivery_started_at TIMESTAMP,
        delivered_at TIMESTAMP,
        canceled_at TIMESTAMP,
        payment_type VARCHAR(50),
        item_id BIGINT,
        item_title VARCHAR(255),
        item_category VARCHAR(100),
        item_quantity INTEGER,
        item_price DECIMAL(10, 2),
        item_canceled_quantity INTEGER,
        item_replaced_id BIGINT,
        order_discount DECIMAL(5, 2),
        item_discount DECIMAL(5, 2),
        order_cancellation_reason VARCHAR(255),
        driver_id BIGINT,
        driver_phone VARCHAR(50),
        delivery_cost DECIMAL(10, 2),
        store_id BIGINT,
        store_address TEXT
    )
    """
    cursor.execute(create_table_sql)
    
    # Создаем индексы если не существуют
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_staging_order_id ON staging.deliveries_raw(order_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_staging_user_id ON staging.deliveries_raw(user_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_staging_item_id ON staging.deliveries_raw(item_id)")
    
    conn.commit()
    cursor.close()
    conn.close()
    
    logger.info("Staging таблица создана/проверена")

def load_parquet_to_staging():
    """Загрузка данных из Parquet в staging таблицу"""
    logger = logging.getLogger(__name__)
    hook = PostgresHook(postgres_conn_id='delivery_postgres')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    # Путь к файлу данных
    file_path = '/opt/airflow/data/deliveries.parquet'
    
    # Очищаем таблицу перед загрузкой
    cursor.execute("TRUNCATE TABLE staging.deliveries_raw")
    conn.commit()
    
    logger.info(f"Начало загрузки данных из {file_path}")
    
    # Читаем Parquet файл
    try:
        # Читаем файл целиком (для 72MB это должно быть ок)
        df = pd.read_parquet(file_path)
        
        logger.info(f"Прочитано {len(df):,} строк из Parquet файла")
        
        # Загружаем данные в PostgreSQL
        # Используем copy_from для эффективной загрузки
        output = io.StringIO()
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        
        cursor.copy_from(output, 'staging.deliveries_raw', null='', sep='\t')
        conn.commit()
        
        logger.info(f"Успешно загружено {len(df):,} строк в staging.deliveries_raw")
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных: {e}")
        conn.rollback()
        raise
    
    finally:
        cursor.close()
        conn.close()

with DAG(
    'load_full_data_to_staging',
    default_args=default_args,
    description='Полная загрузка данных из Parquet в staging таблицу',
    schedule_interval='@daily',
    catchup=False,
    tags=['delivery', 'etl', 'staging'],
) as dag:
    
    start = DummyOperator(task_id='start')
    
    ensure_staging_task = PythonOperator(
        task_id='ensure_staging_table',
        python_callable=ensure_staging_table,
    )
    
    load_data_task = PythonOperator(
        task_id='load_parquet_to_staging',
        python_callable=load_parquet_to_staging,
    )
    
    end = DummyOperator(task_id='end')
    
    start >> ensure_staging_task >> load_data_task >> end
