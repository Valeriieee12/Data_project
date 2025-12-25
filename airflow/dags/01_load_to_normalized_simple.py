from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import pyarrow.parquet as pq
import logging
import os

default_args = {
    'owner': 'delivery_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_staging_schema():
    """Создание staging схемы для временных данных"""
    pg_hook = PostgresHook(postgres_conn_id='delivery_postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    conn.commit()
    
    cursor.close()
    conn.close()
    logging.info("Создана схема staging")

def test_load_small_data():
    """Тестовая загрузка небольшого объема данных"""
    
    logger = logging.getLogger(__name__)
    logger.info("Тестовая загрузка данных")
    
    file_path = "/opt/airflow/data/deliveries.parquet"
    
    try:
        pg_hook = PostgresHook(postgres_conn_id='delivery_postgres')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Создаем таблицу и коммитим
        cursor.execute("""
            DROP TABLE IF EXISTS staging.deliveries_test;
            CREATE TABLE staging.deliveries_test (
                order_id BIGINT,
                user_id BIGINT,
                user_phone VARCHAR(50),
                created_at TIMESTAMP
            );
        """)
        conn.commit()
        
        # Читаем только первые 1000 строк
        logger.info("Чтение данных...")
        
        # Используем PyArrow для чтения
        parquet_file = pq.ParquetFile(file_path)
        
        # Читаем первый батч
        batch = parquet_file.read_row_group(0)
        df = batch.to_pandas()
        
        # Берем только первые 100 строк
        df = df.head(100)
        
        # Вставляем данные
        logger.info(f"Вставляем {len(df)} строк...")
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO staging.deliveries_test (order_id, user_id, user_phone, created_at)
                VALUES (%s, %s, %s, %s)
            """, (row['order_id'], row['user_id'], row['user_phone'], row['created_at']))
        
        conn.commit()
        
        # Проверяем
        cursor.execute("SELECT COUNT(*) FROM staging.deliveries_test")
        count = cursor.fetchone()[0]
        logger.info(f"Вставлено {count} строк в staging.deliveries_test")
        
        # Тестируем вставку в нормализованные таблицы
        cursor.execute("""
            INSERT INTO normalized.users (user_id, user_phone)
            SELECT DISTINCT user_id, user_phone
            FROM staging.deliveries_test
            ON CONFLICT (user_id) DO NOTHING;
        """)
        
        cursor.execute("SELECT COUNT(*) FROM normalized.users")
        user_count = cursor.fetchone()[0]
        logger.info(f"В normalized.users теперь {user_count} пользователей")
        
        # Очищаем тестовую таблицу
        cursor.execute("DROP TABLE IF EXISTS staging.deliveries_test;")
        conn.commit()
        
        cursor.close()
        conn.close()
        
        logger.info("Тестовая загрузка успешно завершена")
        
    except Exception as e:
        logger.error(f"Ошибка при тестовой загрузке: {e}")
        raise

with DAG(
    'test_load_simple',
    default_args=default_args,
    description='Тестовая загрузка данных',
    schedule_interval=None,  # Одноразовый DAG
    catchup=False,
    tags=['delivery', 'test'],
) as dag:
    
    create_staging_task = PythonOperator(
        task_id='create_staging_schema',
        python_callable=create_staging_schema,
    )
    
    test_load_task = PythonOperator(
        task_id='test_load_small_data',
        python_callable=test_load_small_data,
    )
    
    create_staging_task >> test_load_task
