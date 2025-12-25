"""
Тестовая загрузка небольшого количества данных
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

def test_load_small_batch():
    """Тестовая загрузка небольшого количества данных"""
    logger = logging.getLogger(__name__)
    hook = PostgresHook(postgres_conn_id='delivery_postgres')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    # Путь к файлу данных
    file_path = '/opt/airflow/data/deliveries.parquet'
    
    logger.info(f"Чтение небольшого количества данных из {file_path}")
    
    try:
        # Читаем только первые 1000 строк
        df = pd.read_parquet(file_path).head(1000)
        
        logger.info(f"Прочитано {len(df):,} строк из Parquet файла")
        
        # Проверяем, что таблица существует
        cursor.execute("""
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
        """)
        
        # Очищаем таблицу перед загрузкой
        cursor.execute("TRUNCATE TABLE staging.deliveries_raw")
        conn.commit()
        
        # Загружаем данные построчно
        for idx, row in df.iterrows():
            cursor.execute("""
                INSERT INTO staging.deliveries_raw VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
            """, (
                row['order_id'], row['user_id'], row['user_phone'], 
                row['address_text'], row['created_at'], row['paid_at'],
                row['delivery_started_at'], row['delivered_at'], 
                row['canceled_at'], row['payment_type'], row['item_id'],
                row['item_title'], row['item_category'], row['item_quantity'],
                float(row['item_price']) if pd.notna(row['item_price']) else None,
                int(row['item_canceled_quantity']) if pd.notna(row['item_canceled_quantity']) else None,
                row['item_replaced_id'] if pd.notna(row['item_replaced_id']) else None,
                float(row['order_discount']) if pd.notna(row['order_discount']) else None,
                float(row['item_discount']) if pd.notna(row['item_discount']) else None,
                row['order_cancellation_reason'], row['driver_id'],
                row['driver_phone'], 
                float(row['delivery_cost']) if pd.notna(row['delivery_cost']) else None,
                row['store_id'], row['store_address']
            ))
            
            if idx % 100 == 0:
                conn.commit()
                logger.info(f"Загружено {idx+1} строк...")
        
        conn.commit()
        
        # Проверяем загрузку
        cursor.execute("SELECT COUNT(*) FROM staging.deliveries_raw")
        count = cursor.fetchone()[0]
        
        logger.info(f"Успешно загружено {count} строк в staging.deliveries_raw")
        
        # Показываем пример данных
        cursor.execute("SELECT order_id, user_id, created_at FROM staging.deliveries_raw LIMIT 5")
        samples = cursor.fetchall()
        logger.info(f"Пример данных: {samples}")
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных: {e}")
        conn.rollback()
        raise
    
    finally:
        cursor.close()
        conn.close()

with DAG(
    'test_load_small_batch',
    default_args=default_args,
    description='Тестовая загрузка небольшого количества данных',
    schedule_interval=None,  # Одноразовый DAG
    catchup=False,
    tags=['delivery', 'test', 'small_batch'],
) as dag:
    
    start = DummyOperator(task_id='start')
    
    load_task = PythonOperator(
        task_id='test_load_small_batch',
        python_callable=test_load_small_batch,
    )
    
    end = DummyOperator(task_id='end')
    
    start >> load_task >> end
