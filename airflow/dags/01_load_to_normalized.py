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

def extract_city_from_address(address):
    """Извлечение города из адреса"""
    if not address or not isinstance(address, str):
        return None
    # Берем первую часть до запятой
    parts = address.split(',')
    return parts[0].strip() if len(parts) > 0 else None

def load_data_to_normalized():
    """Загрузка данных из Parquet в нормализованные таблицы"""
    
    logger = logging.getLogger(__name__)
    logger.info("Начало загрузки данных в нормализованные таблицы")
    
    # Путь к файлу
    file_path = "/opt/airflow/data/deliveries.parquet"
    
    if not os.path.exists(file_path):
        logger.error(f"Файл не найден: {file_path}")
        raise FileNotFoundError(f"Файл не найден: {file_path}")
    
    try:
        # Подключаемся к PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='delivery_postgres')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # 1. Создаем staging таблицу
        cursor.execute("""
            DROP TABLE IF EXISTS staging.deliveries_raw;
            CREATE TABLE staging.deliveries_raw (
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
                item_title TEXT,
                item_category VARCHAR(255),
                item_quantity INTEGER,
                item_price DECIMAL(10,2),
                item_canceled_quantity INTEGER,
                item_replaced_id BIGINT,
                order_discount DECIMAL(5,2),
                item_discount DECIMAL(5,2),
                order_cancellation_reason TEXT,
                driver_id BIGINT,
                driver_phone VARCHAR(50),
                delivery_cost DECIMAL(10,2),
                store_id BIGINT,
                store_address TEXT
            );
        """)
        
        # 2. Читаем данные из Parquet файла с использованием PyArrow
        logger.info("Чтение данных из Parquet файла...")
        
        # Используем PyArrow для чтения файла по частям
        parquet_file = pq.ParquetFile(file_path)
        
        # Читаем данные батчами
        batch_size = 10000
        total_rows = 0
        
        for batch in parquet_file.iter_batches(batch_size=batch_size):
            df = batch.to_pandas()
            total_rows += len(df)
            
            # Используем COPY для быстрой загрузки
            from io import StringIO
            output = StringIO()
            df.to_csv(output, sep='\t', header=False, index=False, na_rep='')
            output.seek(0)
            
            cursor.copy_from(output, 'staging.deliveries_raw', null='')
            conn.commit()
            
            logger.info(f"Загружено {total_rows} строк...")
            
            # Для теста ограничимся 50000 строками
            if total_rows >= 50000:
                logger.info(f"Достигнут лимит в 50000 строк для теста")
                break
        
        logger.info(f"Всего загружено {total_rows} строк в staging таблицу")
        
        # 3. Загружаем данные в нормализованные таблицы
        logger.info("Загрузка данных в нормализованные таблицы...")
        
        # 3.1. Пользователи
        cursor.execute("""
            INSERT INTO normalized.users (user_id, user_phone)
            SELECT DISTINCT user_id, user_phone
            FROM staging.deliveries_raw
            ON CONFLICT (user_id) DO NOTHING;
        """)
        logger.info("Загружены пользователи")
        
        # 3.2. Магазины
        cursor.execute("""
            INSERT INTO normalized.stores (store_id, store_address)
            SELECT DISTINCT store_id, store_address
            FROM staging.deliveries_raw
            ON CONFLICT (store_id) DO NOTHING;
        """)
        logger.info("Загружены магазины")
        
        # 3.3. Товары
        cursor.execute("""
            INSERT INTO normalized.items (item_id, item_title, item_category)
            SELECT DISTINCT item_id, item_title, item_category
            FROM staging.deliveries_raw
            ON CONFLICT (item_id) DO NOTHING;
        """)
        logger.info("Загружены товары")
        
        # 3.4. Курьеры
        cursor.execute("""
            INSERT INTO normalized.drivers (driver_id, driver_phone)
            SELECT DISTINCT driver_id, driver_phone
            FROM staging.deliveries_raw
            WHERE driver_id IS NOT NULL
            ON CONFLICT (driver_id) DO NOTHING;
        """)
        logger.info("Загружены курьеры")
        
        # 3.5. Заказы
        cursor.execute("""
            INSERT INTO normalized.orders (
                order_id, user_id, store_id, address_text,
                created_at, paid_at, delivery_started_at,
                delivered_at, canceled_at, payment_type,
                order_discount, order_cancellation_reason, delivery_cost
            )
            SELECT DISTINCT 
                order_id, user_id, store_id, address_text,
                created_at, paid_at, delivery_started_at,
                delivered_at, canceled_at, payment_type,
                order_discount, order_cancellation_reason, delivery_cost
            FROM staging.deliveries_raw
            ON CONFLICT (order_id) DO NOTHING;
        """)
        logger.info("Загружены заказы")
        
        # 3.6. Позиции заказов
        cursor.execute("""
            INSERT INTO normalized.order_items (
                order_id, item_id, item_quantity, item_price,
                item_discount, item_canceled_quantity, item_replaced_id
            )
            SELECT 
                order_id, item_id, item_quantity, item_price,
                item_discount, item_canceled_quantity, item_replaced_id
            FROM staging.deliveries_raw
            ON CONFLICT (order_id, item_id) DO NOTHING;
        """)
        logger.info("Загружены позиции заказов")
        
        # 3.7. История доставки (упрощенно - берем первого курьера)
        cursor.execute("""
            INSERT INTO normalized.delivery_history (
                order_id, driver_id, assigned_at, is_final_driver
            )
            SELECT DISTINCT 
                order_id, driver_id, 
                COALESCE(delivery_started_at, created_at) as assigned_at,
                CASE WHEN delivered_at IS NOT NULL THEN TRUE ELSE FALSE END as is_final_driver
            FROM staging.deliveries_raw
            WHERE driver_id IS NOT NULL
            ON CONFLICT DO NOTHING;
        """)
        logger.info("Загружена история доставки")
        
        conn.commit()
        
        # 4. Очищаем staging таблицу
        cursor.execute("DROP TABLE IF EXISTS staging.deliveries_raw;")
        conn.commit()
        
        logger.info(f"Успешно загружено {total_rows} строк")
        logger.info("Данные успешно загружены в нормализованные таблицы")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных: {e}")
        raise

with DAG(
    'load_to_normalized',
    default_args=default_args,
    description='Загрузка данных из Parquet в нормализованные таблицы',
    schedule_interval='@daily',
    catchup=False,
    tags=['delivery', 'etl'],
) as dag:
    
    create_staging_task = PythonOperator(
        task_id='create_staging_schema',
        python_callable=create_staging_schema,
    )
    
    load_data_task = PythonOperator(
        task_id='load_data_to_normalized',
        python_callable=load_data_to_normalized,
    )
    
    create_staging_task >> load_data_task
