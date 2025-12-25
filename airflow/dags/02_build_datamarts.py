from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging
import sys
import os

# Добавляем путь к скриптам
sys.path.append('/opt/airflow/scripts')

default_args = {
    'owner': 'delivery_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def build_datamarts_with_spark():
    """Построение витрин данных с использованием PySpark"""
    
    logger = logging.getLogger(__name__)
    logger.info("Начало построения витрин с PySpark")
    
    try:
        # Импортируем Spark процессор
        from spark_processor import SparkDataProcessor
        
        # Инициализируем Spark процессор
        processor = SparkDataProcessor()
        
        # Загружаем данные из Parquet
        file_path = "/opt/airflow/data/deliveries.parquet"
        raw_df = processor.load_parquet_data(file_path)
        
        # Преобразуем в нормализованные данные
        normalized_data = processor.transform_to_normalized(raw_df)
        
        # Строим витрины
        datamarts = processor.build_datamarts(normalized_data)
        
        # Сохраняем витрины в PostgreSQL
        logger.info("Сохранение витрины orders_daily...")
        processor.save_to_postgres(
            datamarts['orders_daily'], 
            'orders_daily', 
            schema='datamart'
        )
        
        logger.info("Сохранение витрины product_sales_daily...")
        processor.save_to_postgres(
            datamarts['product_sales_daily'], 
            'product_sales_daily', 
            schema='datamart'
        )
        
        # Останавливаем Spark
        processor.stop()
        
        logger.info("Витрины успешно построены и сохранены")
        
    except Exception as e:
        logger.error(f"Ошибка при построении витрин: {e}")
        raise

def build_datamarts_sql():
    """Построение витрин данных с использованием SQL (альтернатива)"""
    
    logger = logging.getLogger(__name__)
    logger.info("Начало построения витрин с SQL")
    
    try:
        # Подключаемся к PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='delivery_postgres')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Очищаем витрины перед обновлением
        cursor.execute("TRUNCATE TABLE datamart.orders_daily;")
        cursor.execute("TRUNCATE TABLE datamart.product_sales_daily;")
        
        # Построение витрины orders_daily
        logger.info("Построение витрины orders_daily...")
        
        cursor.execute("""
            INSERT INTO datamart.orders_daily (
                report_date, city, store_id,
                orders_created, orders_delivered, orders_canceled,
                unique_customers
            )
            SELECT 
                DATE(o.created_at) as report_date,
                CASE 
                    WHEN o.address_text LIKE '%,%' THEN SPLIT_PART(o.address_text, ',', 1)
                    ELSE o.address_text 
                END as city,
                o.store_id,
                COUNT(DISTINCT o.order_id) as orders_created,
                COUNT(DISTINCT CASE WHEN o.delivered_at IS NOT NULL THEN o.order_id END) as orders_delivered,
                COUNT(DISTINCT CASE WHEN o.canceled_at IS NOT NULL THEN o.order_id END) as orders_canceled,
                COUNT(DISTINCT o.user_id) as unique_customers
            FROM normalized.orders o
            WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY 
                DATE(o.created_at),
                CASE 
                    WHEN o.address_text LIKE '%,%' THEN SPLIT_PART(o.address_text, ',', 1)
                    ELSE o.address_text 
                END,
                o.store_id
            ORDER BY report_date DESC, city, store_id;
        """)
        
        # Построение витрины product_sales_daily
        logger.info("Построение витрины product_sales_daily...")
        
        cursor.execute("""
            INSERT INTO datamart.product_sales_daily (
                report_date, city, store_id, item_category, item_id,
                ordered_units, canceled_units, orders_with_item
            )
            SELECT 
                DATE(o.created_at) as report_date,
                CASE 
                    WHEN o.address_text LIKE '%,%' THEN SPLIT_PART(o.address_text, ',', 1)
                    ELSE o.address_text 
                END as city,
                o.store_id,
                i.item_category,
                i.item_id,
                SUM(oi.item_quantity) as ordered_units,
                SUM(oi.item_canceled_quantity) as canceled_units,
                COUNT(DISTINCT oi.order_id) as orders_with_item
            FROM normalized.orders o
            JOIN normalized.order_items oi ON o.order_id = oi.order_id
            JOIN normalized.items i ON oi.item_id = i.item_id
            WHERE o.created_at >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY 
                DATE(o.created_at),
                CASE 
                    WHEN o.address_text LIKE '%,%' THEN SPLIT_PART(o.address_text, ',', 1)
                    ELSE o.address_text 
                END,
                o.store_id,
                i.item_category,
                i.item_id
            ORDER BY report_date DESC, city, store_id, item_category, item_id;
        """)
        
        conn.commit()
        
        logger.info("Витрины успешно построены с помощью SQL")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Ошибка при построении витрин с SQL: {e}")
        raise

with DAG(
    'build_datamarts',
    default_args=default_args,
    description='Построение витрин данных для аналитики',
    schedule_interval='@daily',
    catchup=False,
    tags=['delivery', 'datamart', 'analytics'],
) as dag:
    
    # Задача с PySpark (основная)
    build_datamarts_spark_task = PythonOperator(
        task_id='build_datamarts_with_spark',
        python_callable=build_datamarts_with_spark,
    )
    
    # Задача с SQL (резервная)
    build_datamarts_sql_task = PythonOperator(
        task_id='build_datamarts_sql',
        python_callable=build_datamarts_sql,
    )
    
    # Устанавливаем зависимости (можно запускать параллельно или последовательно)
    # Для начала используем SQL версию как более простую
    build_datamarts_sql_task
