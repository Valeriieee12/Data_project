"""
DAG для построения аналитических витрин из нормализованных данных.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import sys
import os

# Добавляем путь к скриптам
sys.path.append('/opt/airflow/scripts')

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_datamarts_builder():
    """Функция для запуска PySpark скрипта построения витрин."""
    import subprocess
    import sys
    
    # Путь к скрипту
    script_path = "/opt/airflow/scripts/build_datamarts.py"
    
    # Запускаем скрипт
    result = subprocess.run(
        [sys.executable, script_path],
        capture_output=True,
        text=True
    )
    
    # Выводим результаты
    print("STDOUT:", result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
    
    # Проверяем код возврата
    if result.returncode != 0:
        raise Exception(f"Скрипт завершился с ошибкой. Код возврата: {result.returncode}")
    
    return "Витрины успешно построены"

with DAG(
    dag_id='04_build_datamarts',
    default_args=default_args,
    description='Построение аналитических витрин из нормализованных данных',
    schedule_interval='0 2 * * *',  # Запуск каждый день в 2:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['datamart', 'analytics', 'pyspark'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Создаем схему для витрин если её нет
    create_datamart_schema = PostgresOperator(
        task_id='create_datamart_schema',
        postgres_conn_id='postgres_deliveries',
        sql="""
        CREATE SCHEMA IF NOT EXISTS datamart;
        """,
    )
    
    # Создаем таблицу для витрины заказов
    create_orders_datamart_table = PostgresOperator(
        task_id='create_orders_datamart_table',
        postgres_conn_id='postgres_deliveries',
        sql="""
        DROP TABLE IF EXISTS datamart.orders_daily;
        
        CREATE TABLE datamart.orders_daily (
            order_year INTEGER,
            order_month INTEGER,
            order_date DATE,
            city VARCHAR(50),
            store_id INTEGER,
            total_orders INTEGER,
            paid_orders INTEGER,
            delivered_orders INTEGER,
            canceled_orders INTEGER,
            canceled_after_delivery INTEGER,
            service_error_cancels INTEGER,
            unique_customers INTEGER,
            turnover DECIMAL(15, 2),
            revenue DECIMAL(15, 2),
            profit DECIMAL(15, 2),
            avg_check DECIMAL(15, 2),
            orders_per_customer DECIMAL(10, 2),
            revenue_per_customer DECIMAL(15, 2),
            orders_with_driver_change INTEGER,
            active_drivers INTEGER,
            day_of_week VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (order_date, store_id)
        );
        
        COMMENT ON TABLE datamart.orders_daily IS 'Ежедневная витрина метрик по заказам';
        COMMENT ON COLUMN datamart.orders_daily.order_year IS 'Год заказа';
        COMMENT ON COLUMN datamart.orders_daily.order_month IS 'Месяц заказа';
        COMMENT ON COLUMN datamart.orders_daily.order_date IS 'Дата заказа';
        COMMENT ON COLUMN datamart.orders_daily.city IS 'Город магазина';
        COMMENT ON COLUMN datamart.orders_daily.store_id IS 'ID магазина';
        COMMENT ON COLUMN datamart.orders_daily.total_orders IS 'Всего заказов';
        COMMENT ON COLUMN datamart.orders_daily.paid_orders IS 'Оплаченных заказов';
        COMMENT ON COLUMN datamart.orders_daily.delivered_orders IS 'Доставленных заказов';
        COMMENT ON COLUMN datamart.orders_daily.canceled_orders IS 'Отмененных заказов';
        COMMENT ON COLUMN datamart.orders_daily.canceled_after_delivery IS 'Отмен после доставки';
        COMMENT ON COLUMN datamart.orders_daily.service_error_cancels IS 'Отмен из-за ошибок сервиса';
        COMMENT ON COLUMN datamart.orders_daily.unique_customers IS 'Уникальных покупателей';
        COMMENT ON COLUMN datamart.orders_daily.turnover IS 'Оборот';
        COMMENT ON COLUMN datamart.orders_daily.revenue IS 'Выручка';
        COMMENT ON COLUMN datamart.orders_daily.profit IS 'Прибыль';
        COMMENT ON COLUMN datamart.orders_daily.avg_check IS 'Средний чек';
        COMMENT ON COLUMN datamart.orders_daily.orders_per_customer IS 'Заказов на покупателя';
        COMMENT ON COLUMN datamart.orders_daily.revenue_per_customer IS 'Выручка на покупателя';
        COMMENT ON COLUMN datamart.orders_daily.orders_with_driver_change IS 'Заказов со сменой курьера';
        COMMENT ON COLUMN datamart.orders_daily.active_drivers IS 'Активных курьеров';
        COMMENT ON COLUMN datamart.orders_daily.day_of_week IS 'День недели';
        COMMENT ON COLUMN datamart.orders_daily.created_at IS 'Время создания записи';
        """,
    )
    
    # Создаем таблицу для витрины товаров
    create_products_datamart_table = PostgresOperator(
        task_id='create_products_datamart_table',
        postgres_conn_id='postgres_deliveries',
        sql="""
        DROP TABLE IF EXISTS datamart.product_sales_daily;
        
        CREATE TABLE datamart.product_sales_daily (
            sale_year INTEGER,
            sale_month INTEGER,
            sale_date DATE,
            city VARCHAR(50),
            store_id INTEGER,
            item_id INTEGER,
            item_title VARCHAR(255),
            category VARCHAR(100),
            total_ordered_quantity INTEGER,
            total_canceled_quantity INTEGER,
            orders_with_item INTEGER,
            orders_with_canceled_items INTEGER,
            item_turnover DECIMAL(15, 2),
            cancel_rate DECIMAL(5, 4),
            is_daily_top_product INTEGER,
            is_daily_worst_product INTEGER,
            is_monthly_top_product INTEGER,
            is_monthly_worst_product INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (sale_date, store_id, item_id)
        );
        
        COMMENT ON TABLE datamart.product_sales_daily IS 'Ежедневная витрина метрик по товарам';
        COMMENT ON COLUMN datamart.product_sales_daily.sale_year IS 'Год продажи';
        COMMENT ON COLUMN datamart.product_sales_daily.sale_month IS 'Месяц продажи';
        COMMENT ON COLUMN datamart.product_sales_daily.sale_date IS 'Дата продажи';
        COMMENT ON COLUMN datamart.product_sales_daily.city IS 'Город магазина';
        COMMENT ON COLUMN datamart.product_sales_daily.store_id IS 'ID магазина';
        COMMENT ON COLUMN datamart.product_sales_daily.item_id IS 'ID товара';
        COMMENT ON COLUMN datamart.product_sales_daily.item_title IS 'Название товара';
        COMMENT ON COLUMN datamart.product_sales_daily.category IS 'Категория товара';
        COMMENT ON COLUMN datamart.product_sales_daily.total_ordered_quantity IS 'Всего заказанных единиц';
        COMMENT ON COLUMN datamart.product_sales_daily.total_canceled_quantity IS 'Всего отмененных единиц';
        COMMENT ON COLUMN datamart.product_sales_daily.orders_with_item IS 'Заказов с товаром';
        COMMENT ON COLUMN datamart.product_sales_daily.orders_with_canceled_items IS 'Заказов с отменой товара';
        COMMENT ON COLUMN datamart.product_sales_daily.item_turnover IS 'Оборот товара';
        COMMENT ON COLUMN datamart.product_sales_daily.cancel_rate IS 'Процент отмен';
        COMMENT ON COLUMN datamart.product_sales_daily.is_daily_top_product IS 'Флаг топ товара дня';
        COMMENT ON COLUMN datamart.product_sales_daily.is_daily_worst_product IS 'Флаг худшего товара дня';
        COMMENT ON COLUMN datamart.product_sales_daily.is_monthly_top_product IS 'Флаг топ товара месяца';
        COMMENT ON COLUMN datamart.product_sales_daily.is_monthly_worst_product IS 'Флаг худшего товара месяца';
        COMMENT ON COLUMN datamart.product_sales_daily.created_at IS 'Время создания записи';
        """,
    )
    
    # Запускаем PySpark скрипт для построения витрин
    build_datamarts = PythonOperator(
        task_id='build_datamarts',
        python_callable=run_datamarts_builder,
    )
    
    # Проверяем результаты
    check_results = PostgresOperator(
        task_id='check_results',
        postgres_conn_id='postgres_deliveries',
        sql="""
        -- Проверяем витрину заказов
        SELECT 'orders_daily' as table_name, COUNT(*) as row_count FROM datamart.orders_daily
        UNION ALL
        -- Проверяем витрину товаров
        SELECT 'product_sales_daily' as table_name, COUNT(*) as row_count FROM datamart.product_sales_daily;
        """,
    )
    
    end = EmptyOperator(task_id='end')
    
    # Определяем порядок выполнения задач
    start >> create_datamart_schema
    create_datamart_schema >> [create_orders_datamart_table, create_products_datamart_table]
    [create_orders_datamart_table, create_products_datamart_table] >> build_datamarts
    build_datamarts >> check_results >> end
