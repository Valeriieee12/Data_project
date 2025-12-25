"""
DAG для тестирования витрин данных.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def test_orders_datamart():
    """Тестирование витрины заказов."""
    hook = PostgresHook(postgres_conn_id='postgres_deliveries')
    
    # Проверяем наличие данных
    df = hook.get_pandas_df("SELECT * FROM datamart.orders_daily LIMIT 5")
    
    print("=== Тестирование витрины заказов ===")
    print(f"Количество строк: {len(df)}")
    print("\nПервые 5 строк:")
    print(df.to_string())
    
    # Проверяем метрики
    metrics_df = hook.get_pandas_df("""
        SELECT 
            COUNT(*) as total_rows,
            MIN(order_date) as min_date,
            MAX(order_date) as max_date,
            COUNT(DISTINCT store_id) as stores_count,
            COUNT(DISTINCT city) as cities_count,
            SUM(total_orders) as total_orders_sum,
            SUM(revenue) as total_revenue
        FROM datamart.orders_daily
    """)
    
    print("\n=== Агрегированные метрики ===")
    print(metrics_df.to_string())
    
    return "Тестирование витрины заказов завершено"

def test_products_datamart():
    """Тестирование витрины товаров."""
    hook = PostgresHook(postgres_conn_id='postgres_deliveries')
    
    # Проверяем наличие данных
    df = hook.get_pandas_df("SELECT * FROM datamart.product_sales_daily LIMIT 5")
    
    print("=== Тестирование витрины товаров ===")
    print(f"Количество строк: {len(df)}")
    print("\nПервые 5 строк:")
    print(df.to_string())
    
    # Проверяем метрики
    metrics_df = hook.get_pandas_df("""
        SELECT 
            COUNT(*) as total_rows,
            MIN(sale_date) as min_date,
            MAX(sale_date) as max_date,
            COUNT(DISTINCT item_id) as unique_items,
            COUNT(DISTINCT category) as unique_categories,
            SUM(total_ordered_quantity) as total_ordered,
            SUM(item_turnover) as total_turnover
        FROM datamart.product_sales_daily
    """)
    
    print("\n=== Агрегированные метрики ===")
    print(metrics_df.to_string())
    
    # Проверяем топ товары
    top_products_df = hook.get_pandas_df("""
        SELECT 
            sale_date,
            item_title,
            category,
            total_ordered_quantity,
            item_turnover,
            is_daily_top_product
        FROM datamart.product_sales_daily 
        WHERE is_daily_top_product = 1
        ORDER BY sale_date, total_ordered_quantity DESC
        LIMIT 10
    """)
    
    print("\n=== Топ товары по дням ===")
    print(top_products_df.to_string())
    
    return "Тестирование витрины товаров завершено"

def run_sql_queries_from_files():
    """Запуск SQL запросов из файлов для проверки."""
    import os
    
    sql_dir = "/opt/airflow/scripts/sql"
    
    print("=== Запуск SQL запросов из файлов ===")
    
    for filename in ["orders_datamart.sql", "products_datamart.sql"]:
        filepath = os.path.join(sql_dir, filename)
        if os.path.exists(filepath):
            print(f"\n--- Запуск {filename} ---")
            with open(filepath, 'r') as f:
                sql = f.read()
                
            # Выводим первые 200 символов запроса
            print(f"SQL (первые 200 символов): {sql[:200]}...")
        else:
            print(f"Файл {filename} не найден")
    
    return "SQL запросы проверены"

with DAG(
    dag_id='05_test_datamarts',
    default_args=default_args,
    description='Тестирование витрин данных',
    schedule_interval=None,  # Только ручной запуск
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test', 'datamart', 'validation'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    test_sql_files = PythonOperator(
        task_id='test_sql_files',
        python_callable=run_sql_queries_from_files,
    )
    
    test_orders = PythonOperator(
        task_id='test_orders_datamart',
        python_callable=test_orders_datamart,
    )
    
    test_products = PythonOperator(
        task_id='test_products_datamart',
        python_callable=test_products_datamart,
    )
    
    summary = PostgresOperator(
        task_id='summary',
        postgres_conn_id='postgres_deliveries',
        sql="""
        SELECT 
            'orders_daily' as table_name,
            COUNT(*) as row_count,
            MIN(order_date) as min_date,
            MAX(order_date) as max_date
        FROM datamart.orders_daily
        UNION ALL
        SELECT 
            'product_sales_daily' as table_name,
            COUNT(*) as row_count,
            MIN(sale_date) as min_date,
            MAX(sale_date) as max_date
        FROM datamart.product_sales_daily
        """,
    )
    
    end = EmptyOperator(task_id='end')
    
    # Определяем порядок выполнения задач
    start >> test_sql_files >> [test_orders, test_products] >> summary >> end
