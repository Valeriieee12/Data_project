"""
Самый простой тестовый DAG для проверки загрузки
"""
from datetime import datetime, timedelta
import logging
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

def simple_test_load():
    """Простая тестовая загрузка"""
    logger = logging.getLogger(__name__)
    logger.info("Начало простой тестовой загрузки")
    
    # Просто проверяем подключение и создаем тестовую запись
    hook = PostgresHook(postgres_conn_id='delivery_postgres')
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    # Создаем простую тестовую таблицу
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS staging.test_table (
            id SERIAL PRIMARY KEY,
            test_text VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Вставляем тестовую запись
    cursor.execute("INSERT INTO staging.test_table (test_text) VALUES ('Тестовая запись от Airflow')")
    conn.commit()
    
    # Проверяем
    cursor.execute("SELECT COUNT(*) FROM staging.test_table")
    count = cursor.fetchone()[0]
    
    logger.info(f"В таблице staging.test_table теперь {count} записей")
    
    cursor.close()
    conn.close()
    
    logger.info("Простая тестовая загрузка завершена успешно")

with DAG(
    'simple_test_load',
    default_args=default_args,
    description='Простейший тестовый DAG',
    schedule_interval=None,
    catchup=False,
    tags=['delivery', 'test', 'simple'],
) as dag:
    
    start = DummyOperator(task_id='start')
    
    test_task = PythonOperator(
        task_id='simple_test_load',
        python_callable=simple_test_load,
    )
    
    end = DummyOperator(task_id='end')
    
    start >> test_task >> end
