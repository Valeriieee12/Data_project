"""
DAG для проверки соединений и настроек перед запуском пайплайна.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
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

def run_connection_check():
    """Запуск проверки соединений."""
    import subprocess
    import sys
    
    script_path = "/opt/airflow/scripts/check_connections.py"
    
    result = subprocess.run(
        [sys.executable, script_path],
        capture_output=True,
        text=True
    )
    
    print("STDOUT:")
    print(result.stdout)
    
    if result.stderr:
        print("STDERR:")
        print(result.stderr)
    
    if result.returncode != 0:
        raise Exception("Проверка соединений не прошла")
    
    return "Проверка соединений успешно завершена"

with DAG(
    dag_id='01_check_connections',
    default_args=default_args,
    description='Проверка соединений и настроек перед запуском пайплайна',
    schedule_interval='0 0 * * *',  # Запуск каждый день в 0:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['check', 'connection', 'validation'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    check_connections = PythonOperator(
        task_id='check_connections',
        python_callable=run_connection_check,
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> check_connections >> end
