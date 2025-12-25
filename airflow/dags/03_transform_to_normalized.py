"""
DAG для трансформации данных из staging в нормализованные таблицы
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'delivery_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def transform_to_normalized_task():
    """Задача трансформации данных"""
    import sys
    sys.path.append('/opt/airflow/scripts')
    from transform_to_normalized import transform_to_normalized
    return transform_to_normalized()

with DAG(
    'transform_to_normalized',
    default_args=default_args,
    description='Трансформация данных из staging в нормализованные таблицы',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['delivery', 'etl', 'normalization'],
) as dag:

    start = DummyOperator(task_id='start')
    
    transform_task = PythonOperator(
        task_id='transform_staging_to_normalized',
        python_callable=transform_to_normalized_task,
    )
    
    end = DummyOperator(task_id='end')
    
    start >> transform_task >> end
