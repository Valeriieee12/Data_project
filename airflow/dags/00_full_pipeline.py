"""
Мастер-DAG для полного пайплайна обработки данных доставок.
Запускает все этапы: загрузку, нормализацию, построение витрин.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='00_full_pipeline',
    default_args=default_args,
    description='Полный пайплайн обработки данных доставок',
    schedule_interval='0 1 * * *',  # Запуск каждый день в 1:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['pipeline', 'delivery', 'etl'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    # Триггерим DAG для загрузки данных
    trigger_load_data = TriggerDagRunOperator(
        task_id='trigger_load_data',
        trigger_dag_id='02_load_full_data',
        wait_for_completion=True,
        reset_dag_run=True,
    )
    
    # Триггерим DAG для трансформации в нормализованные таблицы
    trigger_transform = TriggerDagRunOperator(
        task_id='trigger_transform',
        trigger_dag_id='03_transform_to_normalized',
        wait_for_completion=True,
        reset_dag_run=True,
    )
    
    # Триггерим DAG для построения витрин
    trigger_build_datamarts = TriggerDagRunOperator(
        task_id='trigger_build_datamarts',
        trigger_dag_id='04_build_datamarts',
        wait_for_completion=True,
        reset_dag_run=True,
    )
    
    # Триггерим DAG для тестирования витрин
    trigger_test_datamarts = TriggerDagRunOperator(
        task_id='trigger_test_datamarts',
        trigger_dag_id='05_test_datamarts',
        wait_for_completion=True,
        reset_dag_run=True,
    )
    
    end = EmptyOperator(task_id='end')
    
    # Определяем порядок выполнения
    start >> trigger_load_data >> trigger_transform >> trigger_build_datamarts >> trigger_test_datamarts >> end
