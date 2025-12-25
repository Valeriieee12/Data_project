#!/usr/bin/env python3
"""
Скрипт для настройки подключения к базе данных delivery_db в Airflow
Запускать внутри контейнера airflow-webserver или airflow-scheduler
"""
import os
from airflow.models import Connection
from airflow.settings import Session

# Параметры подключения
conn_id = "delivery_postgres"
conn_type = "postgres"
host = "postgres"  # Имя сервиса в docker-compose
port = 5432
login = "delivery_user"
password = "delivery_password"
schema = "delivery_db"

def setup_connection():
    """Настройка подключения в Airflow"""
    
    # Проверяем, существует ли уже такое подключение
    session = Session()
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    
    if existing_conn:
        print(f"Подключение {conn_id} уже существует. Обновляем...")
        existing_conn.host = host
        existing_conn.port = port
        existing_conn.login = login
        existing_conn.password = password
        existing_conn.schema = schema
        existing_conn.conn_type = conn_type
    else:
        print(f"Создаем новое подключение {conn_id}...")
        new_conn = Connection(
            conn_id=conn_id,
            conn_type=conn_type,
            host=host,
            port=port,
            login=login,
            password=password,
            schema=schema
        )
        session.add(new_conn)
    
    session.commit()
    session.close()
    print(f"Подключение {conn_id} успешно настроено")
    
    # Также создаем подключение через переменные окружения для PySpark
    os.environ['DELIVERY_DB_CONN'] = f"postgresql+psycopg2://{login}:{password}@{host}:{port}/{schema}"
    print("Переменная окружения DELIVERY_DB_CONN установлена")

if __name__ == "__main__":
    setup_connection()
