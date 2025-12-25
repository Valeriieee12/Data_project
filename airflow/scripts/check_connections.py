"""
Скрипт для проверки всех соединений и настроек.
"""
import os
import sys
from pyspark.sql import SparkSession
import psycopg2

def check_postgres_connection():
    """Проверка подключения к PostgreSQL."""
    print("=== Проверка подключения к PostgreSQL ===")
    
    try:
        conn = psycopg2.connect(
            host="postgres",
            port="5432",
            database="delivery_db",
            user="delivery_user",
            password="delivery_password"
        )
        cursor = conn.cursor()
        
        # Проверяем соединение
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        
        print("✓ PostgreSQL подключение успешно")
        print(f"  Результат запроса: {result}")
        
        # Проверяем таблицы
        cursor.execute("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_schema IN ('public', 'staging', 'datamart')
            ORDER BY table_schema, table_name
        """)
        
        tables = cursor.fetchall()
        print(f"  Найдено таблиц: {len(tables)}")
        
        for schema, table in tables[:10]:  # Показываем первые 10
            print(f"    {schema}.{table}")
        
        if len(tables) > 10:
            print(f"    ... и еще {len(tables) - 10} таблиц")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"✗ Ошибка подключения к PostgreSQL: {e}")
        return False
    
    return True

def check_spark_session():
    """Проверка создания Spark сессии."""
    print("\n=== Проверка Spark сессии ===")
    
    try:
        spark = SparkSession.builder \
            .appName("ConnectionTest") \
            .config("spark.jars", "/opt/airflow/postgresql-42.7.0.jar") \
            .config("spark.driver.extraClassPath", "/opt/airflow/postgresql-42.7.0.jar") \
            .getOrCreate()
        
        print("✓ Spark сессия создана успешно")
        print(f"  Spark версия: {spark.version}")
        
        # Проверяем доступность PostgreSQL через Spark
        jdbc_url = "jdbc:postgresql://postgres:5432/delivery_db"
        connection_properties = {
            "user": "delivery_user",
            "password": "delivery_password",
            "driver": "org.postgresql.Driver"
        }
        
        try:
            df = spark.read \
                .jdbc(url=jdbc_url, table="(SELECT 1 as test) as tmp", properties=connection_properties)
            print("✓ Spark может подключиться к PostgreSQL")
            print(f"  Результат запроса: {df.collect()}")
        except Exception as e:
            print(f"✗ Spark не может подключиться к PostgreSQL: {e}")
        
        spark.stop()
        
    except Exception as e:
        print(f"✗ Ошибка создания Spark сессии: {e}")
        return False
    
    return True

def check_data_files():
    """Проверка наличия файлов с данными."""
    print("\n=== Проверка файлов данных ===")
    
    data_path = "/opt/airflow/data"
    
    if os.path.exists(data_path):
        files = os.listdir(data_path)
        print(f"✓ Директория данных существует: {data_path}")
        print(f"  Файлов в директории: {len(files)}")
        
        for file in files[:5]:  # Показываем первые 5 файлов
            file_path = os.path.join(data_path, file)
            size = os.path.getsize(file_path)
            print(f"    {file} ({size} bytes)")
        
        if len(files) > 5:
            print(f"    ... и еще {len(files) - 5} файлов")
        
        # Проверяем наличие parquet файла
        parquet_files = [f for f in files if f.endswith('.parquet')]
        if parquet_files:
            print(f"✓ Найдены Parquet файлы: {parquet_files}")
        else:
            print("✗ Parquet файлы не найдены")
            
    else:
        print(f"✗ Директория данных не существует: {data_path}")
        return False
    
    return True

def check_scripts():
    """Проверка наличия скриптов."""
    print("\n=== Проверка скриптов ===")
    
    scripts_path = "/opt/airflow/scripts"
    required_scripts = [
        "config.py",
        "build_datamarts.py",
        "test_spark_postgres.py",
        "transform_to_normalized.py",
        "load_with_spark_final.py"
    ]
    
    if os.path.exists(scripts_path):
        print(f"✓ Директория скриптов существует: {scripts_path}")
        
        for script in required_scripts:
            script_path = os.path.join(scripts_path, script)
            if os.path.exists(script_path):
                size = os.path.getsize(script_path)
                print(f"  ✓ {script} ({size} bytes)")
            else:
                print(f"  ✗ {script} не найден")
    else:
        print(f"✗ Директория скриптов не существует: {scripts_path}")
        return False
    
    return True

def main():
    """Основная функция проверки."""
    print("=" * 60)
    print("ПРОВЕРКА НАСТРОЕК И СОЕДИНЕНИЙ")
    print("=" * 60)
    
    checks = [
        ("PostgreSQL Connection", check_postgres_connection),
        ("Spark Session", check_spark_session),
        ("Data Files", check_data_files),
        ("Scripts", check_scripts),
    ]
    
    results = []
    
    for check_name, check_func in checks:
        try:
            success = check_func()
            results.append((check_name, success))
        except Exception as e:
            print(f"✗ Ошибка при проверке {check_name}: {e}")
            results.append((check_name, False))
    
    print("\n" + "=" * 60)
    print("ИТОГИ ПРОВЕРКИ:")
    print("=" * 60)
    
    all_passed = True
    for check_name, success in results:
        status = "✓ ПРОЙДЕНО" if success else "✗ НЕ ПРОЙДЕНО"
        print(f"{status} - {check_name}")
        if not success:
            all_passed = False
    
    print("\n" + "=" * 60)
    if all_passed:
        print("ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ УСПЕШНО!")
        print("Система готова к работе.")
    else:
        print("НЕКОТОРЫЕ ПРОВЕРКИ НЕ ПРОЙДЕНЫ!")
        print("Пожалуйста, исправьте ошибки перед запуском пайплайна.")
    
    return all_passed

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
