"""
Тестовый скрипт для проверки подключения PySpark к PostgreSQL.
"""
from pyspark.sql import SparkSession

def test_spark_postgres():
    """Тестирование подключения Spark к PostgreSQL."""
    print("Создание Spark сессии...")
    
    spark = SparkSession.builder \
        .appName("TestPostgreSQLConnection") \
        .config("spark.jars", "/opt/airflow/postgresql-42.7.0.jar") \
        .config("spark.driver.extraClassPath", "/opt/airflow/postgresql-42.7.0.jar") \
        .config("spark.executor.extraClassPath", "/opt/airflow/postgresql-42.7.0.jar") \
        .getOrCreate()
    
    print("Spark сессия создана успешно!")
    
    # Настройки подключения к PostgreSQL
    jdbc_url = "jdbc:postgresql://postgres:5432/delivery_db"
    connection_properties = {
        "user": "delivery_user",
        "password": "delivery_password",
        "driver": "org.postgresql.Driver"
    }
    
    try:
        # Пытаемся прочитать небольшую таблицу
        print("Попытка подключения к PostgreSQL...")
        df = spark.read \
            .jdbc(url=jdbc_url, table="(SELECT 1 as test_column) as test", properties=connection_properties)
        
        print("Подключение успешно!")
        print(f"Результат запроса: {df.collect()}")
        
        # Пробуем прочитать реальную таблицу если есть
        try:
            tables_df = spark.read \
                .jdbc(url=jdbc_url, table="(SELECT table_name FROM information_schema.tables WHERE table_schema = 'public') as tables", 
                      properties=connection_properties)
            
            print(f"\nДоступные таблицы в схеме public: {[row.table_name for row in tables_df.collect()]}")
        except Exception as e:
            print(f"Не удалось получить список таблиц: {e}")
            
    except Exception as e:
        print(f"Ошибка подключения к PostgreSQL: {e}")
        print("Проверьте:")
        print("1. PostgreSQL запущен и доступен")
        print("2. Драйвер postgresql-42.7.0.jar находится в /opt/airflow/")
        print("3. Настройки подключения корректны")
    
    finally:
        spark.stop()
        print("\nSpark сессия остановлена")

if __name__ == "__main__":
    test_spark_postgres()
