import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_parquet_with_spark():
    """Загрузка Parquet файла в PostgreSQL с помощью PySpark"""
    
    logger.info("Инициализация Spark сессии...")
    
    # Создаем Spark сессию
    spark = SparkSession.builder \
        .appName("ParquetToPostgreSQL") \
        .config("spark.jars", "/opt/airflow/jars/postgresql-42.7.0.jar") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()
    
    try:
        # Путь к файлу
        file_path = "/opt/airflow/data/deliveries.parquet"
        logger.info(f"Чтение Parquet файла: {file_path}")
        
        # Читаем Parquet файл
        df = spark.read.parquet(file_path)
        
        # Показываем информацию о данных
        logger.info(f"Прочитано {df.count():,} строк")
        logger.info("Схема данных:")
        df.printSchema()
        
        # Показываем первые 5 строк для проверки
        logger.info("Первые 5 строк:")
        df.show(5)
        
        # Настройки подключения к PostgreSQL
        url = "jdbc:postgresql://postgres:5432/delivery_db"
        properties = {
            "user": "delivery_user",
            "password": "delivery_password",
            "driver": "org.postgresql.Driver"
        }
        
        # Записываем данные в PostgreSQL
        logger.info("Запись данных в PostgreSQL...")
        df.write \
            .mode("overwrite") \
            .jdbc(url=url, table="staging.deliveries_raw", properties=properties)
        
        logger.info("✓ Данные успешно загружены в staging.deliveries_raw")
        
        # Проверяем количество записанных строк
        result_df = spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("dbtable", "staging.deliveries_raw") \
            .option("user", properties["user"]) \
            .option("password", properties["password"]) \
            .load()
        
        logger.info(f"✓ Проверка: в таблице сейчас {result_df.count():,} строк")
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных: {e}")
        raise
    finally:
        # Останавливаем Spark сессию
        spark.stop()
        logger.info("Spark сессия остановлена")

if __name__ == "__main__":
    load_parquet_with_spark()
