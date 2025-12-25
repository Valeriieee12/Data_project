import sys
import os
import subprocess

# Устанавливаем переменные окружения для Java 17 (ARM64)
java_home_path = '/usr/lib/jvm/java-17-openjdk-arm64'
os.environ['JAVA_HOME'] = java_home_path
os.environ['PATH'] = f"{java_home_path}/bin:{os.environ.get('PATH', '')}"

# Проверяем переменные окружения
print(f"JAVA_HOME: {os.environ.get('JAVA_HOME')}")
print(f"PATH начало: {os.environ.get('PATH')[:100]}")

from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_parquet_with_spark():
    """Загрузка Parquet файла в PostgreSQL с помощью PySpark"""
    
    logger.info("=== НАЧАЛО ЗАГРУЗКИ ДАННЫХ ===")
    
    # Проверяем доступность Java
    try:
        result = subprocess.run(['java', '-version'], capture_output=True, text=True, timeout=10)
        logger.info(f"Java доступна: {result.stderr.split(chr(10))[0] if result.stderr else 'OK'}")
    except Exception as e:
        logger.error(f"Java не доступна: {e}")
        return False
    
    # Путь к JAR файлу PostgreSQL
    postgres_jar = "/opt/airflow/jars/postgresql-42.7.0.jar"
    logger.info(f"PostgreSQL JAR: {postgres_jar}")
    
    try:
        # Создаем Spark сессию
        logger.info("Создание Spark сессии...")
        
        spark = SparkSession.builder \
            .appName("DeliveryDataLoader") \
            .config("spark.jars", postgres_jar) \
            .config("spark.driver.extraClassPath", postgres_jar) \
            .master("local[*]") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .getOrCreate()
        
        logger.info("✓ Spark сессия создана успешно!")
        
        # Путь к файлу
        file_path = "/opt/airflow/data/deliveries.parquet"
        logger.info(f"Чтение файла: {file_path}")
        
        # Читаем Parquet файл с ограничением для теста
        logger.info("Загрузка данных...")
        df = spark.read.parquet(file_path).limit(10000)  # 10,000 строк для теста
        
        # Информация о данных
        total_rows = df.count()
        logger.info(f"✓ Прочитано {total_rows:,} строк")
        
        # Показываем пример данных
        logger.info("Пример данных (первые 3 строки):")
        df.select("order_id", "user_id", "created_at", "item_title", "item_price").show(3, truncate=False)
        
        # Настройки подключения к PostgreSQL
        url = "jdbc:postgresql://postgres:5432/delivery_db"
        properties = {
            "user": "delivery_user",
            "password": "delivery_password",
            "driver": "org.postgresql.Driver",
            "batchsize": "10000"
        }
        
        # Записываем данные в PostgreSQL
        logger.info("Запись данных в PostgreSQL...")
        df.write \
            .mode("overwrite") \
            .option("batchsize", 10000) \
            .jdbc(url=url, table="staging.deliveries_raw", properties=properties)
        
        logger.info(f"✓ Данные успешно загружены!")
        logger.info(f"✓ Загружено {total_rows:,} строк в staging.deliveries_raw")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Ошибка при загрузке данных: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    finally:
        try:
            spark.stop()
            logger.info("Spark сессия остановлена")
        except:
            pass

if __name__ == "__main__":
    success = load_parquet_with_spark()
    sys.exit(0 if success else 1)
