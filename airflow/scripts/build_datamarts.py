"""
PySpark скрипт для построения витрин данных из нормализованных таблиц.
"""
import sys
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Добавляем путь к родительской директории для импорта настроек
sys.path.append('/opt/airflow')
from airflow.scripts.config import POSTGRES_CONNECTION

def create_spark_session():
    """Создание Spark сессии с PostgreSQL драйвером."""
    spark = SparkSession.builder \
        .appName("DatamartsBuilder") \
        .config("spark.jars", "/opt/airflow/postgresql-42.7.0.jar") \
        .config("spark.driver.extraClassPath", "/opt/airflow/postgresql-42.7.0.jar") \
        .config("spark.executor.extraClassPath", "/opt/airflow/postgresql-42.7.0.jar") \
        .getOrCreate()
    
    # Установим время для корректной работы с часовыми поясами
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    
    return spark

def read_from_postgres(spark, table_name):
    """Чтение таблицы из PostgreSQL."""
    jdbc_url = f"jdbc:postgresql://{POSTGRES_CONNECTION['host']}:{POSTGRES_CONNECTION['port']}/{POSTGRES_CONNECTION['database']}"
    
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", POSTGRES_CONNECTION['user']) \
        .option("password", POSTGRES_CONNECTION['password']) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    return df

def write_to_postgres(df, table_name, mode="overwrite"):
    """Запись DataFrame в PostgreSQL."""
    jdbc_url = f"jdbc:postgresql://{POSTGRES_CONNECTION['host']}:{POSTGRES_CONNECTION['port']}/{POSTGRES_CONNECTION['database']}"
    
    df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", POSTGRES_CONNECTION['user']) \
        .option("password", POSTGRES_CONNECTION['password']) \
        .option("driver", "org.postgresql.Driver") \
        .mode(mode) \
        .save()
    
    print(f"Данные успешно записаны в таблицу {table_name}")

def create_datamart_schema(spark):
    """Создание схемы datamart в PostgreSQL через Spark."""
    jdbc_url = f"jdbc:postgresql://{POSTGRES_CONNECTION['host']}:{POSTGRES_CONNECTION['port']}/{POSTGRES_CONNECTION['database']}"
    
    # SQL для создания схемы
    create_schema_sql = """
    CREATE SCHEMA IF NOT EXISTS datamart;
    """
    
    # Выполняем SQL через JDBC
    spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "(SELECT 1) as tmp") \
        .option("user", POSTGRES_CONNECTION['user']) \
        .option("password", POSTGRES_CONNECTION['password']) \
        .option("driver", "org.postgresql.Driver") \
        .load() \
        .createOrReplaceTempView("tmp")
    
    # Используем прямое соединение для выполнения DDL
    import psycopg2
    conn = psycopg2.connect(
        host=POSTGRES_CONNECTION['host'],
        port=POSTGRES_CONNECTION['port'],
        database=POSTGRES_CONNECTION['database'],
        user=POSTGRES_CONNECTION['user'],
        password=POSTGRES_CONNECTION['password']
    )
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(create_schema_sql)
    cursor.close()
    conn.close()
    
    print("Схема datamart создана или уже существует")

def build_orders_datamart(spark):
    """Построение витрины заказов."""
    print("Начинаем построение витрины заказов...")
    
    # Чтение необходимых таблиц
    orders_df = read_from_postgres(spark, "orders")
    order_items_df = read_from_postgres(spark, "order_items")
    stores_df = read_from_postgres(spark, "stores")
    delivery_history_df = read_from_postgres(spark, "delivery_history")
    
    # Преобразование типов данных
    orders_df = orders_df \
        .withColumn("created_at", to_timestamp(col("created_at"))) \
        .withColumn("paid_at", to_timestamp(col("paid_at"))) \
        .withColumn("delivered_at", to_timestamp(col("delivered_at"))) \
        .withColumn("canceled_at", to_timestamp(col("canceled_at"))) \
        .withColumn("delivery_cost", col("delivery_cost").cast("double")) \
        .withColumn("order_discount", col("order_discount").cast("double"))
    
    order_items_df = order_items_df \
        .withColumn("item_price", col("item_price").cast("double")) \
        .withColumn("item_discount", col("item_discount").cast("double")) \
        .withColumn("item_quantity", col("item_quantity").cast("integer")) \
        .withColumn("item_canceled_quantity", col("item_canceled_quantity").cast("integer"))
    
    # Извлечение города из адреса магазина
    stores_df = stores_df.withColumn(
        "city",
        when(col("store_address").contains("Москва"), "Москва")
        .when(col("store_address").contains("Санкт-Петербург"), "Санкт-Петербург")
        .when(col("store_address").contains("Казань"), "Казань")
        .otherwise("Неизвестно")
    )
    
    # Основные расчеты на уровне заказа
    order_calculations = order_items_df \
        .filter(col("item_replaced_id").isNull()) \
        .groupBy("order_id") \
        .agg(
            sum((col("item_price") * col("item_quantity")) * 
                (1 - coalesce(col("item_discount"), lit(0)) / 100.0)).alias("order_revenue_before_discount")
        )
    
    # Объединяем данные
    orders_with_calc = orders_df \
        .join(order_calculations, "order_id", "left") \
        .join(stores_df.select("store_id", "city"), "store_id", "left") \
        .withColumn("order_revenue_before_discount", 
                   coalesce(col("order_revenue_before_discount"), lit(0)))
    
    # Добавляем флаги и метрики
    orders_enriched = orders_with_calc \
        .withColumn("order_date", to_date(col("created_at"))) \
        .withColumn("order_year", year(col("created_at"))) \
        .withColumn("order_month", month(col("created_at"))) \
        .withColumn("is_paid", when(col("paid_at").isNotNull(), 1).otherwise(0)) \
        .withColumn("is_delivered", when(col("delivered_at").isNotNull(), 1).otherwise(0)) \
        .withColumn("is_canceled", when(col("canceled_at").isNotNull(), 1).otherwise(0)) \
        .withColumn("is_canceled_after_delivery", 
                   when((col("canceled_at").isNotNull()) & 
                        (col("delivered_at").isNotNull()) & 
                        (col("canceled_at") > col("delivered_at")), 1).otherwise(0)) \
        .withColumn("is_service_error_cancel",
                   when(col("order_cancellation_reason").isin(["Ошибка приложения", "Проблемы с оплатой"]), 1)
                   .otherwise(0)) \
        .withColumn("order_revenue",
                   when(col("paid_at").isNotNull(),
                        (col("order_revenue_before_discount") * 
                         (1 - coalesce(col("order_discount"), lit(0)) / 100.0)) + 
                         coalesce(col("delivery_cost"), lit(0)))
                   .otherwise(0)) \
        .withColumn("order_profit",
                   when(col("paid_at").isNotNull(),
                        ((col("order_revenue_before_discount") * 
                          (1 - coalesce(col("order_discount"), lit(0)) / 100.0)) + 
                          coalesce(col("delivery_cost"), lit(0))) * 0.3)
                   .otherwise(0))
    
    # Агрегация по дням и магазинам
    orders_aggregated = orders_enriched \
        .groupBy("order_year", "order_month", "order_date", "city", "store_id") \
        .agg(
            countDistinct("order_id").alias("total_orders"),
            countDistinct(when(col("is_paid") == 1, col("order_id"))).alias("paid_orders"),
            countDistinct(when(col("is_delivered") == 1, col("order_id"))).alias("delivered_orders"),
            countDistinct(when(col("is_canceled") == 1, col("order_id"))).alias("canceled_orders"),
            sum("is_canceled_after_delivery").alias("canceled_after_delivery"),
            sum("is_service_error_cancel").alias("service_error_cancels"),
            countDistinct("user_id").alias("unique_customers"),
            sum("order_revenue_before_discount").alias("turnover"),
            sum("order_revenue").alias("revenue"),
            sum("order_profit").alias("profit"),
            avg("order_revenue").alias("avg_check"),
            (countDistinct("order_id").cast("double") / 
                when(countDistinct("user_id") == 0, None).otherwise(countDistinct("user_id"))).alias("orders_per_customer"),
            (sum("order_revenue") / 
                when(countDistinct("user_id") == 0, None).otherwise(countDistinct("user_id"))).alias("revenue_per_customer"),
            countDistinct("driver_id").alias("active_drivers")
        ) \
        .withColumn("day_of_week", date_format(col("order_date"), "EEEE")) \
        .orderBy("order_date", "store_id")
    
    # Добавляем информацию о сменах курьеров из истории доставки
    driver_changes = delivery_history_df \
        .groupBy("order_id") \
        .agg(count("*").alias("driver_count")) \
        .withColumn("has_driver_change", when(col("driver_count") > 1, 1).otherwise(0)) \
        .select("order_id", "has_driver_change")
    
    # Присоединяем информацию о сменах курьеров
    orders_with_driver_changes = orders_enriched \
        .join(driver_changes, "order_id", "left") \
        .withColumn("has_driver_change", coalesce(col("has_driver_change"), lit(0)))
    
    # Агрегируем информацию о сменах курьеров
    driver_changes_agg = orders_with_driver_changes \
        .groupBy("order_year", "order_month", "order_date", "city", "store_id") \
        .agg(sum("has_driver_change").alias("orders_with_driver_change"))
    
    # Объединяем все метрики
    final_orders_datamart = orders_aggregated \
        .join(driver_changes_agg, ["order_year", "order_month", "order_date", "city", "store_id"], "left") \
        .withColumn("orders_with_driver_change", coalesce(col("orders_with_driver_change"), lit(0))) \
        .withColumn("created_at", current_timestamp()) \
        .select(
            "order_year", "order_month", "order_date", "city", "store_id",
            "total_orders", "paid_orders", "delivered_orders", "canceled_orders",
            "canceled_after_delivery", "service_error_cancels", "unique_customers",
            "turnover", "revenue", "profit", "avg_check", "orders_per_customer",
            "revenue_per_customer", "orders_with_driver_change", "active_drivers",
            "day_of_week", "created_at"
        )
    
    print(f"Витрина заказов построена. Количество строк: {final_orders_datamart.count()}")
    
    return final_orders_datamart

def build_products_datamart(spark):
    """Построение витрины товаров."""
    print("Начинаем построение витрины товаров...")
    
    # Чтение необходимых таблиц
    orders_df = read_from_postgres(spark, "orders")
    order_items_df = read_from_postgres(spark, "order_items")
    items_df = read_from_postgres(spark, "items")
    stores_df = read_from_postgres(spark, "stores")
    
    # Преобразование типов данных
    orders_df = orders_df \
        .withColumn("created_at", to_timestamp(col("created_at")))
    
    order_items_df = order_items_df \
        .withColumn("item_price", col("item_price").cast("double")) \
        .withColumn("item_discount", col("item_discount").cast("double")) \
        .withColumn("item_quantity", col("item_quantity").cast("integer")) \
        .withColumn("item_canceled_quantity", col("item_canceled_quantity").cast("integer"))
    
    items_df = items_df.withColumnRenamed("item_category", "category")
    
    # Извлечение города из адреса магазина
    stores_df = stores_df.withColumn(
        "city",
        when(col("store_address").contains("Москва"), "Москва")
        .when(col("store_address").contains("Санкт-Петербург"), "Санкт-Петербург")
        .when(col("store_address").contains("Казань"), "Казань")
        .otherwise("Неизвестно")
    )
    
    # Объединяем все данные
    product_sales = order_items_df \
        .filter(col("item_replaced_id").isNull()) \
        .join(items_df, "item_id") \
        .join(orders_df.select("order_id", "created_at", "store_id"), "order_id") \
        .join(stores_df.select("store_id", "city"), "store_id") \
        .withColumn("sale_date", to_date(col("created_at"))) \
        .withColumn("sale_year", year(col("created_at"))) \
        .withColumn("sale_month", month(col("created_at"))) \
        .withColumn("item_turnover", 
                   (col("item_price") * col("item_quantity")) * 
                   (1 - coalesce(col("item_discount"), lit(0)) / 100.0)) \
        .withColumn("has_canceled_items", 
                   when(col("item_canceled_quantity") > 0, 1).otherwise(0))
    
    # Агрегация по дням, магазинам и товарам
    daily_product_metrics = product_sales \
        .groupBy("sale_year", "sale_month", "sale_date", "city", 
                "store_id", "item_id", "item_title", "category") \
        .agg(
            sum("item_quantity").alias("total_ordered_quantity"),
            sum("item_canceled_quantity").alias("total_canceled_quantity"),
            countDistinct("order_id").alias("orders_with_item"),
            countDistinct(when(col("has_canceled_items") == 1, col("order_id"))).alias("orders_with_canceled_items"),
            sum("item_turnover").alias("item_turnover")
        ) \
        .withColumn("cancel_rate", 
                   when(col("total_ordered_quantity") == 0, 0)
                   .otherwise(col("total_canceled_quantity") / col("total_ordered_quantity")))
    
    # Определяем окна для ранжирования
    daily_window = Window.partitionBy("sale_date", "store_id") \
        .orderBy(col("total_ordered_quantity").desc())
    
    monthly_window = Window.partitionBy("sale_year", "sale_month", "store_id") \
        .orderBy(col("total_ordered_quantity").desc())
    
    # Ранжирование товаров
    ranked_products = daily_product_metrics \
        .withColumn("daily_rank", row_number().over(daily_window)) \
        .withColumn("max_daily_rank", max(col("daily_rank")).over(
            Window.partitionBy("sale_date", "store_id"))) \
        .withColumn("monthly_rank", row_number().over(monthly_window)) \
        .withColumn("max_monthly_rank", max(col("monthly_rank")).over(
            Window.partitionBy("sale_year", "sale_month", "store_id")))
    
    # Создаем финальную витрину
    final_products_datamart = ranked_products \
        .withColumn("is_daily_top_product", when(col("daily_rank") == 1, 1).otherwise(0)) \
        .withColumn("is_daily_worst_product", 
                   when(col("daily_rank") == col("max_daily_rank"), 1).otherwise(0)) \
        .withColumn("is_monthly_top_product", when(col("monthly_rank") == 1, 1).otherwise(0)) \
        .withColumn("is_monthly_worst_product",
                   when(col("monthly_rank") == col("max_monthly_rank"), 1).otherwise(0)) \
        .withColumn("created_at", current_timestamp()) \
        .select(
            "sale_year", "sale_month", "sale_date", "city", "store_id",
            "item_id", "item_title", "category",
            "total_ordered_quantity", "total_canceled_quantity",
            "orders_with_item", "orders_with_canceled_items",
            "item_turnover", "cancel_rate",
            "is_daily_top_product", "is_daily_worst_product",
            "is_monthly_top_product", "is_monthly_worst_product",
            "created_at"
        ) \
        .orderBy("sale_date", "store_id", col("total_ordered_quantity").desc())
    
    print(f"Витрина товаров построена. Количество строк: {final_products_datamart.count()}")
    
    return final_products_datamart

def main():
    """Основная функция для построения витрин."""
    print("Запуск построения витрин данных...")
    
    # Создаем Spark сессию
    spark = create_spark_session()
    
    try:
        # Создаем схему datamart если её нет
        create_datamart_schema(spark)
        
        # Строим витрину заказов
        orders_datamart = build_orders_datamart(spark)
        
        # Строим витрину товаров
        products_datamart = build_products_datamart(spark)
        
        # Записываем витрины в PostgreSQL
        print("Записываем витрину заказов в PostgreSQL...")
        write_to_postgres(orders_datamart, "datamart.orders_daily")
        
        print("Записываем витрину товаров в PostgreSQL...")
        write_to_postgres(products_datamart, "datamart.product_sales_daily")
        
        print("Построение витрин успешно завершено!")
        
        # Показываем статистику
        print("\n=== Статистика витрин ===")
        print(f"Витрина заказов: {orders_datamart.count()} строк")
        orders_datamart.show(5, truncate=False)
        
        print(f"\nВитрина товаров: {products_datamart.count()} строк")
        products_datamart.show(5, truncate=False)
        
    except Exception as e:
        print(f"Ошибка при построении витрин: {str(e)}")
        raise
    finally:
        # Останавливаем Spark сессию
        spark.stop()
        print("Spark сессия остановлена")

if __name__ == "__main__":
    main()
