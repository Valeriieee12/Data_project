from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

class SparkDataProcessor:
    """Класс для обработки данных с помощью PySpark"""
    
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("DeliveryDataProcessor") \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()
        
        self.logger = logging.getLogger(__name__)
    
    def extract_city_from_address(self, address_col):
        """Извлечение города из адреса"""
        # Базовая логика - берем первую часть до запятой
        return split(address_col, ',')[0]
    
    def load_parquet_data(self, file_path):
        """Загрузка данных из Parquet файла"""
        self.logger.info(f"Загрузка данных из: {file_path}")
        df = self.spark.read.parquet(file_path)
        self.logger.info(f"Загружено {df.count()} строк")
        return df
    
    def transform_to_normalized(self, raw_df):
        """Преобразование денормализованных данных в нормализованные"""
        
        # 1. Уникальные пользователи
        users_df = raw_df.select(
            col('user_id').cast('bigint'),
            col('user_phone')
        ).distinct()
        
        # 2. Уникальные магазины
        stores_df = raw_df.select(
            col('store_id').cast('bigint'),
            col('store_address')
        ).distinct()
        
        # 3. Уникальные товары
        items_df = raw_df.select(
            col('item_id').cast('bigint'),
            col('item_title'),
            col('item_category')
        ).distinct()
        
        # 4. Уникальные курьеры
        drivers_df = raw_df.select(
            col('driver_id').cast('bigint'),
            col('driver_phone')
        ).distinct()
        
        # 5. Заказы
        orders_df = raw_df.select(
            col('order_id').cast('bigint'),
            col('user_id').cast('bigint'),
            col('store_id').cast('bigint'),
            col('address_text'),
            col('created_at').cast('timestamp'),
            col('paid_at').cast('timestamp'),
            col('delivery_started_at').cast('timestamp'),
            col('delivered_at').cast('timestamp'),
            col('canceled_at').cast('timestamp'),
            col('payment_type'),
            col('order_discount').cast('decimal(5,2)'),
            col('order_cancellation_reason'),
            col('delivery_cost').cast('decimal(10,2)')
        ).distinct()
        
        # 6. Позиции заказов
        order_items_df = raw_df.select(
            col('order_id').cast('bigint'),
            col('item_id').cast('bigint'),
            col('item_quantity').cast('int'),
            col('item_price').cast('decimal(10,2)'),
            col('item_discount').cast('decimal(5,2)'),
            col('item_canceled_quantity').cast('int'),
            col('item_replaced_id').cast('bigint')
        )
        
        return {
            'users': users_df,
            'stores': stores_df,
            'items': items_df,
            'drivers': drivers_df,
            'orders': orders_df,
            'order_items': order_items_df
        }
    
    def build_datamarts(self, normalized_data):
        """Построение витрин данных"""
        
        orders_df = normalized_data['orders']
        order_items_df = normalized_data['order_items']
        items_df = normalized_data['items']
        stores_df = normalized_data['stores']
        
        # Извлекаем город из адресов
        orders_with_city = orders_df.withColumn(
            'city',
            self.extract_city_from_address(col('address_text'))
        )
        
        stores_with_city = stores_df.withColumn(
            'store_city',
            self.extract_city_from_address(col('store_address'))
        )
        
        # Витрина заказов (ежедневная)
        orders_daily = orders_with_city \
            .withColumn('report_date', to_date(col('created_at'))) \
            .groupBy('report_date', 'city', 'store_id') \
            .agg(
                count('order_id').alias('orders_created'),
                sum(when(col('delivered_at').isNotNull(), 1).otherwise(0)).alias('orders_delivered'),
                sum(when(col('canceled_at').isNotNull(), 1).otherwise(0)).alias('orders_canceled'),
                countDistinct('user_id').alias('unique_customers')
            )
        
        # Витрина товаров (ежедневная)
        product_sales_daily = order_items_df \
            .join(orders_with_city, 'order_id') \
            .join(items_df, 'item_id') \
            .withColumn('report_date', to_date(col('created_at'))) \
            .groupBy('report_date', 'city', 'store_id', 'item_category', 'item_id') \
            .agg(
                sum(col('item_quantity') * col('item_price')).alias('turnover'),
                sum('item_quantity').alias('ordered_units'),
                sum('item_canceled_quantity').alias('canceled_units'),
                countDistinct('order_id').alias('orders_with_item')
            )
        
        return {
            'orders_daily': orders_daily,
            'product_sales_daily': product_sales_daily
        }
    
    def save_to_postgres(self, df, table_name, schema='normalized'):
        """Сохранение DataFrame в PostgreSQL"""
        url = "jdbc:postgresql://postgres:5432/delivery_db"
        properties = {
            "user": "delivery_user",
            "password": "delivery_password",
            "driver": "org.postgresql.Driver"
        }
        
        df.write \
            .mode("overwrite") \
            .jdbc(url=url, table=f"{schema}.{table_name}", properties=properties)
        
        self.logger.info(f"Данные сохранены в {schema}.{table_name}")
    
    def stop(self):
        """Остановка Spark сессии"""
        self.spark.stop()
