"""
Скрипт для трансформации данных из staging в нормализованные таблицы
"""
import sys
import logging
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
import os

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_db_connection():
    """Получение подключения к PostgreSQL"""
    conn = psycopg2.connect(
        host="postgres",
        port="5432",
        database="delivery_db",
        user="delivery_user",
        password="delivery_password"
    )
    return conn

def transform_to_normalized():
    """
    Основная функция трансформации данных
    Заполняет все нормализованные таблицы из staging.deliveries_raw
    """
    logger.info("=== НАЧАЛО ТРАНСФОРМАЦИИ ДАННЫХ В НОРМАЛИЗОВАННЫЕ ТАБЛИЦЫ ===")
    
    conn = None
    cursor = None
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Шаг 1: Сначала создадим все необходимые индексы для ускорения
        logger.info("1. Создание индексов для оптимизации...")
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_raw_user_id ON staging.deliveries_raw(user_id);
            CREATE INDEX IF NOT EXISTS idx_raw_store_id ON staging.deliveries_raw(store_id);
            CREATE INDEX IF NOT EXISTS idx_raw_item_id ON staging.deliveries_raw(item_id);
            CREATE INDEX IF NOT EXISTS idx_raw_driver_id ON staging.deliveries_raw(driver_id);
            CREATE INDEX IF NOT EXISTS idx_raw_created_at ON staging.deliveries_raw(created_at);
        """)
        conn.commit()
        logger.info("✓ Индексы созданы")
        
        # Шаг 2: Проверяем количество данных в staging
        cursor.execute("SELECT COUNT(*) as cnt FROM staging.deliveries_raw")
        staging_count = cursor.fetchone()[0]
        logger.info(f"2. В staging таблице {staging_count:,} строк")
        
        # Шаг 3: Заполняем таблицу users
        logger.info("3. Заполняем таблицу users...")
        cursor.execute("""
            INSERT INTO normalized.users (user_id, user_phone)
            SELECT DISTINCT user_id, user_phone 
            FROM staging.deliveries_raw
            WHERE user_id IS NOT NULL AND user_phone IS NOT NULL
            ON CONFLICT (user_id) DO UPDATE SET
                user_phone = EXCLUDED.user_phone;
        """)
        conn.commit()
        cursor.execute("SELECT COUNT(*) FROM normalized.users")
        users_count = cursor.fetchone()[0]
        logger.info(f"✓ Таблица users заполнена: {users_count:,} пользователей")
        
        # Шаг 4: Заполняем таблицу stores
        logger.info("4. Заполняем таблицу stores...")
        cursor.execute("""
            INSERT INTO normalized.stores (store_id, store_address)
            SELECT DISTINCT 
                store_id, 
                store_address
            FROM staging.deliveries_raw
            WHERE store_id IS NOT NULL AND store_address IS NOT NULL
            ON CONFLICT (store_id) DO UPDATE SET
                store_address = EXCLUDED.store_address;
        """)
        conn.commit()
        cursor.execute("SELECT COUNT(*) FROM normalized.stores")
        stores_count = cursor.fetchone()[0]
        logger.info(f"✓ Таблица stores заполнена: {stores_count:,} магазинов")
        
        # Шаг 5: Заполняем таблицу items
        logger.info("5. Заполняем таблицу items...")
        cursor.execute("""
            INSERT INTO normalized.items (item_id, item_title, item_category)
            SELECT DISTINCT 
                item_id, 
                item_title,
                item_category
            FROM staging.deliveries_raw
            WHERE item_id IS NOT NULL AND item_title IS NOT NULL
            ON CONFLICT (item_id) DO UPDATE SET
                item_title = EXCLUDED.item_title,
                item_category = EXCLUDED.item_category;
        """)
        conn.commit()
        cursor.execute("SELECT COUNT(*) FROM normalized.items")
        items_count = cursor.fetchone()[0]
        logger.info(f"✓ Таблица items заполнена: {items_count:,} товаров")
        
        # Шаг 6: Заполняем таблицу drivers
        logger.info("6. Заполняем таблицу drivers...")
        cursor.execute("""
            INSERT INTO normalized.drivers (driver_id, driver_phone)
            SELECT DISTINCT driver_id, driver_phone
            FROM staging.deliveries_raw
            WHERE driver_id IS NOT NULL AND driver_phone IS NOT NULL
            ON CONFLICT (driver_id) DO UPDATE SET
                driver_phone = EXCLUDED.driver_phone;
        """)
        conn.commit()
        cursor.execute("SELECT COUNT(*) FROM normalized.drivers")
        drivers_count = cursor.fetchone()[0]
        logger.info(f"✓ Таблица drivers заполнена: {drivers_count:,} курьеров")
        
        # Шаг 7: Заполняем таблицу orders
        logger.info("7. Заполняем таблицу orders...")
        cursor.execute("""
            INSERT INTO normalized.orders (
                order_id, user_id, store_id, address_text,
                created_at, paid_at, delivery_started_at, 
                delivered_at, canceled_at, payment_type,
                order_discount, order_cancellation_reason, 
                delivery_cost
            )
            SELECT DISTINCT ON (order_id)
                order_id,
                user_id,
                store_id,
                address_text,
                created_at,
                paid_at,
                delivery_started_at,
                delivered_at,
                canceled_at,
                CASE 
                    WHEN LOWER(payment_type) LIKE '%сразу%' THEN 'Сразу'
                    WHEN LOWER(payment_type) LIKE '%постоплат%' THEN 'постоплата курьеру'
                    ELSE 'постоплата курьеру'
                END as payment_type,
                COALESCE(order_discount, 0) as order_discount,
                order_cancellation_reason,
                COALESCE(delivery_cost, 0) as delivery_cost
            FROM staging.deliveries_raw
            WHERE order_id IS NOT NULL AND user_id IS NOT NULL AND store_id IS NOT NULL
            ORDER BY order_id, created_at DESC
            ON CONFLICT (order_id) DO UPDATE SET
                paid_at = EXCLUDED.paid_at,
                delivery_started_at = EXCLUDED.delivery_started_at,
                delivered_at = EXCLUDED.delivered_at,
                canceled_at = EXCLUDED.canceled_at;
        """)
        conn.commit()
        cursor.execute("SELECT COUNT(*) FROM normalized.orders")
        orders_count = cursor.fetchone()[0]
        logger.info(f"✓ Таблица orders заполнена: {orders_count:,} заказов")
        
        # Шаг 8: Заполняем таблицу order_items
        logger.info("8. Заполняем таблицу order_items...")
        # Сначала очистим таблицу
        cursor.execute("TRUNCATE TABLE normalized.order_items RESTART IDENTITY;")
        
        # Затем вставим данные с агрегацией
        cursor.execute("""
            INSERT INTO normalized.order_items (
                order_id, item_id, item_quantity, item_canceled_quantity,
                item_price, item_discount, item_replaced_id
            )
            SELECT 
                order_id,
                item_id,
                SUM(COALESCE(item_quantity, 0)) as item_quantity,
                SUM(COALESCE(item_canceled_quantity, 0)) as item_canceled_quantity,
                AVG(COALESCE(item_price, 0)) as item_price,
                AVG(COALESCE(item_discount, 0)) as item_discount,
                MAX(item_replaced_id) as item_replaced_id
            FROM staging.deliveries_raw
            WHERE order_id IS NOT NULL AND item_id IS NOT NULL
            GROUP BY order_id, item_id;
        """)
        conn.commit()
        cursor.execute("SELECT COUNT(*) FROM normalized.order_items")
        order_items_count = cursor.fetchone()[0]
        logger.info(f"✓ Таблица order_items заполнена: {order_items_count:,} позиций в заказах")
        
        # Шаг 9: Заполняем таблицу delivery_history (ИСПРАВЛЕНО - без ON CONFLICT)
        logger.info("9. Заполняем таблицу delivery_history...")
        # Сначала очистим таблицу
        cursor.execute("TRUNCATE TABLE normalized.delivery_history RESTART IDENTITY;")
        
        # Затем вставим данные
        cursor.execute("""
            INSERT INTO normalized.delivery_history (
                order_id, driver_id, assigned_at, removed_at, is_final_driver
            )
            WITH delivery_data AS (
                SELECT 
                    order_id,
                    driver_id,
                    COALESCE(delivery_started_at, created_at) as assigned_at,
                    COALESCE(delivered_at, canceled_at) as removed_at,
                    CASE 
                        WHEN delivered_at IS NOT NULL AND driver_id IS NOT NULL THEN TRUE
                        ELSE FALSE
                    END as is_final
                FROM staging.deliveries_raw
                WHERE driver_id IS NOT NULL AND order_id IS NOT NULL
            )
            SELECT 
                order_id,
                driver_id,
                MIN(assigned_at) as assigned_at,
                MAX(removed_at) as removed_at,
                BOOL_OR(is_final) as is_final_driver
            FROM delivery_data
            GROUP BY order_id, driver_id;
        """)
        conn.commit()
        cursor.execute("SELECT COUNT(*) FROM normalized.delivery_history")
        delivery_history_count = cursor.fetchone()[0]
        logger.info(f"✓ Таблица delivery_history заполнена: {delivery_history_count:,} записей")
        
        # Шаг 10: Проверяем целостность данных
        logger.info("10. Проверяем целостность данных...")
        cursor.execute("""
            SELECT 
                'users' as table_name, COUNT(*) as count FROM normalized.users
            UNION ALL
            SELECT 'stores', COUNT(*) FROM normalized.stores
            UNION ALL
            SELECT 'items', COUNT(*) FROM normalized.items
            UNION ALL
            SELECT 'drivers', COUNT(*) FROM normalized.drivers
            UNION ALL
            SELECT 'orders', COUNT(*) FROM normalized.orders
            UNION ALL
            SELECT 'order_items', COUNT(*) FROM normalized.order_items
            UNION ALL
            SELECT 'delivery_history', COUNT(*) FROM normalized.delivery_history;
        """)
        results = cursor.fetchall()
        
        logger.info("✓ Сводка по всем таблицам:")
        for row in results:
            logger.info(f"  - {row[0]}: {row[1]:,}")
        
        # Шаг 11: Пример данных для проверки
        logger.info("11. Пример данных для проверки...")
        cursor.execute("""
            SELECT 
                o.order_id,
                u.user_phone,
                s.store_address,
                o.created_at,
                o.payment_type,
                o.delivery_cost,
                COUNT(DISTINCT oi.item_id) as items_count,
                SUM(oi.item_quantity) as total_items,
                SUM(oi.item_price * oi.item_quantity) as order_total
            FROM normalized.orders o
            JOIN normalized.users u ON o.user_id = u.user_id
            JOIN normalized.stores s ON o.store_id = s.store_id
            LEFT JOIN normalized.order_items oi ON o.order_id = oi.order_id
            GROUP BY o.order_id, u.user_phone, s.store_address, o.created_at, o.payment_type, o.delivery_cost
            ORDER BY o.created_at DESC
            LIMIT 5;
        """)
        sample_data = cursor.fetchall()
        logger.info("✓ Пример заказов (последние 5):")
        for i, row in enumerate(sample_data, 1):
            logger.info(f"  {i}. Заказ {row[0]}, тел: {row[1]}, магазин: {row[2][:30]}..., дата: {row[3]}, оплата: {row[4]}, доставка: {row[5]}, позиций: {row[6]}, товаров: {row[7]}, сумма: {row[8]:.2f}")
        
        # Шаг 12: Очистка временных индексов
        logger.info("12. Очистка временных индексов...")
        cursor.execute("""
            DROP INDEX IF EXISTS idx_raw_user_id;
            DROP INDEX IF EXISTS idx_raw_store_id;
            DROP INDEX IF EXISTS idx_raw_item_id;
            DROP INDEX IF EXISTS idx_raw_driver_id;
            DROP INDEX IF EXISTS idx_raw_created_at;
        """)
        conn.commit()
        logger.info("✓ Временные индексы удалены")
        
        logger.info("=== ТРАНСФОРМАЦИЯ ДАННЫХ УСПЕШНО ЗАВЕРШЕНА ===")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Ошибка при трансформации данных: {e}")
        import traceback
        logger.error(traceback.format_exc())
        if conn:
            conn.rollback()
        return False
        
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    success = transform_to_normalized()
    sys.exit(0 if success else 1)
