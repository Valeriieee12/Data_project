-- Создание staging схемы и таблицы для сырых данных
CREATE SCHEMA IF NOT EXISTS staging;

-- Таблица для хранения сырых данных из Parquet
CREATE TABLE IF NOT EXISTS staging.deliveries_raw (
    order_id BIGINT,
    user_id BIGINT,
    user_phone VARCHAR(50),
    address_text TEXT,
    created_at TIMESTAMP,
    paid_at TIMESTAMP,
    delivery_started_at TIMESTAMP,
    delivered_at TIMESTAMP,
    canceled_at TIMESTAMP,
    payment_type VARCHAR(50),
    item_id BIGINT,
    item_title VARCHAR(255),
    item_category VARCHAR(100),
    item_quantity INTEGER,
    item_price DECIMAL(10, 2),
    item_canceled_quantity INTEGER,
    item_replaced_id BIGINT,
    order_discount DECIMAL(5, 2),
    item_discount DECIMAL(5, 2),
    order_cancellation_reason VARCHAR(255),
    driver_id BIGINT,
    driver_phone VARCHAR(50),
    delivery_cost DECIMAL(10, 2),
    store_id BIGINT,
    store_address TEXT
);

-- Индекс для ускорения загрузки
CREATE INDEX IF NOT EXISTS idx_staging_order_id ON staging.deliveries_raw(order_id);
CREATE INDEX IF NOT EXISTS idx_staging_user_id ON staging.deliveries_raw(user_id);
CREATE INDEX IF NOT EXISTS idx_staging_item_id ON staging.deliveries_raw(item_id);

-- Комментарии
COMMENT ON SCHEMA staging IS 'Схема для временного хранения сырых данных';
COMMENT ON TABLE staging.deliveries_raw IS 'Сырые данные доставок из Parquet файла';
