-- Инициализационный скрипт для PostgreSQL
-- Создание схем и таблиц для проекта доставок

-- Создание схем
CREATE SCHEMA IF NOT EXISTS normalized;
CREATE SCHEMA IF NOT EXISTS datamart;

-- 1. Таблица пользователей
CREATE TABLE normalized.users (
    user_id BIGINT PRIMARY KEY,
    user_phone VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Таблица магазинов
CREATE TABLE normalized.stores (
    store_id BIGINT PRIMARY KEY,
    store_address TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Таблица товаров
CREATE TABLE normalized.items (
    item_id BIGINT PRIMARY KEY,
    item_title TEXT NOT NULL,
    item_category VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Таблица курьеров
CREATE TABLE normalized.drivers (
    driver_id BIGINT PRIMARY KEY,
    driver_phone VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. Таблица заказов
CREATE TABLE normalized.orders (
    order_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES normalized.users(user_id),
    store_id BIGINT NOT NULL REFERENCES normalized.stores(store_id),
    address_text TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    paid_at TIMESTAMP,
    delivery_started_at TIMESTAMP,
    delivered_at TIMESTAMP,
    canceled_at TIMESTAMP,
    payment_type VARCHAR(50) CHECK (payment_type IN ('Сразу', 'постоплата курьеру')),
    order_discount DECIMAL(5,2) DEFAULT 0.00,
    order_cancellation_reason TEXT,
    delivery_cost DECIMAL(10,2) DEFAULT 0.00,
    created_at_meta TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 6. Таблица позиций в заказе
CREATE TABLE normalized.order_items (
    order_item_id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES normalized.orders(order_id) ON DELETE CASCADE,
    item_id BIGINT NOT NULL REFERENCES normalized.items(item_id),
    item_quantity INTEGER NOT NULL CHECK (item_quantity > 0),
    item_price DECIMAL(10,2) NOT NULL,
    item_discount DECIMAL(5,2) DEFAULT 0.00,
    item_canceled_quantity INTEGER DEFAULT 0 CHECK (item_canceled_quantity >= 0),
    item_replaced_id BIGINT REFERENCES normalized.items(item_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(order_id, item_id)
);

-- 7. Таблица истории доставки
CREATE TABLE normalized.delivery_history (
    delivery_id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES normalized.orders(order_id) ON DELETE CASCADE,
    driver_id BIGINT NOT NULL REFERENCES normalized.drivers(driver_id),
    assigned_at TIMESTAMP NOT NULL,
    removed_at TIMESTAMP,
    is_final_driver BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для нормализованных таблиц
CREATE INDEX idx_orders_user_id ON normalized.orders(user_id);
CREATE INDEX idx_orders_store_id ON normalized.orders(store_id);
CREATE INDEX idx_orders_created_at ON normalized.orders(created_at);
CREATE INDEX idx_order_items_order_id ON normalized.order_items(order_id);
CREATE INDEX idx_order_items_item_id ON normalized.order_items(item_id);
CREATE INDEX idx_delivery_history_order_id ON normalized.delivery_history(order_id);
CREATE INDEX idx_delivery_history_driver_id ON normalized.delivery_history(driver_id);

-- Витрина 1: Агрегированные данные по заказам
CREATE TABLE datamart.orders_daily (
    report_date DATE NOT NULL,
    city VARCHAR(100),
    store_id BIGINT,
    turnover DECIMAL(15,2),
    revenue DECIMAL(15,2),
    profit DECIMAL(15,2),
    orders_created BIGINT,
    orders_delivered BIGINT,
    orders_canceled BIGINT,
    canceled_after_delivery BIGINT,
    canceled_service_error BIGINT,
    unique_customers BIGINT,
    avg_order_value DECIMAL(10,2),
    orders_per_customer DECIMAL(10,2),
    revenue_per_customer DECIMAL(15,2),
    driver_changes_count BIGINT,
    active_drivers_count BIGINT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (report_date, city, store_id)
);

-- Витрина 2: Агрегированные данные по товарам
CREATE TABLE datamart.product_sales_daily (
    report_date DATE NOT NULL,
    city VARCHAR(100),
    store_id BIGINT,
    item_category VARCHAR(255),
    item_id BIGINT,
    turnover DECIMAL(15,2),
    ordered_units BIGINT,
    canceled_units BIGINT,
    orders_with_item BIGINT,
    orders_with_canceled_item BIGINT,
    popularity_rank_daily BIGINT,
    popularity_rank_weekly BIGINT,
    popularity_rank_monthly BIGINT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (report_date, city, store_id, item_category, item_id)
);

-- Индексы для витрин
CREATE INDEX idx_orders_daily_date ON datamart.orders_daily(report_date);
CREATE INDEX idx_orders_daily_city ON datamart.orders_daily(city);
CREATE INDEX idx_orders_daily_store ON datamart.orders_daily(store_id);
CREATE INDEX idx_product_daily_date ON datamart.product_sales_daily(report_date);
CREATE INDEX idx_product_daily_city ON datamart.product_sales_daily(city);
CREATE INDEX idx_product_daily_store ON datamart.product_sales_daily(store_id);
CREATE INDEX idx_product_daily_category ON datamart.product_sales_daily(item_category);
CREATE INDEX idx_product_daily_item ON datamart.product_sales_daily(item_id);

-- Комментарии к таблицам
COMMENT ON SCHEMA normalized IS 'Нормализованные данные доставок (3НФ)';
COMMENT ON SCHEMA datamart IS 'Витрины данных для аналитики';

COMMENT ON TABLE normalized.orders IS 'Основная таблица заказов';
COMMENT ON TABLE datamart.orders_daily IS 'Ежедневная витрина по заказам с агрегацией по городу и магазину';
COMMENT ON TABLE datamart.product_sales_daily IS 'Ежедневная витрина по товарам с агрегацией по категориям';

-- Создание пользователя для доступа (если нужно)
-- CREATE USER delivery_user WITH PASSWORD 'delivery_password';
-- GRANT ALL PRIVILEGES ON SCHEMA normalized TO delivery_user;
-- GRANT ALL PRIVILEGES ON SCHEMA datamart TO delivery_user;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA normalized TO delivery_user;
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA datamart TO delivery_user;

-- Staging схема и таблицы
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

-- Индексы для staging
CREATE INDEX IF NOT EXISTS idx_staging_order_id ON staging.deliveries_raw(order_id);
CREATE INDEX IF NOT EXISTS idx_staging_user_id ON staging.deliveries_raw(user_id);
CREATE INDEX IF NOT EXISTS idx_staging_item_id ON staging.deliveries_raw(item_id);

-- ============================================
-- СОЗДАНИЕ СХЕМЫ И ТАБЛИЦ ДЛЯ ВИТРИН ДАННЫХ
-- ============================================

-- Создаем схему для витрин
CREATE SCHEMA IF NOT EXISTS datamart;

-- Витрина заказов (агрегация по дням и магазинам)
DROP TABLE IF EXISTS datamart.orders_daily;
CREATE TABLE datamart.orders_daily (
    order_year INTEGER,
    order_month INTEGER,
    order_date DATE,
    city VARCHAR(50),
    store_id INTEGER,
    total_orders INTEGER,
    paid_orders INTEGER,
    delivered_orders INTEGER,
    canceled_orders INTEGER,
    canceled_after_delivery INTEGER,
    service_error_cancels INTEGER,
    unique_customers INTEGER,
    turnover DECIMAL(15, 2),
    revenue DECIMAL(15, 2),
    profit DECIMAL(15, 2),
    avg_check DECIMAL(15, 2),
    orders_per_customer DECIMAL(10, 2),
    revenue_per_customer DECIMAL(15, 2),
    orders_with_driver_change INTEGER,
    active_drivers INTEGER,
    day_of_week VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (order_date, store_id)
);

COMMENT ON TABLE datamart.orders_daily IS 'Ежедневная витрина метрик по заказам';
COMMENT ON COLUMN datamart.orders_daily.order_year IS 'Год заказа';
COMMENT ON COLUMN datamart.orders_daily.order_month IS 'Месяц заказа';
COMMENT ON COLUMN datamart.orders_daily.order_date IS 'Дата заказа';
COMMENT ON COLUMN datamart.orders_daily.city IS 'Город магазина';
COMMENT ON COLUMN datamart.orders_daily.store_id IS 'ID магазина';
COMMENT ON COLUMN datamart.orders_daily.total_orders IS 'Всего заказов';
COMMENT ON COLUMN datamart.orders_daily.paid_orders IS 'Оплаченных заказов';
COMMENT ON COLUMN datamart.orders_daily.delivered_orders IS 'Доставленных заказов';
COMMENT ON COLUMN datamart.orders_daily.canceled_orders IS 'Отмененных заказов';
COMMENT ON COLUMN datamart.orders_daily.canceled_after_delivery IS 'Отмен после доставки';
COMMENT ON COLUMN datamart.orders_daily.service_error_cancels IS 'Отмен из-за ошибок сервиса';
COMMENT ON COLUMN datamart.orders_daily.unique_customers IS 'Уникальных покупателей';
COMMENT ON COLUMN datamart.orders_daily.turnover IS 'Оборот';
COMMENT ON COLUMN datamart.orders_daily.revenue IS 'Выручка';
COMMENT ON COLUMN datamart.orders_daily.profit IS 'Прибыль';
COMMENT ON COLUMN datamart.orders_daily.avg_check IS 'Средний чек';
COMMENT ON COLUMN datamart.orders_daily.orders_per_customer IS 'Заказов на покупателя';
COMMENT ON COLUMN datamart.orders_daily.revenue_per_customer IS 'Выручка на покупателя';
COMMENT ON COLUMN datamart.orders_daily.orders_with_driver_change IS 'Заказов со сменой курьера';
COMMENT ON COLUMN datamart.orders_daily.active_drivers IS 'Активных курьеров';
COMMENT ON COLUMN datamart.orders_daily.day_of_week IS 'День недели';
COMMENT ON COLUMN datamart.orders_daily.created_at IS 'Время создания записи';

-- Витрина товаров (агрегация по дням, магазинам и товарам)
DROP TABLE IF EXISTS datamart.product_sales_daily;
CREATE TABLE datamart.product_sales_daily (
    sale_year INTEGER,
    sale_month INTEGER,
    sale_date DATE,
    city VARCHAR(50),
    store_id INTEGER,
    item_id INTEGER,
    item_title VARCHAR(255),
    category VARCHAR(100),
    total_ordered_quantity INTEGER,
    total_canceled_quantity INTEGER,
    orders_with_item INTEGER,
    orders_with_canceled_items INTEGER,
    item_turnover DECIMAL(15, 2),
    cancel_rate DECIMAL(5, 4),
    is_daily_top_product INTEGER,
    is_daily_worst_product INTEGER,
    is_monthly_top_product INTEGER,
    is_monthly_worst_product INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (sale_date, store_id, item_id)
);

COMMENT ON TABLE datamart.product_sales_daily IS 'Ежедневная витрина метрик по товарам';
COMMENT ON COLUMN datamart.product_sales_daily.sale_year IS 'Год продажи';
COMMENT ON COLUMN datamart.product_sales_daily.sale_month IS 'Месяц продажи';
COMMENT ON COLUMN datamart.product_sales_daily.sale_date IS 'Дата продажи';
COMMENT ON COLUMN datamart.product_sales_daily.city IS 'Город магазина';
COMMENT ON COLUMN datamart.product_sales_daily.store_id IS 'ID магазина';
COMMENT ON COLUMN datamart.product_sales_daily.item_id IS 'ID товара';
COMMENT ON COLUMN datamart.product_sales_daily.item_title IS 'Название товара';
COMMENT ON COLUMN datamart.product_sales_daily.category IS 'Категория товара';
COMMENT ON COLUMN datamart.product_sales_daily.total_ordered_quantity IS 'Всего заказанных единиц';
COMMENT ON COLUMN datamart.product_sales_daily.total_canceled_quantity IS 'Всего отмененных единиц';
COMMENT ON COLUMN datamart.product_sales_daily.orders_with_item IS 'Заказов с товаром';
COMMENT ON COLUMN datamart.product_sales_daily.orders_with_canceled_items IS 'Заказов с отменой товара';
COMMENT ON COLUMN datamart.product_sales_daily.item_turnover IS 'Оборот товара';
COMMENT ON COLUMN datamart.product_sales_daily.cancel_rate IS 'Процент отмен';
COMMENT ON COLUMN datamart.product_sales_daily.is_daily_top_product IS 'Флаг топ товара дня';
COMMENT ON COLUMN datamart.product_sales_daily.is_daily_worst_product IS 'Флаг худшего товара дня';
COMMENT ON COLUMN datamart.product_sales_daily.is_monthly_top_product IS 'Флаг топ товара месяца';
COMMENT ON COLUMN datamart.product_sales_daily.is_monthly_worst_product IS 'Флаг худшего товара месяца';
COMMENT ON COLUMN datamart.product_sales_daily.created_at IS 'Время создания записи';

-- Создаем индексы для оптимизации запросов
CREATE INDEX idx_orders_daily_date ON datamart.orders_daily(order_date);
CREATE INDEX idx_orders_daily_store ON datamart.orders_daily(store_id);
CREATE INDEX idx_orders_daily_city ON datamart.orders_daily(city);

CREATE INDEX idx_product_sales_date ON datamart.product_sales_daily(sale_date);
CREATE INDEX idx_product_sales_store ON datamart.product_sales_daily(store_id);
CREATE INDEX idx_product_sales_item ON datamart.product_sales_daily(item_id);
CREATE INDEX idx_product_sales_category ON datamart.product_sales_daily(category);

-- Представления для удобства использования аналитиками

-- Представление для месячной агрегации заказов
CREATE OR REPLACE VIEW datamart.orders_monthly AS
SELECT 
    order_year,
    order_month,
    city,
    store_id,
    SUM(total_orders) AS total_orders,
    SUM(paid_orders) AS paid_orders,
    SUM(delivered_orders) AS delivered_orders,
    SUM(canceled_orders) AS canceled_orders,
    SUM(canceled_after_delivery) AS canceled_after_delivery,
    SUM(service_error_cancels) AS service_error_cancels,
    AVG(unique_customers) AS avg_unique_customers,
    SUM(turnover) AS turnover,
    SUM(revenue) AS revenue,
    SUM(profit) AS profit,
    AVG(avg_check) AS avg_check,
    AVG(orders_per_customer) AS orders_per_customer,
    AVG(revenue_per_customer) AS revenue_per_customer,
    SUM(orders_with_driver_change) AS orders_with_driver_change,
    AVG(active_drivers) AS avg_active_drivers
FROM datamart.orders_daily
GROUP BY order_year, order_month, city, store_id
ORDER BY order_year, order_month, city, store_id;

COMMENT ON VIEW datamart.orders_monthly IS 'Месячная агрегация метрик по заказам';

-- Представление для месячной агрегации товаров
CREATE OR REPLACE VIEW datamart.product_sales_monthly AS
SELECT 
    sale_year,
    sale_month,
    city,
    store_id,
    item_id,
    item_title,
    category,
    SUM(total_ordered_quantity) AS total_ordered_quantity,
    SUM(total_canceled_quantity) AS total_canceled_quantity,
    SUM(orders_with_item) AS orders_with_item,
    SUM(orders_with_canceled_items) AS orders_with_canceled_items,
    SUM(item_turnover) AS item_turnover,
    AVG(cancel_rate) AS avg_cancel_rate,
    MAX(is_monthly_top_product) AS is_monthly_top_product,
    MAX(is_monthly_worst_product) AS is_monthly_worst_product
FROM datamart.product_sales_daily
GROUP BY sale_year, sale_month, city, store_id, item_id, item_title, category
ORDER BY sale_year, sale_month, city, store_id, total_ordered_quantity DESC;

COMMENT ON VIEW datamart.product_sales_monthly IS 'Месячная агрегация метрик по товарам';

-- Представление для топ товаров по месяцам
CREATE OR REPLACE VIEW datamart.top_products_monthly AS
SELECT 
    sale_year,
    sale_month,
    city,
    store_id,
    item_id,
    item_title,
    category,
    total_ordered_quantity,
    item_turnover,
    ROW_NUMBER() OVER (PARTITION BY sale_year, sale_month, city, store_id ORDER BY total_ordered_quantity DESC) AS rank_by_quantity,
    ROW_NUMBER() OVER (PARTITION BY sale_year, sale_month, city, store_id ORDER BY item_turnover DESC) AS rank_by_turnover
FROM datamart.product_sales_monthly
WHERE is_monthly_top_product = 1 OR total_ordered_quantity > 0;

COMMENT ON VIEW datamart.top_products_monthly IS 'Топ товары по месяцам с ранжированием';
