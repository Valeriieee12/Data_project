-- ВИТРИНЫ ДАННЫХ ДЛЯ АНАЛИТИКИ

-- Витрина 1: Агрегированные данные по заказам (ежедневная)
CREATE TABLE datamart.orders_daily (
    report_date DATE NOT NULL,
    city VARCHAR(100),
    store_id BIGINT,
    
    -- Метрики
    turnover DECIMAL(15,2),           -- Оборот (сумма заказов со скидками)
    revenue DECIMAL(15,2),            -- Выручка (оплаченная сумма с учетом замен)
    profit DECIMAL(15,2),             -- Прибыль (revenue - delivery_cost)
    orders_created BIGINT,            -- Количество созданных заказов
    orders_delivered BIGINT,          -- Количество доставленных заказов
    orders_canceled BIGINT,           -- Количество отмененных заказов
    canceled_after_delivery BIGINT,   -- Отмены после доставки
    canceled_service_error BIGINT,    -- Отмены из-за ошибок сервиса
    unique_customers BIGINT,          -- Количество уникальных покупателей
    avg_order_value DECIMAL(10,2),    -- Средний чек
    orders_per_customer DECIMAL(10,2),-- Заказов на покупателя
    revenue_per_customer DECIMAL(15,2),-- Выручка на покупателя
    driver_changes_count BIGINT,      -- Заказов со сменой курьера
    active_drivers_count BIGINT,      -- Активных курьеров
    
    -- Технические поля
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (report_date, city, store_id)
);

-- Витрина 2: Агрегированные данные по товарам (ежедневная)
CREATE TABLE datamart.product_sales_daily (
    report_date DATE NOT NULL,
    city VARCHAR(100),
    store_id BIGINT,
    item_category VARCHAR(255),
    item_id BIGINT,
    
    -- Метрики
    turnover DECIMAL(15,2),           -- Оборот товара
    ordered_units BIGINT,             -- Заказанных единиц
    canceled_units BIGINT,            -- Отмененных единиц
    orders_with_item BIGINT,          -- Заказов с товаром
    orders_with_canceled_item BIGINT, -- Заказов с отменой товара
    
    -- Ранги (можно рассчитывать в запросе)
    popularity_rank_daily BIGINT,
    popularity_rank_weekly BIGINT,
    popularity_rank_monthly BIGINT,
    
    -- Технические поля
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
