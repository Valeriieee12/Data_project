-- НОРМАЛИЗОВАННЫЕ ТАБЛИЦЫ (3НФ)

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

-- 5. Таблица заказов (основная сущность)
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

-- 7. Таблица истории доставки (для смен курьеров)
CREATE TABLE normalized.delivery_history (
    delivery_id BIGSERIAL PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES normalized.orders(order_id) ON DELETE CASCADE,
    driver_id BIGINT NOT NULL REFERENCES normalized.drivers(driver_id),
    assigned_at TIMESTAMP NOT NULL,
    removed_at TIMESTAMP,
    is_final_driver BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для оптимизации
CREATE INDEX idx_orders_user_id ON normalized.orders(user_id);
CREATE INDEX idx_orders_store_id ON normalized.orders(store_id);
CREATE INDEX idx_orders_created_at ON normalized.orders(created_at);
CREATE INDEX idx_order_items_order_id ON normalized.order_items(order_id);
CREATE INDEX idx_order_items_item_id ON normalized.order_items(item_id);
CREATE INDEX idx_delivery_history_order_id ON normalized.delivery_history(order_id);
CREATE INDEX idx_delivery_history_driver_id ON normalized.delivery_history(driver_id);
