-- Витрина заказов (агрегация по дням и магазинам)
WITH order_calculations AS (
    SELECT 
        o.order_id,
        o.user_id,
        o.store_id,
        DATE(o.created_at) AS order_date,
        EXTRACT(YEAR FROM o.created_at) AS order_year,
        EXTRACT(MONTH FROM o.created_at) AS order_month,
        o.created_at,
        o.paid_at,
        o.delivered_at,
        o.canceled_at,
        s.store_address,
        -- Извлекаем город из адреса магазина
        CASE 
            WHEN s.store_address LIKE '%Москва%' THEN 'Москва'
            WHEN s.store_address LIKE '%Санкт-Петербург%' THEN 'Санкт-Петербург'
            WHEN s.store_address LIKE '%Казань%' THEN 'Казань'
            ELSE 'Неизвестно'
        END AS city,
        
        -- Расчеты для заказа
        -- Оборот (сумма всех товаров с учетом скидок на товар)
        SUM(
            (oi.item_price * oi.item_quantity) * 
            (1 - COALESCE(oi.item_discount, 0) / 100.0)
        ) AS order_revenue_before_order_discount,
        
        -- Выручка (с учетом скидки на заказ и замен товаров)
        CASE 
            WHEN o.paid_at IS NOT NULL THEN
                (SUM(
                    (oi.item_price * oi.item_quantity) * 
                    (1 - COALESCE(oi.item_discount, 0) / 100.0)
                ) * (1 - COALESCE(o.order_discount, 0) / 100.0)) 
                + COALESCE(o.delivery_cost, 0)
            ELSE 0
        END AS order_revenue,
        
        -- Прибыль (выручка - расходы, упрощенно считаем расходы как 70% от выручки)
        CASE 
            WHEN o.paid_at IS NOT NULL THEN
                ((SUM(
                    (oi.item_price * oi.item_quantity) * 
                    (1 - COALESCE(oi.item_discount, 0) / 100.0)
                ) * (1 - COALESCE(o.order_discount, 0) / 100.0)) 
                + COALESCE(o.delivery_cost, 0)) * 0.3  -- 30% прибыль
            ELSE 0
        END AS order_profit,
        
        -- Флаги для агрегаций
        CASE WHEN o.canceled_at IS NOT NULL THEN 1 ELSE 0 END AS is_canceled,
        CASE WHEN o.delivered_at IS NOT NULL THEN 1 ELSE 0 END AS is_delivered,
        CASE WHEN o.paid_at IS NOT NULL THEN 1 ELSE 0 END AS is_paid,
        
        -- Отмена после доставки (если canceled_at > delivered_at)
        CASE 
            WHEN o.canceled_at IS NOT NULL 
                 AND o.delivered_at IS NOT NULL 
                 AND o.canceled_at > o.delivered_at 
            THEN 1 
            ELSE 0 
        END AS is_canceled_after_delivery,
        
        -- Отмена из-за ошибок сервиса
        CASE 
            WHEN o.order_cancellation_reason IN ('Ошибка приложения', 'Проблемы с оплатой') 
            THEN 1 
            ELSE 0 
        END AS is_service_error_cancel,
        
        -- Смена курьеров (если в истории доставки больше 1 записи для заказа)
        (SELECT COUNT(*) > 1 FROM delivery_history dh WHERE dh.order_id = o.order_id) AS has_driver_change,
        
        -- Активные курьеры в этот день (по заказам)
        COUNT(DISTINCT o.driver_id) AS active_drivers_count
        
    FROM orders o
    LEFT JOIN order_items oi ON o.order_id = oi.order_id
    LEFT JOIN stores s ON o.store_id = s.store_id
    WHERE oi.item_replaced_id IS NULL  -- Исключаем записи замены товаров
    GROUP BY 
        o.order_id, o.user_id, o.store_id, s.store_address, 
        o.created_at, o.paid_at, o.delivered_at, o.canceled_at,
        o.order_discount, o.delivery_cost, o.order_cancellation_reason,
        o.driver_id
),
daily_aggregation AS (
    SELECT 
        -- Разрезы
        order_year,
        order_month,
        order_date,
        city,
        store_id,
        
        -- Метрики
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT CASE WHEN is_paid = 1 THEN order_id END) AS paid_orders,
        COUNT(DISTINCT CASE WHEN is_delivered = 1 THEN order_id END) AS delivered_orders,
        COUNT(DISTINCT CASE WHEN is_canceled = 1 THEN order_id END) AS canceled_orders,
        SUM(is_canceled_after_delivery) AS canceled_after_delivery,
        SUM(is_service_error_cancel) AS service_error_cancels,
        COUNT(DISTINCT user_id) AS unique_customers,
        
        -- Финансовые метрики
        SUM(order_revenue_before_order_discount) AS turnover,
        SUM(order_revenue) AS revenue,
        SUM(order_profit) AS profit,
        
        -- Средние значения
        AVG(order_revenue) AS avg_check,
        
        -- Метрики на покупателя
        COUNT(DISTINCT order_id)::DECIMAL / NULLIF(COUNT(DISTINCT user_id), 0) AS orders_per_customer,
        SUM(order_revenue) / NULLIF(COUNT(DISTINCT user_id), 0) AS revenue_per_customer,
        
        -- Метрики по курьерам
        SUM(CASE WHEN has_driver_change = TRUE THEN 1 ELSE 0 END) AS orders_with_driver_change,
        MAX(active_drivers_count) AS active_drivers
        
    FROM order_calculations
    GROUP BY 
        order_year, order_month, order_date, city, store_id
)
SELECT 
    *,
    -- Самый популярный день недели (для информации)
    TO_CHAR(order_date, 'Day') AS day_of_week
FROM daily_aggregation
ORDER BY order_date, store_id;
