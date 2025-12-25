-- Витрина товаров (агрегация по дням, магазинам и товарам)
WITH product_sales AS (
    SELECT 
        -- Разрезы времени
        DATE(o.created_at) AS sale_date,
        EXTRACT(YEAR FROM o.created_at) AS sale_year,
        EXTRACT(MONTH FROM o.created_at) AS sale_month,
        
        -- Разрезы локации
        s.store_id,
        CASE 
            WHEN s.store_address LIKE '%Москва%' THEN 'Москва'
            WHEN s.store_address LIKE '%Санкт-Петербург%' THEN 'Санкт-Петербург'
            WHEN s.store_address LIKE '%Казань%' THEN 'Казань'
            ELSE 'Неизвестно'
        END AS city,
        
        -- Разрезы товара
        i.item_id,
        i.item_title,
        i.item_category,
        
        -- Метрики товара в заказе
        oi.order_id,
        oi.item_quantity AS ordered_quantity,
        oi.item_canceled_quantity AS canceled_quantity,
        oi.item_price,
        oi.item_discount,
        
        -- Флаги
        CASE WHEN oi.item_canceled_quantity > 0 THEN 1 ELSE 0 END AS has_canceled_items,
        CASE WHEN oi.item_replaced_id IS NOT NULL THEN 1 ELSE 0 END AS is_replacement_item,
        
        -- Расчет оборота по товару
        (oi.item_price * oi.item_quantity) * 
        (1 - COALESCE(oi.item_discount, 0) / 100.0) AS item_turnover
        
    FROM order_items oi
    JOIN items i ON oi.item_id = i.item_id
    JOIN orders o ON oi.order_id = o.order_id
    LEFT JOIN stores s ON o.store_id = s.store_id
    WHERE oi.item_replaced_id IS NULL  -- Основные товары, не замены
),
daily_product_metrics AS (
    SELECT 
        -- Разрезы
        sale_year,
        sale_month,
        sale_date,
        city,
        store_id,
        item_id,
        item_title,
        item_category,
        
        -- Базовые метрики
        SUM(ordered_quantity) AS total_ordered_quantity,
        SUM(canceled_quantity) AS total_canceled_quantity,
        COUNT(DISTINCT order_id) AS orders_with_item,
        COUNT(DISTINCT CASE WHEN has_canceled_items = 1 THEN order_id END) AS orders_with_canceled_items,
        SUM(item_turnover) AS item_turnover,
        
        -- Расчет эффективности
        CASE 
            WHEN SUM(ordered_quantity) > 0 
            THEN SUM(canceled_quantity)::DECIMAL / SUM(ordered_quantity) 
            ELSE 0 
        END AS cancel_rate
        
    FROM product_sales
    GROUP BY 
        sale_year, sale_month, sale_date, city, 
        store_id, item_id, item_title, item_category
),
ranked_products AS (
    SELECT 
        *,
        -- Ранжирование товаров по популярности за день
        ROW_NUMBER() OVER (
            PARTITION BY sale_date, store_id 
            ORDER BY total_ordered_quantity DESC
        ) AS daily_rank,
        
        -- Ранжирование товаров по популярности за месяц
        ROW_NUMBER() OVER (
            PARTITION BY sale_year, sale_month, store_id 
            ORDER BY SUM(total_ordered_quantity) OVER (
                PARTITION BY sale_year, sale_month, store_id, item_id
            ) DESC
        ) AS monthly_rank
        
    FROM daily_product_metrics
)
SELECT 
    -- Разрезы
    sale_year,
    sale_month,
    sale_date,
    city,
    store_id,
    item_id,
    item_title,
    item_category,
    
    -- Метрики
    total_ordered_quantity,
    total_canceled_quantity,
    orders_with_item,
    orders_with_canceled_items,
    item_turnover,
    cancel_rate,
    
    -- Популярность
    CASE WHEN daily_rank = 1 THEN 1 ELSE 0 END AS is_daily_top_product,
    CASE WHEN daily_rank = (SELECT MAX(daily_rank) FROM ranked_products r2 
                           WHERE r2.sale_date = r.sale_date AND r2.store_id = r.store_id) 
         THEN 1 ELSE 0 END AS is_daily_worst_product,
    
    CASE WHEN monthly_rank = 1 THEN 1 ELSE 0 END AS is_monthly_top_product,
    CASE WHEN monthly_rank = (SELECT MAX(monthly_rank) FROM ranked_products r2 
                             WHERE r2.sale_year = r.sale_year 
                               AND r2.sale_month = r.sale_month 
                               AND r2.store_id = r.store_id) 
         THEN 1 ELSE 0 END AS is_monthly_worst_product
    
FROM ranked_products r
ORDER BY sale_date, store_id, total_ordered_quantity DESC;
