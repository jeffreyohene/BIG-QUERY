/*
  this query analyzes customer purchase behavior from GA4 event data to understand repeat purchase patterns and overall transactional activity.
  the final resultset contains following columns
    - customer_id
    - first_purchase_date
    - second_purchase_date
    - days_between_first_second
    - total_transactions
    - total_revenue
    - aov
    - last_purchase_date
    - days_between_first_last
    - days_since_last_purchase
*/

WITH purchases AS (
  SELECT
    user_pseudo_id AS customer_id,
    PARSE_DATE('%Y%m%d', event_date) AS order_date,
    TIMESTAMP_MICROS(event_timestamp) AS ts,
    ecommerce.transaction_id AS order_id,
    ROUND(ecommerce.purchase_revenue, 2) AS revenue,
    ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY event_timestamp) AS order_rank
  FROM `yourtable.events_*`
  WHERE event_name = 'purchase'
),

first_two AS (
  SELECT *
  FROM purchases
  WHERE order_rank <= 2
),

pivoted AS (
  SELECT
    customer_id,
    MAX(IF(order_rank = 1, order_date, NULL)) AS first_purchase_date,
    MAX(IF(order_rank = 2, order_date, NULL)) AS second_purchase_date
  FROM first_two
  GROUP BY customer_id
),

aggregates AS (
  SELECT
    customer_id,
    COUNT(DISTINCT order_id) AS total_transactions,
    SUM(revenue) AS total_revenue,
    ROUND(AVG(revenue), 2) AS aov,
    MIN(order_date) AS first_purchase_date,
    MAX(order_date) AS last_purchase_date,
    DATE_DIFF(MAX(order_date), MIN(order_date), DAY) AS days_between_first_last,
    DATE_DIFF(CURRENT_DATE(), MAX(order_date), DAY) AS days_since_last_purchase
  FROM purchases
  GROUP BY customer_id
)

SELECT
  p.customer_id,
  p.first_purchase_date,
  p.second_purchase_date,
  DATE_DIFF(p.second_purchase_date, p.first_purchase_date, DAY) AS days_between_first_second,
  a.total_transactions,
  a.total_revenue,
  a.aov,
  a.last_purchase_date,
  a.days_between_first_last,
  a.days_since_last_purchase
FROM pivoted p
JOIN aggregates a ON p.customer_id = a.customer_id
WHERE p.second_purchase_date IS NOT NULL
