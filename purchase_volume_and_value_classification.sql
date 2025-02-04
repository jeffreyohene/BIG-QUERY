-- purchase volume and value classification
/*
This query generates a report based on GA4 purchase data, including user session and transaction details. 
It calculates the total number of sessions, the number of purchases, and the total revenue for each user. 
It also classifies users based on their purchase value (top 60% = A, next 20% = B, last 20% = C) and transaction frequency (velocity classification).
*/

WITH base AS (
  SELECT
    CONCAT(
      (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id'),
      user_pseudo_id
    ) AS session_id,
    user_pseudo_id AS user_id,
    event_name,
    traffic_source.source AS source,
    traffic_source.medium AS medium,
    traffic_source.name AS campaign,
    IFNULL(ecommerce.purchase_revenue, 0) AS purchase_revenue  -- purchase revenue (0 if no purchase)
  FROM `project.events_*`
),

ranked_value AS (
  -- aggregates the base data to calculate session counts, purchase counts, total purchase revenue, and conversion rate for each user
  SELECT
    user_id,
    source,
    medium,
    campaign,
    COUNT(session_id) AS sessions,
    COUNTIF(event_name = 'purchase') AS purchases,
    SUM(purchase_revenue) AS total_purchase_revenue,
    CASE
      WHEN COUNT(session_id) > 0 THEN ROUND(COUNTIF(event_name = 'purchase') / COUNT(session_id) * 100, 2)  -- calculate conversion rate
      ELSE 0
    END AS conversion_rate
  FROM base
  GROUP BY user_id, source, medium, campaign
),

ranked_velocity AS (
  -- classifies users based on purchase value and transaction velocity
  SELECT
    user_id,
    sessions,
    purchases AS transactions,
    total_purchase_revenue AS transaction_value,
    conversion_rate AS purchase_conversion_rate,
    source,
    medium,
    campaign,
    NTILE(5) OVER (ORDER BY total_purchase_revenue DESC) AS value_rank,  -- classifies users based on purchase value
    NTILE(5) OVER (ORDER BY purchases DESC) AS velocity_rank  -- classifies users based on purchase velocity
  FROM ranked_value
)

SELECT
  user_id,
  source,
  medium,
  campaign,
  sessions,
  transactions,
  transaction_value,
  purchase_conversion_rate,
  CASE 
    WHEN transactions = 0 THEN 'No Purchases'  -- flag users with no purchases
    WHEN value_rank <= 3 THEN 'A'  -- classify top 60% by transaction value as 'A'
    WHEN value_rank = 4 THEN 'B'  -- classify next 20% by transaction value as 'B'
    ELSE 'C'  -- classify the last 20% by transaction value as 'C'
  END AS value_classification,
  CASE 
    WHEN transactions = 0 THEN 'No Purchases'  -- flag users with no purchases
    WHEN velocity_rank <= 3 THEN 'A'  -- classify top 60% by purchase velocity as 'A'
    WHEN velocity_rank = 4 THEN 'B'  -- classify next 20% by purchase velocity as 'B'
    ELSE 'C'  -- classify the last 20% by purchase velocity as 'C'
  END AS velocity_classification
FROM ranked_velocity