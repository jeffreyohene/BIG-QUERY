/*
  The purpose of this query is to calculate RFM score (Recency, Frequency, Monetary) for 
  each customer based on their purchase behavior. 

  This involves:
  - Calculating various base metrics for RFM (total revenue, average revenue, total 
    transactions, most recent purchase date, and recency) for each user from the GA4 events data.
  - Segmenting users into RFM categories based on their RFM scores.
  - Mapping the RFM scores to predefined segments to interpret customer behavior relative to business goals.

  The final output provides customer IDs along with their RFM scores and segments, which can be used
  for targeted marketing strategies, customer retention efforts, and personalized communication.
*/

-- calculate various base for RFM metrics for each user from the GA4 events data.
WITH rfm_data AS (
  SELECT
    user_pseudo_id AS user_id,  -- selecting user pseudo ID as user ID
    SUM(ecommerce.purchase_revenue) AS total_revenue,  -- calculate total revenue per user
    ROUND(AVG(ecommerce.purchase_revenue), 2) AS average_revenue,  -- calculate average revenue per user
    COUNT(DISTINCT ecommerce.transaction_id) AS total_transactions,  -- count total transactions per user
    MAX(DATETIME(TIMESTAMP_MICROS(event_timestamp), 'UTC')) AS most_recent_purchase,  -- calculate the most recent purchase date per user
    (
      SELECT 
        MAX(DATETIME(TIMESTAMP_MICROS(event_timestamp), 'UTC'))
      FROM `yourtable.events_*`
    ) AS max_order_date,  -- Finding the maximum order date across all events
    DATE_DIFF(
      (
        SELECT 
          MAX(DATETIME(TIMESTAMP_MICROS(event_timestamp), 'UTC'))
        FROM `yourtable.events_*`
      ),
        MAX(DATETIME(TIMESTAMP_MICROS(event_timestamp), 'UTC')), day  -- Calculating recency as the difference between the maximum order date and the most recent purchase date
    ) AS recency
  FROM 
    `yourtable.events_*`
  WHERE 
    _TABLE_SUFFIX >= FORMAT_DATE('%Y%m', DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH))  -- Filtering events from the last 3 months
  AND 
    event_name = 'purchase'  -- Filtering only purchase events
  GROUP BY 
    1  -- Grouping by user ID
),

-- This CTE segments users into RFM categories based on their RFM scores.
rfm_segments AS (
  SELECT
    user_id,
    NTILE(5) OVER (ORDER BY recency) AS rfm_recency,  -- assign recency score by dividing users into 5 equal segments
    NTILE(5) OVER (ORDER BY total_transactions) AS rfm_frequency,  -- assign frequency score by dividing users into 5 equal segments
    NTILE(5) OVER (ORDER BY average_revenue) AS rfm_monetary  -- assign monetary score by dividing users into 5 equal segments
  FROM rfm_data
),

-- combine the RFM scores into a single string for each user as the rfm score
rfm_strings AS (
  SELECT
    user_id,
    rfm_recency,
    rfm_frequency,
    rfm_monetary,
    CONCAT(
      CAST(rfm_recency AS STRING),
      CAST(rfm_frequency AS STRING),
      CAST(rfm_monetary AS STRING)
    ) AS rfm_string  -- concatenate RFM scores into a single string
  FROM rfm_segments
),

-- map the RFM strings to predefined segments, interpretation of segments can be customized relative to business goals
rfm_mapping AS (
  SELECT
    user_id,
    rfm_recency,
    rfm_frequency,
    rfm_monetary,
    rfm_string,
    CASE
      WHEN 
        rfm_string IN ('555', '554', '544', '545', '454', '455', '445') 
      THEN 'champions'
      WHEN 
        rfm_string IN ('543', '444', '435', '355', '354', '345', '344', '335') 
      THEN 'loyal customers'
      WHEN 
        rfm_string IN ('553', '551', '552', '541', '542', '533', '532', '531', '452', '451', '442', '441', '431', '453', '433', '432', '423', '353', '352', '351', '342', '341', '333', '323') 
      THEN 'potential loyalist'
      WHEN
        rfm_string IN ('512', '511', '422', '421', '412', '411', '311') 
      THEN 'recent customers'
      WHEN
        rfm_string IN ('525', '524', '523', '522', '521', '515', '514', '513', '425', '424', '413', '414', '415', '315', '314', '313')
      THEN 'promising'
      WHEN
        rfm_string IN ('535', '534', '443', '434', '343', '334', '325', '324') 
      THEN 'customers needing attention'
      WHEN
       rfm_string IN ('331', '321', '312', '221', '213') 
      THEN 'about to sleep'
      WHEN
       rfm_string IN ('255', '254', '245', '244', '253', '252', '243', '242', '235', '234', '225', '224', '153', '152', '145', '143', '142', '135', '134', '133', '125', '124')
      THEN 'at risk'
      WHEN
        rfm_string IN ('155', '154', '144', '214', '215', '115', '114', '113')
      THEN 'cannot lose them'
      WHEN
       rfm_string IN ('332', '322', '231', '241', '251', '233', '232', '223', '222', '132', '123', '122', '212', '211') 
      THEN 'hibernating'
      WHEN 
        rfm_string IN ('111', '112', '121', '131', '141', '151')
      THEN 'lost'
      ELSE 'NA'
    END AS rfm_segment
  FROM rfm_strings
)

-- execute final sql query
SELECT 
  user_id AS customer_id,
  rfm_recency AS recency,
  rfm_frequency AS frequency,
  rfm_monetary AS monetary,
  rfm_segment AS segment
FROM rfm_mapping;
