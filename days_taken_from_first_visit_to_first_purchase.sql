/*
  This SQL query calculates the number of days between a user's first visit and their first purchase for a business based on GA4  data. 
  We use two Common Table Expressions (CTEs) to extract the first visit date and the first purchase date for each user, and then join these 
  them to compute the difference in days between these two events for each customer.

  Key points to know before implementing this:
  - Data Source: The query operates on the GA4 eventdata so in the FROM command, enter your GA4 eventdata source there.
  - SQL Engine: This query is written for BigQuery, implementing in other DBMS systems might lead to unexpected behavior as native commands may differ.

  Steps performed in the query:
  - Extract the first visit date for each user from events with `event_name = 'first_visit'`.
  - Extract the first purchase date for each user from events with `event_name = 'purchase'`, using a row number to identify the first event.
  - Join the results of the two CTEs on `user_pseudo_id`.
  - Calculate the number of days between the first visit and the first purchase for each user.
*/

-- cte to extract the first visit date for each user
WITH first_visit AS (
  SELECT
    user_pseudo_id,
    PARSE_DATE('%Y%m%d', event_date) AS first_visit_date
  FROM
    `tbl.events_*`
  WHERE
    event_name = 'first_visit'  -- filter to include only first visit events
),

-- cte to extract the first purchase date for each user
first_purchase AS (
  SELECT
    user_pseudo_id,
    PARSE_DATE('%Y%m%d', event_date) AS first_purchase_date
  FROM (
    SELECT
      user_pseudo_id,
      event_date,
      ROW_NUMBER() OVER (PARTITION BY user_pseudo_id ORDER BY PARSE_DATE('%Y%m%d', event_date)) AS row_num  -- assign row numbers to events for each user ordered by date
    FROM
      `tbl.events_*`
    WHERE
      event_name = 'purchase'  -- filter to include only purchase events
  )
  WHERE row_num = 1  -- we will select only the first purchase event for each user since we are looking for time between first visit and first purchase
)

-- main query to join the first_visit and first_purchase dates for each customer, ordered by number of days taken.
SELECT
  fv.user_pseudo_id,
  fv.first_visit_date,  -- first visit date for each user
  fp.first_purchase_date,  -- first purchase date for each user
  DATE_DIFF(fp.first_purchase_date, fv.first_visit_date, DAY) AS days_between  -- date_diff() calculates the number of days between first visit and first purchase
FROM
  first_visit fv
INNER JOIN
  first_purchase fp
  ON fv.user_pseudo_id = fp.user_pseudo_id  -- join both tables on customer identifier
ORDER BY
  days_between DESC;  -- order results by the number of days between first visit and first purchase
