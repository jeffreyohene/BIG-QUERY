-- Query to create product performance report

with erste as (  
  select
  timestamp_micros(event_timestamp) as time_stamp,
    event_name,
    i.item_id as id,
    i.item_list_name as list_name,
    i.item_list_index as list_index,
    i.item_name as name,
    i.item_category as category,
    i.item_variant as variant,
    i.item_brand as brand,
    i.quantity as quantity,
    i.price as price,
    i.item_revenue as revenue,
  from `data.events_*`, unnest(items) as i
  where event_name in ('view_item_list','add_to_cart','remove_from_cart','view_item','begin_checkout','select_item','purchase') -- subsetting relevant events for report
),
 
zweite as(
  select
    id,
    list_name,
    list_index,
    name,
    category,
    variant,
    brand,
    #quantity,
    price,
    revenue,
    extract(date from time_stamp) as date_col,
    extract(isoweek from time_stamp) as week,
    extract(dayofweek from time_stamp) as day,
    extract(month from time_stamp) as month,
    extract(year from time_stamp) as year,
    countif(distinct(event_name = 'view_item_list')) as view_item_list,
    countif(distinct(event_name = 'select_item')) as product_click,
    countif(distinct(event_name = 'view_item')) as view_item,
    countif(distinct(event_name = 'add_to_cart')) as add_to_cart,
    countif(distinct(event_name = 'remove_from_cart')) as remove_from_cart,  
    countif(distinct(event_name = 'begin_checkout')) as begin_checkout,
    countif(event_name = 'purchase') as purchase
  from erste
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)

select
  date_col, -- -- helps easier custom breakdown in BigQuery, not so important in Looker because of its flexibility of handling dates. Case statements into smaller parts below
  year,
  case
    when month = 1 then 'January'
    when month = 2 then 'February'
    when month = 3 then 'March'
    when month = 4 then 'April'
    when month = 5 then 'May'
    when month = 6 then 'June'
    when month = 7 then 'July'
    when month = 8 then 'August'
    when month = 9 then 'September'
    when month = 10 then 'October'
    when month = 11 then 'November'
    when month = 12 then 'December'
    else 'Flag!' -- for easier debugging
  end as month, 
  case 
    when day = 1 then 'Sunday'
    when day = 2 then 'Monday'
    when day = 3 then 'Tuesday'
    when day = 4 then 'Wednesday'
    when day = 5 then 'Thursday'
    when day = 6 then 'Friday'
    when day = 7 then 'Saturday'
    else 'Flag!' -- for easier debugging
  end as weekday,
  week,
  id,
  name,
  category,
  price,
  sum(view_item_list) as view_item_list,
  sum(product_click) as product_click,
  sum(view_item) as view_item,
  sum(add_to_cart) as add_to_cart,
  sum(remove_from_cart) as remove_from_cart,
  sum(begin_checkout) as begin_checkout,
  sum(purchase) as purchase,
  revenue,
from zweite
group by 1,2,3,4,5,6,7,8,9,17