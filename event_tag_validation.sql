-- Basically event tag validation for working with GTM data in BigQuery

with erste as(    
  select
  timestamp_micros(event_timestamp) as times,
  event_name,
    concat((select value.int_value from unnest(event_params) where key = 'ga_session_id'), user_pseudo_id) as session_id,
      user_pseudo_id as ga_client_id,
    array_agg(
      if(event_name in('first_visit','session_start', 'page_view', 'select_item','view_item','view_item_list','purchase','add_to_cart','begin_checkout','remove_from_cart'), struct(
        event_timestamp,
        lower((select value.string_value from unnest(event_params) where key = 'container id')) as container_id,
        (select value.int_value from unnest(event_params) where key = 'container version') as container_version,
        lower((select value.string_value from unnest(event_params) where key = 'tag name')) as tag_name,
        lower((select value.string_value from unnest(event_params) where key = 'tag category')) as tag_category,
        lower((select value.string_value from unnest(event_params) where key = 'page_location')) as page_location,
        lower((select value.string_value from unnest(event_params) where key = 'page_title')) as page_title,
        lower((select value.string_value from unnest(event_params) where key = 'tag date creation')) as tag_date_creation,
        (select value.int_value from unnest(event_params) where key = 'entrances') as is_entrance,
        (select value.int_value from unnest(event_params) where key = 'ignore_referrer') as ignore_referrer
        ), null) 
      ignore nulls) as channels_in_session,
  from `data.events_*`
  group by times, event_name, user_pseudo_id, session_id,event_timestamp
),

zweite as(
  select
    (select t.container_id from unnest(channels_in_session) as t where t.ignore_referrer is null order by t.event_timestamp asc limit 1) as container_id,
    (select t.container_version from unnest(channels_in_session) as t where t.ignore_referrer is null order by t.event_timestamp asc limit 1) as container_version,
    (select t.tag_name from unnest(channels_in_session) as t where t.ignore_referrer is null order by t.event_timestamp asc limit 1) as tag_name,
    (select t.tag_category from unnest(channels_in_session) as t where t.ignore_referrer is null order by t.event_timestamp asc limit 1) as tag_category,
    (select t.page_location from unnest(channels_in_session) as t where t.ignore_referrer is null order by t.event_timestamp asc limit 1) as page_location,
    (select t.page_title from unnest(channels_in_session) as t where t.ignore_referrer is null order by t.event_timestamp asc limit 1) as page_title,
    (select t.tag_date_creation from unnest(channels_in_session) as t where t.ignore_referrer is null order by t.event_timestamp asc limit 1) as tag_date_creation,
    event_name,
    times, -- new line
    extract(month from times) as month,
    extract(year from times) as year
  from erste)  


select 

event_name,
container_id,
container_version,
tag_name,
tag_category,
tag_date_creation,
count(*) as event_count
from zweite
where event_name not in ('user_engagement','scroll','form_start', 'form_submit','page_view','session_start','first_visit', 'click')
group by 1,2,3,4,5,6
order by event_count desc