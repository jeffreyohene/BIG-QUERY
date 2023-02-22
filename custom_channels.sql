-- Creating custom 
with erste as(
    select
    user_pseudo_id as ga_client_id, 
    concat(user_pseudo_id,'.',(select cast(value.int_value as string) from unnest(event_params) where key = 'ga_session_id')) as session_id, -- combine user_pseudo_id and session_id for a unique session-id
    timestamp_micros(min(event_timestamp)) as session_start,
    array_agg(
        if(event_name in('user_engagement','page_view','scroll'), struct(
            event_timestamp,
            lower((select value.string_value from unnest(event_params) where key = 'source')) as source,
            lower((select value.string_value from unnest(event_params) where key = 'medium')) as medium,
            lower((select value.string_value from unnest(event_params) where key = 'name')) as name,
            lower((select value.string_value from unnest(event_params) where key = 'campaign')) as campaign,
            (select value.int_value from unnest(event_params) where key = 'entrances') as is_entrance,
            (select value.int_value from unnest(event_params) where key = 'ignore_referrer') as ignore_referrer
        ), null) 
    ignore nulls) as channels_in_session,
    countif(event_name = 'purchase') as conversions,
    sum(ecommerce.purchase_revenue) as conversion_value
from
    `data.events_*` 
group by 1,2
),


zweite as (
    select
        (select t.source from unnest(channels_in_session) as t where t.ignore_referrer is null order by t.event_timestamp asc limit 1) as source,
        (select t.medium from unnest(channels_in_session) as t where t.ignore_referrer is null order by t.event_timestamp asc limit 1) as medium,
        (select t.campaign from unnest(channels_in_session) as t where t.ignore_referrer is null order by t.event_timestamp asc limit 1) as campaign,
        (select t.name from unnest(channels_in_session) as t where t.ignore_referrer is null order by t.event_timestamp asc limit 1) as name,
        count(distinct session_id) as sessions,
        sum(conversions) as conversions,
        ifnull(sum(conversion_value), 0) as conversion_value
    from
        erste
    group by
        1, 2, 3, 4
)

-- using a bit of regex(yeah I hate it too) to create matches so we can create custom names for source
select
    case
     when regexp_contains(campaign, r'^(.*shop.*)$') and regexp_contains(medium, r'^(.*cp.*|ppc|paid.*)$') 
        then 'shopping_paid'
    when regexp_contains(source, r'^(google|bing)$') and regexp_contains(medium, r'^(.*cp.*|ppc|paid.*)$') 
        then 'paid_search'
    when regexp_contains(source, r'^(twitter|instagram|ig|fb|linkedin|facebook|pinterest)$') and regexp_contains(medium, r'^(.*cp.*|ppc|paid.*|social_paid)$') 
        then 'paid_social'
    when regexp_contains(source, r'^(youtube)$') and regexp_contains(medium, r'^(.*cp.*|ppc|paid.*)$') 
        then 'video_paid'
    when regexp_contains(medium, r'^(display|banner|expandable|interstitial|cpm)$') 
        then 'display'
    when regexp_contains(medium, r'^(.*cp.*|ppc|paid.*)$') 
        then 'other_paid'
    when regexp_contains(medium, r'^(.*shop.*)$') 
        then 'shopping_organic'
    when regexp_contains(source, r'^.*(twitter|t\.co|facebook|instagram|linkedin|lnkd\.in|pinterest).*') or regexp_contains(medium, r'^(social|social_advertising|social-advertising|social_network|social-network|social_media|social-media|sm|social-unpaid|social_unpaid)$') 
        then 'organic_social'
    when regexp_contains(medium, r'^(.*video.*)$') 
        then 'video_organic'
    when regexp_contains(source, r'^(google|bing|yahoo)$') or medium = 'organic'
        then 'organic_search'
    when regexp_contains(source, r'^(email|mail|e-mail|e_mail)$') or regexp_contains(medium, r'^(email|mail|e-mail|e_mail)$') 
        then 'email'
    when regexp_contains(medium, r'^(affiliate|affiliates)$') 
        then 'affiliate'
    when medium = 'referral'
        then 'referral'
    when medium = 'audio' 
        then 'audio'
    when medium = 'sms'
        then 'sms'
    when ends_with(medium, 'push') or regexp_contains(medium, r'.*(mobile|notification|notif).*') 
        then 'mobile_push'
    else 'Flag!' -- personally use this as it helps easier debugging in the event that there are new sources/media that do not get bucketed
    end as custom_channels,
    sum(sessions) as sessions,
    sum(conversions) as conversions,
    sum(conversion_value) as conversion_value

from zweite
group by 1