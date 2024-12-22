SELECT
  event_date, -- the date of the event (yyyymmdd format)
  event_timestamp, -- the exact timestamp of the event in microseconds
  event_name, -- the name of the event (e.g., 'page_view')
  user_pseudo_id, -- a unique identifier for the user

  -- extracting specific parameters from the event_params array
  /*
  nested fields can be a little bit tricky. i used query below to find distinct event parameters for page views. not every exported event comes with
  all these parameters so you might need this to make sure you're including all the parameters so u don't omit data:

  SELECT DISTINCT
    ep.key AS parameter_key
  FROM
    `ga4_events_table_name`,
    UNNEST(event_params) AS ep
  WHERE
    event_name = 'first_visit'
  */
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title') AS page_title, -- the title of the page viewed
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location, -- the url of the page
  (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number') AS ga_session_number, -- the session number
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'term') AS term, -- the search term used, if applicable
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_referrer') AS page_referrer, -- the referrer url
  (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'entrances') AS entrances, -- the number of entrances (first interactions in a session)
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source') AS source, -- the traffic source
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium') AS medium, -- the traffic medium (e.g., 'organic', 'cpc')
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign') AS campaign, -- the campaign associated with the session
  (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id, -- the session id
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged') AS session_engaged, -- whether the session was engaged
  (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'engaged_session_event') AS engaged_session_event, -- engaged session event count
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'ignore_referrer') AS ignore_referrer, -- whether to ignore the referrer
  (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'debug_mode') AS debug_mode, -- debug mode status
  (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'container_version') AS container_version, -- the container version of the tag
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'container_id') AS container_id, -- the container id of the tag
  (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'batch_page_id') AS batch_page_id, -- the batch page id for grouped events
  (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'batch_ordering_id') AS batch_ordering_id, -- the ordering id for grouped events

  -- user and device metadata
  user_first_touch_timestamp,
  device.category,
  device.mobile_brand_name,
  device.mobile_os_hardware_model,
  device.operating_system,
  device.operating_system_version,
  device.language,
  device.is_limited_ad_tracking,
  device.time_zone_offset_seconds,
  device.browser,
  device.browser_version,
  device.web_info.browser, 
  device.web_info.hostname,

  -- location info
  geo.city,
  geo.country,
  geo.continent,
  geo.region,
  geo.sub_continent,
  geo.metro,

  -- app info(relevant to page views? will cnfirm later)
  app_info.id,
  app_info.version,
  app_info.install_store,
  app_info.firebase_app_id,
  app_info.install_source,

  -- traffic
  traffic_source.name,
  traffic_source.medium,
  traffic_source.source,

  -- other metadata(heads up, these are columns exported with events. will have to crpsscheck if they're important at all to page views)
  stream_id,
  platform,
  event_dimensions.hostname,
  collected_traffic_source.manual_campaign_id,
  collected_traffic_source.manual_campaign_name,
  collected_traffic_source.manual_source,
  collected_traffic_source.manual_content,
  collected_traffic_source.manual_source_platform,
  collected_traffic_source.manual_marketing_tactic,

  -- misc
  is_active_user,
  batch_event_index, -- index of the event within the batch, nested param field has only id and ordering id, index might be interesting to look at
  batch_page_id,
  batch_ordering_id,

  -- session-level campaign info from manual last-click attribution
  session_traffic_source_last_click.manual_campaign.campaign_id,
  session_traffic_source_last_click.manual_campaign.campaign_name,
  session_traffic_source_last_click.manual_campaign.source,
  session_traffic_source_last_click.manual_campaign.medium,
  session_traffic_source_last_click.manual_campaign.term,
  session_traffic_source_last_click.manual_campaign.content,
  session_traffic_source_last_click.manual_campaign.source_platform,
  session_traffic_source_last_click.manual_campaign.creative_format,
  session_traffic_source_last_click.manual_campaign.marketing_tactic,

  -- advertising campaign details (google ads, sa360, cm360)
  session_traffic_source_last_click.google_ads_campaign.customer_id,
  session_traffic_source_last_click.google_ads_campaign.account_name,
  session_traffic_source_last_click.google_ads_campaign.campaign_id,
  session_traffic_source_last_click.google_ads_campaign.campaign_name,
  session_traffic_source_last_click.google_ads_campaign.ad_group_id,
  session_traffic_source_last_click.google_ads_campaign.ad_group_name,
  session_traffic_source_last_click.cross_channel_campaign.campaign_id,
  session_traffic_source_last_click.cross_channel_campaign.campaign_name,
  session_traffic_source_last_click.cross_channel_campaign.source,
  session_traffic_source_last_click.cross_channel_campaign.medium,
  session_traffic_source_last_click.cross_channel_campaign.source_platform,
  session_traffic_source_last_click.cross_channel_campaign.default_channel_group,
  session_traffic_source_last_click.cross_channel_campaign.primary_channel_group,
  session_traffic_source_last_click.sa360_campaign.campaign_id,
  session_traffic_source_last_click.sa360_campaign.campaign_name,
  session_traffic_source_last_click.sa360_campaign.ad_group_id,
  session_traffic_source_last_click.sa360_campaign.ad_group_name,
  session_traffic_source_last_click.sa360_campaign.creative_format,
  session_traffic_source_last_click.sa360_campaign.engine_account_name,
  session_traffic_source_last_click.sa360_campaign.engine_account_type,
  session_traffic_source_last_click.sa360_campaign.manager_account_name,
  session_traffic_source_last_click.cm360_campaign.campaign_id,
  session_traffic_source_last_click.cm360_campaign.campaign_name,
  session_traffic_source_last_click.cm360_campaign.source,
  session_traffic_source_last_click.cm360_campaign.medium,
  session_traffic_source_last_click.cm360_campaign.account_id,
  session_traffic_source_last_click.cm360_campaign.account_name,
  session_traffic_source_last_click.cm360_campaign.advertiser_id,
  session_traffic_source_last_click.cm360_campaign.advertiser_name,
  session_traffic_source_last_click.cm360_campaign.creative_id,
  session_traffic_source_last_click.cm360_campaign.creative_format,
  session_traffic_source_last_click.cm360_campaign.creative_name,
  session_traffic_source_last_click.cm360_campaign.creative_type,
  session_traffic_source_last_click.cm360_campaign.creative_type_id,
  session_traffic_source_last_click.cm360_campaign.creative_version,
  session_traffic_source_last_click.cm360_campaign.placement_id

FROM `ga4_events_table_name`
-- UNNEST(event_params) AS ev_params
WHERE event_name = 'page_view'
ORDER BY event_timestamp ASC;