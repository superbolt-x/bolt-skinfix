{{ config (
    alias = target.database + '_googleads_ad_performance'
)}}

SELECT 
account_id,
ad_name,
ad_id,
ad_status,
ad_group_name,
ad_group_id,
SPLIT_PART(ad_group_name, ' - ', 3) AS product_name,
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
CASE WHEN account_id = '7985220394' THEN 'US'
    WHEN account_id = '4973168899' THEN 'CA'
END AS market,
date,
date_granularity,
CASE WHEN account_id = '4973168899' THEN spend*0.76
    ELSE spend
END as spend,
impressions,
clicks,
conversions as purchases,
CASE WHEN account_id = '4973168899' THEN conversions_value*0.76
    ELSE conversions_value
END as revenue
FROM {{ ref('googleads_performance_by_ad') }}
