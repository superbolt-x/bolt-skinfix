{{ config (
    alias = target.database + '_tiktok_ad_performance_age'
)}}

SELECT 
campaign_name,
campaign_id,
campaign_status,
CASE WHEN campaign_name ~* 'prospecting' THEN 'Campaign Type: Prospecting'
    ELSE 'Campaign Type: Remarketing'
END AS campaign_type_default,
CASE 
    WHEN campaign_name ~* 'sephora' THEN 'Sephora'
    WHEN campaign_name !~* 'sephora' THEN 'DTC'
END AS campaign_type_custom,
CASE WHEN campaign_name ~* 'ca' THEN 'CA'
    WHEN campaign_name !~* 'ca' THEN 'US'
END as market,
SPLIT_PART(adgroup_name, ' - ', 3) as product_type,
SPLIT_PART(ad_name, ' - ', 4) as product_name,
SPLIT_PART(ad_name, ' - ', 3) as product_focus,
adgroup_name,
adgroup_id,
adgroup_status,
audience,
ad_name,
ad_id,
ad_status,
SPLIT_PART(ad_name, ' - ', 6) as ad_approach,
SPLIT_PART(ad_name, ' - ', 1) as ad_type,
visual,
age,  
date,
date_granularity,
cost as spend,
impressions,
clicks
FROM {{ ref('tiktok_performance_by_ad_age') }}
