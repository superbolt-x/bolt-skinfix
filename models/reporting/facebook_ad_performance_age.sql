{{ config (
    alias = target.database + '_facebook_ad_performance_age'
)}}

SELECT
account_id,
campaign_name,
CASE 
    WHEN campaign_name ~* 'ca' THEN 'CA'
    WHEN campaign_name !~* 'ca' THEN 'US'
END AS market,
SPLIT_PART(ad_name, ' - ', 4) AS product_focus,
SPLIT_PART(ad_name, ' - ', 3) AS product_type,
SPLIT_PART(ad_name, ' -', 5) AS product_name,
campaign_id,
campaign_effective_status,
CASE WHEN campaign_name ~* 'prospecting' THEN 'Campaign Type: Prospecting'
    ELSE 'Campaign Type: Remarketing'
END AS campaign_type_default,
CASE 
    WHEN campaign_name ~* 'sephora' THEN 'Sephora'
    WHEN campaign_name !~* 'sephora' THEN 'DTC'
END AS campaign_type_custom,
adset_name,
adset_id,
adset_effective_status,
audience,
CASE
    WHEN ((campaign_name ~* 'sephora' OR campaign_name ~* 'collab') AND adset_name ~* '-') THEN SPLIT_PART(adset_name, ' - ', 3)
    WHEN ((campaign_name ~* 'sephora' OR campaign_name ~* 'collab') AND adset_name ~* '|') THEN SPLIT_PART(adset_name, ' | ', 3)
END AS audience_collection_type,
ad_name,
ad_id,
ad_effective_status,
SPLIT_PART(ad_name, ' - ', 8) as ad_approach,
SPLIT_PART(ad_name, ' - ', 1) as ad_type,
visual,
copy,
format_visual,
visual_copy,
age,
date,
date_granularity,
spend,
clicks,
impressions,
link_clicks,
add_to_cart,
purchases,
revenue
FROM {{ ref('facebook_performance_by_ad_age') }}
