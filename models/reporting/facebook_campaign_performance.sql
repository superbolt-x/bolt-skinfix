{{ config (
    alias = target.database + '_facebook_campaign_performance'
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
date,
date_granularity,
spend,
clicks,
impressions,
link_clicks,
add_to_cart,
purchases,
revenue
FROM {{ ref('facebook_performance_by_campaign') }}
