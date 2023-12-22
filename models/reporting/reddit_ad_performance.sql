{{ config (
    alias = target.database + '_reddit_ad_performance'
)}}

SELECT 
campaign_name,
campaign_id,
campaign_effective_status as campaign_status,
campaign_type_default,
ad_group_name,
ad_group_id,
ad_group_effective_status as ad_group_status,
audience,
ad_name,
ad_id,
ad_effective_status as ad_status,
format_visual,
visual,
copy,
visual_copy,
date,
date_granularity,
spend,
impressions,
clicks,
add_to_cart,
purchase as purchases,
purchase_value as revenue
FROM {{ ref('reddit_performance_by_ad') }}
