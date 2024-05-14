{{ config (
    alias = target.database + '_googleads_campaign_performance'
)}}

SELECT 
account_id,
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
CASE 
    WHEN campaign_name ~* 'sephora' THEN 'Sephora'
    WHEN campaign_name !~* 'sephora' THEN 'DTC'
END AS campaign_type_custom,
CASE WHEN account_id = '7985220394' THEN 'US'
    WHEN account_id = '4973168899' THEN 'CA'
END AS market,
date,
date_granularity,
CASE WHEN account_id = '4973168899' THEN spend*0.73
    ELSE spend
END as spend,
impressions,
clicks,
conversions as purchases,
CASE WHEN account_id = '4973168899' THEN conversions_value*0.73
    ELSE conversions_value
END as revenue,
search_impression_share,
search_budget_lost_impression_share,
search_rank_lost_impression_share
FROM {{ ref('googleads_performance_by_campaign') }}
