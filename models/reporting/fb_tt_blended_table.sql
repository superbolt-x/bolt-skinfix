{{ config (
    alias = target.database + '_fb_tt_blended_table'
)}}

  (SELECT 'Facebook' as channel, date, date_granularity, market, ad_name, visual, campaign_name, ad_approach, ad_type, campaign_type_custom,
        COALESCE(SUM(spend),0) AS spend, COALESCE(SUM(impressions),0) AS impressions, COALESCE(SUM(link_clicks),0) AS clicks, COALESCE(SUM(purchases),0) AS purchases, 
        COALESCE(SUM(revenue),0) AS revenue
    FROM {{ source('reporting','facebook_ad_performance') }}
    WHERE account_id = '152712828589300'
    GROUP BY channel, date, date_granularity, market, ad_name, visual, campaign_name, ad_approach, ad_type, campaign_type_custom)
    
    UNION ALL
    
    (SELECT 'TikTok' as channel, date, date_granularity, market, ad_name, visual, campaign_name, ad_approach, ad_type, campaign_type_custom,
        COALESCE(SUM(spend),0) AS spend, COALESCE(SUM(impressions),0) AS impressions, COALESCE(SUM(clicks),0) AS clicks, COALESCE(SUM(purchases),0) AS purchases, 
        COALESCE(SUM(revenue),0) AS revenue
    FROM {{ source('reporting','tiktok_ad_performance') }}
    GROUP BY channel, date, date_granularity, market, ad_name, visual, campaign_name, ad_approach, ad_type, campaign_type_custom)
