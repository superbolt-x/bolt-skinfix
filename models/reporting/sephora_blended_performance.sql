{{ config (
    alias = target.database + '_sephora_blended_performance'
)}}

  (SELECT 'Facebook' as channel, date, date_granularity, market, campaign_name, adset_name as ad_group_name, product_name,
        COALESCE(SUM(spend),0) AS spend, COALESCE(SUM(impressions),0) AS impressions, COALESCE(SUM(link_clicks),0) AS clicks, COALESCE(SUM(purchases),0) AS purchases, 
        COALESCE(SUM(revenue),0) AS revenue
    FROM {{ source('reporting','facebook_ad_performance') }}
    WHERE account_id = '152712828589300'
    AND campaign_type_custom = 'Sephora'
    GROUP BY channel, date, date_granularity, market, campaign_name, ad_group_name, product_name)
    
    UNION ALL
    
    (SELECT 'TikTok' as channel, date, date_granularity, market, campaign_name, adgroup_name as ad_group_name, product_name,
        COALESCE(SUM(spend),0) AS spend, COALESCE(SUM(impressions),0) AS impressions, COALESCE(SUM(clicks),0) AS clicks, COALESCE(SUM(purchases),0) AS purchases, 
        COALESCE(SUM(revenue),0) AS revenue
    FROM {{ source('reporting','tiktok_ad_performance') }}
    WHERE campaign_type_custom = 'Sephora'
    GROUP BY channel, date, date_granularity, market, campaign_name, ad_group_name, product_name)

    UNION ALL
    
    (SELECT 'Youtube Shorts' as channel, date, date_granularity, market, campaign_name, ad_group_name, product_name,
        COALESCE(SUM(spend),0) AS spend, COALESCE(SUM(impressions),0) AS impressions, COALESCE(SUM(clicks),0) AS clicks, COALESCE(SUM(purchases),0) AS purchases, 
        COALESCE(SUM(revenue),0) AS revenue
    FROM {{ source('reporting','googleads_ad_performance') }}
    WHERE campaign_name ~* 'youtube shorts'
    AND campaign_type_custom = 'Sephora'
    GROUP BY channel, date, date_granularity, market, campaign_name, ad_group_name, product_name)
