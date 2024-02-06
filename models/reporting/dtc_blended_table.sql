{{ config (
    alias = target.database + '_dtc_blended_table'
)}}

    (SELECT 'Facebook' as channel, date, date_granularity, market, campaign_name, ad_approach, ad_name,
        COALESCE(SUM(spend),0) AS spend, COALESCE(SUM(CASE WHEN adset_name !~* 'past purchasers' THEN spend END),0) as first_spend,
        COALESCE(SUM(CASE WHEN adset_name ~* 'past purchasers' THEN spend END),0) as repeat_spend,
        COALESCE(SUM(impressions),0) AS impressions, COALESCE(SUM(link_clicks),0) AS clicks, COALESCE(SUM(purchases),0) AS paid_purchases, 
        COALESCE(SUM(revenue),0) AS paid_revenue, 0 as shopify_purchases, 0 as shopify_revenue, 0 as shopify_first_purchases, 0 as shopify_first_revenue,
        0 as shopify_repeat_revenue, 0 as shopify_repeat_purchases
    FROM {{ source('reporting','facebook_ad_performance') }}
    WHERE account_id = '152712828589300'
    AND campaign_type_custom = 'DTC'
    GROUP BY channel, date, date_granularity, market, campaign_name, ad_approach, ad_name)
    
    UNION ALL
    
    (SELECT 'TikTok' as channel, date, date_granularity, market, campaign_name, ad_approach, ad_name,
        COALESCE(SUM(spend),0) AS spend, 0 as first_spend, 0 as repeat_spend, COALESCE(SUM(impressions),0) AS impressions, COALESCE(SUM(clicks),0) AS clicks,
        COALESCE(SUM(purchases),0) AS paid_purchases, COALESCE(SUM(revenue),0) AS paid_revenue, 0 as shopify_purchases, 0 as shopify_revenue, 0 as shopify_first_purchases, 
        0 as shopify_first_revenue, 0 as shopify_repeat_revenue, 0 as shopify_repeat_purchases
    FROM {{ source('reporting','tiktok_ad_performance') }}
    WHERE campaign_type_custom = 'DTC'
    GROUP BY channel, date, date_granularity, market, campaign_name, ad_approach, ad_name)
    
    UNION ALL
    
    (SELECT 'Google' as channel, date, date_granularity, market, campaign_name,  NULL as ad_approach, NULL as ad_name,
        COALESCE(SUM(spend),0) AS spend, 0 as first_spend, 0 as repeat_spend, COALESCE(SUM(impressions),0) AS impressions, COALESCE(SUM(clicks),0) AS clicks,
        COALESCE(SUM(purchases),0) AS paid_purchases, COALESCE(SUM(revenue),0) AS paid_revenue, 0 as shopify_purchases, 0 as shopify_revenue, 0 as shopify_first_purchases, 
        0 as shopify_first_revenue, 0 as shopify_repeat_revenue, 0 as shopify_repeat_purchases
    FROM {{ source('reporting','googleads_campaign_performance') }}
    WHERE campaign_name !~* 'youtube'
    GROUP BY channel, date, date_granularity, market, campaign_name, ad_approach, ad_name)
    
    UNION ALL
    
    (SELECT 'Youtube' as channel, date, date_granularity, market, campaign_name,  NULL as ad_approach, NULL as ad_name,
        COALESCE(SUM(spend),0) AS spend, 0 as first_spend, 0 as repeat_spend, COALESCE(SUM(impressions),0) AS impressions, COALESCE(SUM(clicks),0) AS clicks,
        COALESCE(SUM(purchases),0) AS paid_purchases, COALESCE(SUM(revenue),0) AS paid_revenue, 0 as shopify_purchases, 0 as shopify_revenue, 0 as shopify_first_purchases, 
        0 as shopify_first_revenue, 0 as shopify_repeat_revenue, 0 as shopify_repeat_purchases
    FROM {{ source('reporting','googleads_campaign_performance') }}
    WHERE campaign_name ~* 'youtube'
    GROUP BY channel, date, date_granularity, market, campaign_name, ad_approach, ad_name)
    
    UNION ALL
    
    (SELECT 'Shopify' as channel, date, date_granularity, 'US' as market, NULL as campaign_name, NULL as ad_approach, NULL as ad_name,
        0 AS spend, 0 as first_spend, 0 as repeat_spend, 0 AS impressions, 0 AS clicks, 0 AS paid_purchases, 0 AS paid_revenue, COALESCE(SUM(orders),0) as shopify_purchases, 
        COALESCE(SUM(gross_sales),0) as shopify_revenue, COALESCE(SUM(first_orders),0) as shopify_first_purchases, 
        COALESCE(SUM(first_order_gross_sales),0) as shopify_first_revenue, COALESCE(SUM(repeat_orders),0) as shopify_repeat_purchases, 
        COALESCE(SUM(repeat_order_gross_sales),0) as shopify_repeat_revenue
    FROM {{ source('reporting','shopify_us_sales') }}
    GROUP BY date, date_granularity, market, channel, campaign_name, ad_approach, ad_name
    UNION ALL
    SELECT 'Shopify' as channel, date, date_granularity, 'CA' as market, NULL as campaign_name, NULL as ad_approach, NULL as ad_name,
        0 AS spend, 0 as first_spend, 0 as repeat_spend, 0 AS impressions, 0 AS clicks, 0 AS paid_purchases, 0 AS paid_revenue, COALESCE(SUM(orders),0) as shopify_purchases, 
        COALESCE(SUM(gross_sales),0) as shopify_revenue, COALESCE(SUM(first_orders),0) as shopify_first_purchases, 
        COALESCE(SUM(first_order_gross_sales),0) as shopify_first_revenue, COALESCE(SUM(repeat_orders),0) as shopify_repeat_purchases, 
        COALESCE(SUM(repeat_order_gross_sales),0) as shopify_repeat_revenue
    FROM {{ source('reporting','shopify_ca_sales') }}
    GROUP BY date, date_granularity, market, channel, campaign_name, ad_approach, ad_name)
