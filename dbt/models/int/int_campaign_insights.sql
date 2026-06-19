{{ 
  config(
    materialized = 'ephemeral',
    tags = ['int', 'google', 'campaign']
  ) 
}}

select
    date,
    month,
    year,

    insights.department,
    insights.account,    
    
    insights.customer_id,
    insights.campaign_id,
    
    insights.impressions,
    insights.clicks,
    insights.spend,
    insights.conversions,
    insights.conversion_value,

    campaign.campaign_name,

    case
        when campaign.campaign_status = 'ENABLED' then '🟢'
        when campaign.campaign_status = 'PAUSED'  then '⚪'
        when campaign.campaign_status = 'REMOVED' then '🔴'
        else '❓'
    end as campaign_status,

    campaign.platform,
    campaign.objective,
    campaign.budget_group,
    campaign.region,
    campaign.category_level_1,
    campaign.optimization,
    campaign.track,
    campaign.pillar,
    campaign.`group`

from {{ ref('stg_campaign_insights') }} insights
left join `{{ target.project }}.{{ var('company') }}_dataset_google_api_raw.{{ var('company') }}_table_google_{{ var('department') }}_{{ var('account') }}_campaign_metadata` campaign
    on insights.customer_id = campaign.customer_id
   and insights.campaign_id = campaign.campaign_id