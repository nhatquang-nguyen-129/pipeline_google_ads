{{ 
  config(
    alias = var('company') ~ '_table_google_all_all_campaign_performance',
    partition_by = {
      "field": "date",
      "data_type": "date"
    },
    cluster_by = ["customer_id", "campaign_id"],
    tags = ['mart', 'google', 'campaign']
  ) 
}}

select
    date,
    month,
    year,

    department,
    account,
    
    customer_id,
    campaign_id,
    campaign_name,
    campaign_status,

    impressions,
    clicks,
    spend,
    conversions,
    conversion_value,

    platform,
    objective,
    region,
    budget_group,
    category_level_1,
    optimization,
    track,
    pillar,
    `group`
from {{ ref('int_campaign_insights') }}