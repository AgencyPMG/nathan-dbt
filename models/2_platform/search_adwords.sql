{{ config(
    partition_by={
      "field": "date",
      "data_type": "timestamp"
    },
    cluster_by="campaign_id"
)}}

WITH raw_data AS (
    SELECT * FROM {{ref('base_core_search_ad')}}
    WHERE LOWER(platform) = 'google adwords'
),

fees AS (
    SELECT * FROM {{ref('base_gdrive_fee_schedule')}}
)

SELECT
    raw_data.date
    ,raw_data.source_table
    ,raw_data.account_id
    ,raw_data.account_name
    ,raw_data.campaign_id
    ,raw_data.campaign_name
    ,raw_data.placement_id
    ,raw_data.placement_name
    ,raw_data.ad_id
    ,raw_data.ad_name
    ,raw_data.media_channel
    ,raw_data.platform AS platform_group
    ,raw_data.platform AS platform
    ,LOWER( {{ qualify ('surveymonkey.search_tactic(raw_data.campaign_name)')}} ) AS tactic
    ,CAST(LOWER({{qualify('surveymonkey.omni_strategy(raw_data.media_channel,raw_data.campaign_name,raw_data.placement_name)')}})  AS {{ dbt_utils.type_string() }}) AS strategy
    ,LOWER({{ qualify ('surveymonkey.omni_targeting(LOWER(surveymonkey.omni_strategy(raw_data.media_channel,raw_data.campaign_name,raw_data.placement_name)))')}} ) AS targeting
    ,LOWER( {{ qualify ('surveymonkey.omni_business_unit(raw_data.account_name,raw_data.campaign_name)')}}) AS business_unit
    ,LOWER( {{ qualify ('surveymonkey.omni_geo_detail(raw_data.campaign_name)')}} ) AS geo_detail
    ,LOWER( {{ qualify ('surveymonkey.adwords_audience_conversion_goal(raw_data.creative_final_urls,surveymonkey.omni_business_unit(raw_data.account_name, raw_data.campaign_name),raw_data.campaign_name)')}}) AS audience_conversion_goal
    ,raw_data.clicks
    ,raw_data.impressions
    ,raw_data.media_spend
    ,raw_data.media_spend * FEES_TRACK.fee AS tracking_fee
    ,COALESCE(raw_data.media_spend * FEE_TECH.fee,0) AS agency_tech_fee
    ,COALESCE(raw_data.media_spend * FEE_MGMNT.fee,0) AS agency_management_fee
    ,{{ time_current_zone() }} AS last_updated

  FROM raw_data

  LEFT JOIN fees FEES_TRACK
			ON LOWER(raw_data.media_channel) = LOWER(FEES_TRACK.channel)
				AND LOWER(raw_data.platform) = LOWER(FEES_TRACK.platform)
				AND raw_data.date BETWEEN FEES_TRACK.start_date AND FEES_TRACK.end_date
				AND FEES_track.fee_desc = 'tracking'

  LEFT JOIN fees FEE_TECH
			ON raw_data.date BETWEEN FEE_TECH.start_date AND FEE_TECH.end_date
				AND LOWER(raw_data.media_channel) = LOWER(FEE_TECH.channel)
				AND LOWER(raw_data.platform) = LOWER(FEE_TECH.platform)
				AND FEE_TECH.fee_desc = 'tech'

  LEFT JOIN fees FEE_MGMNT
			ON raw_data.date BETWEEN FEE_MGMNT.start_date AND FEE_MGMNT.end_date
				AND LOWER(raw_data.media_channel) = LOWER(FEE_MGMNT.channel)
				AND LOWER(raw_data.platform) = (FEE_MGMNT.platform)
				AND FEE_MGMNT.fee_desc = 'management'
