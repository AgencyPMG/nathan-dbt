{{ config(
    partition_by={
      "field": "date",
      "data_type": "timestamp"
    },
    cluster_by=["media_channel","campaign_id"]
)}}

WITH raw_data AS (
     SELECT * FROM {{ref('base_adwords_insights_ad')}}
    )

SELECT raw_data.date AS date
	   ,raw_data.source_table AS source_table
	   ,raw_data.account_id AS account_id
	   ,raw_data.account_name AS account_name
	   ,raw_data.campaign_id AS campaign_id
	   ,raw_data.campaign_name AS campaign_name
	   ,raw_data.placement_id AS placement_id
	   ,raw_data.placement_name AS placement_name
	   ,raw_data.ad_id AS ad_id
	   ,raw_data.ad_name AS ad_name
	   ,raw_data.media_channel AS media_channel
	   ,raw_data.platform AS platform_group
	   ,raw_data.platform AS platform
	   ,LOWER( {{ qualify('surveymonkey.display_tactic(raw_data.campaign_name)') }} ) AS tactic
	   ,LOWER( {{ qualify('surveymonkey.omni_strategy(raw_data.media_channel,raw_data.campaign_name,raw_data.placement_name)') }} ) AS strategy
       ,LOWER( {{ qualify('surveymonkey.omni_targeting(surveymonkey.omni_strategy(raw_data.media_channel,raw_data.campaign_name,raw_data.placement_name))') }} ) AS targeting
	   ,LOWER( {{ qualify('surveymonkey.omni_business_unit(raw_data.account_name,raw_data.campaign_name)') }} ) AS business_unit
	   ,LOWER( {{ qualify('surveymonkey.omni_geo_detail(raw_data.campaign_name)') }} ) AS geo_detail
	   ,LOWER( {{ qualify('surveymonkey.adwords_audience_conversion_goal(raw_data.creative_final_urls,surveymonkey.omni_business_unit(raw_data.account_name, raw_data.campaign_name),raw_data.campaign_name)') }} ) AS audience_conversion_goal
       ,raw_data.clicks AS clicks
       ,raw_data.impressions AS impressions
	FROM raw_data
