WITH source AS (
    SELECT * FROM surveymonkey_core.search_ad
),

transformed AS (
  SELECT date
      ,'surveymonkey_core.search_ad' AS source_table
      ,account_id
      ,LOWER(account_name) AS account_name
      ,campaign_id
      ,LOWER(campaign_name) AS campaign_name
      ,adgroup_id AS placement_id
      ,LOWER(adgroup_name) AS placement_name
      ,ad_id
      ,LOWER(ad_description) AS ad_name
      ,CAST('paid search' AS {{ dbt_utils.type_string() }} ) AS media_channel
      ,CAST(LOWER(platform) AS {{ dbt_utils.type_string() }} ) AS platform
      ,LOWER(final_urls) AS creative_final_urls
      ,impressions
      ,clicks
      ,cost AS media_spend
  FROM source
)

SELECT * FROM transformed
