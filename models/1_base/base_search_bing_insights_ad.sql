WITH source AS (
    SELECT * FROM {{ source('surveymonkey', 'bing_insights_ad') }}
),

transformed AS (
  SELECT
     CAST(timeperiod AS TIMESTAMP) AS date
     ,'surveymonkey.bing_insights_ad' AS source_table
     ,accountid AS account_id
     ,LOWER(accountname) AS account_name
     ,campaignid AS campaign_id
     ,LOWER(campaignname) AS campaign_name
     ,adgroupid AS placement_id
     ,LOWER(adgroupname) AS placement_name
     ,adid AS ad_id
     ,LOWER(addescription) AS ad_name						--need to confirm this confirm
     ,CAST('paid search' AS {{ dbt_utils.type_string() }} ) AS media_channel
     ,'microsoft bing' AS platform_group
     ,CAST(
        CASE
          WHEN LOWER(network) LIKE 'audience' THEN 'audience network'
          ELSE 'microsoft bing'
        END
        AS {{ dbt_utils.type_string() }}
        ) AS platform
     ,LOWER(finalurl) AS creative_final_urls
     ,impressions
     ,clicks
     ,CAST((spend) AS {{dbt_utils.type_float()}}) AS media_spend

  FROM source  		--this table is only added to get audience network data from bing(core data does not have this data)
)

SELECT * FROM transformed
