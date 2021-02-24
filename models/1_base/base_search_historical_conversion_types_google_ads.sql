WITH source AS (
    SELECT * FROM {{ source('surveymonkey', 'search_historical_conversion_types_google_ads') }}
),

transformed AS (
  SELECT day AS date
     ,'surveymonkey.search_historical_conversion_types_google_ads' AS source_table
     ,CAST((NULL) AS {{dbt_utils.type_string()}}) AS account_id
     ,account AS account_name
     ,campaign_id
     ,campaign AS campaign_name
     ,'paid search' AS media_channel
     ,'google ads' AS platform_group
     ,'google ads' AS platform
     ,conversions
     ,conversion_value
     ,conversion_action
     ,campaign_type
  FROM source
)

SELECT * FROM transformed
WHERE LOWER(campaign_type) = 'search'
