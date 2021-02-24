WITH source AS (
    SELECT * FROM {{ source('surveymonkey', 'search_historical_conversion_types_bing') }}
),

transformed AS (
  SELECT day AS date
       ,'surveymonkey.search_historical_conversion_types_bing' AS source_table
       ,CAST((NULL) AS {{dbt_utils.type_string()}}) AS account_id
       ,account_name
       ,campaign_id
       ,campaign AS campaign_name
       ,'paid search' AS media_channel
       ,'bing ads' AS platform_group
       ,'bing ads' AS platform
       ,conv AS conversions
       ,revenue AS conversion_value
       ,goalname AS conversion_action
       ,NULL AS sa360_custom_conversion
       ,campaign_type
  FROM source
)

SELECT * FROM transformed
WHERE LOWER(campaign_type) = 'search'
  AND date <= '2019-10-25'
