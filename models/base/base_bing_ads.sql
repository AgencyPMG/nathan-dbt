WITH source AS (
     SELECT * FROM {{ source('surveymonkey','bing_ads') }}
    ),

transformed AS (
     SELECT date
            ,'surveymonkey.bing_ads' AS source_table
            ,NULL AS account_id
            ,LOWER(accountdescriptivename) AS account_name
            ,CAST((campaignid) AS {{dbt_utils.type_string()}}) AS campaign_id
            ,LOWER(campaignname) AS campaign_name
            ,CAST((adgroupid) AS {{dbt_utils.type_string()}}) AS placement_id
            ,LOWER(adgroupname) AS placement_name
            ,CAST('n/a' AS {{dbt_utils.type_string()}}) AS ad_id
            ,CAST('n/a' AS {{dbt_utils.type_string()}}) AS ad_name
            ,LOWER(adnetworktype1) AS media_channel
            ,LOWER(adnetworktype2) AS platform
    	      ,LOWER(creativefinalurls) AS creative_final_urls
            ,impressions
            ,clicks
            ,cost/1000000 AS media_spend
    FROM source
)

SELECT *
FROM transformed
