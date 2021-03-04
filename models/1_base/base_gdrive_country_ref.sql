WITH source AS (
    SELECT * FROM {{source('surveymonkey','gdrive_surveymonkey_country_ref')}}
),

transformed AS(
    SELECT
      CAST((geo_detail) AS {{dbt_utils.type_string()}}) AS geo_detail
      ,CAST((geo_detail_hrn) AS {{dbt_utils.type_string()}}) AS geo_detail_hrn
      ,CAST((geo_group) AS {{dbt_utils.type_string()}}) AS geo_group
      ,CAST((region) AS {{dbt_utils.type_string()}}) AS region
    FROM source
)

SELECT * FROM transformed
