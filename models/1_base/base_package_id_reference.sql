WITH source AS (
    SELECT * FROM {{source('surveymonkey','package_id_reference')}}
),

transformed AS (
    SELECT CAST((packageid) AS {{dbt_utils.type_string()}}) AS package_id
           ,LOWER(description) AS description
           ,CAST((annual) AS {{dbt_utils.type_string()}}) AS annual
           ,CAST((monthly) AS {{dbt_utils.type_string()}}) AS monthly
           ,paid
           ,LOWER(desc1) AS desc1
           ,packagerank AS package_rank
           ,includeinreports AS include_in_reports
           ,allowsteams
           ,isplatinum AS is_platinum
           ,packagegroup AS package_group
           ,packagegroupwithids AS packagegroup_with_ids
    FROM source
)
SELECT * FROM transformed
