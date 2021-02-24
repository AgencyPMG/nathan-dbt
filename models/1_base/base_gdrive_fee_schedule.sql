WITH source AS (
     SELECT *
     FROM {{source('surveymonkey','gdrive_fee_schedule')}}
    ),

transformed AS (
     SELECT id
            ,LOWER(platform) AS platform
            ,fee_desc
            ,start_date
            ,end_date
            ,fee
            ,ad_type
            ,creative_size
            ,channel
            ,min_impressions
            ,max_impressions
    FROM source
)

SELECT * FROM transformed
