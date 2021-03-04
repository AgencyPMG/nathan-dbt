WITH source AS (
    SELECT * FROM {{source('surveymonkey','categorization_override_dcm')}}
),

transformed AS (
    SELECT LOWER(cm_advertiser) AS advertiser_name
           ,cm_advertiser_id AS advertiser_id
           ,LOWER(cm_campaign) AS campaign_name
           ,cm_campaign_id AS campaign_id
           ,LOWER(cm_site) AS site
           ,cm_site_ide AS site_id
           ,LOWER(cm_placement) AS placement_name
           ,cm_placement_id AS placement_id
           ,LOWER(ref_business_unit) AS business_unit
           ,LOWER(ref_channel) AS media_channel
           ,LOWER(ref_platform) AS platform_group
           ,LOWER(ref_platform_detail) AS platform
           ,LOWER(ref_tactic) AS tactic
           ,LOWER(ref_geo) AS geo_detail
    FROM source
)

SELECT * FROM transformed