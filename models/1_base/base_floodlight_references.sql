WITH source AS (
    SELECT * FROM {{source('surveymonkey','floodlight_references')}}
),

transformed AS (
    SELECT DISTINCT ref_uvar3_package_id
				,ref_activity_id AS activity_id
				,LOWER(ref_floodlight_config) AS ref_floodlight_config
				,LOWER(ref_conversion_bu) AS ref_conversion_bu
				,LOWER(ref_conversion_type) AS ref_conversion_type
				,LOWER(ref_paid_free) AS ref_paid_free
				,LOWER(ref_conversion_new_vs_repeat) AS ref_conversion_new_vs_repeat
				,LOWER(ref_package_group) AS ref_package_group
				,LOWER(ref_package_name) AS ref_package_name
				,LOWER(ref_sub_type) AS ref_sub_type
    FROM source
)

SELECT * FROM transformed

