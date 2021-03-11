WITH source AS (
    SELECT * FROM {{ source('surveymonkey', 'search_engine_conversion_name_references') }}
),

transformed AS (
  SELECT ref_engine
         ,ref_account_name
         ,ref_goalname
         ,ref_use_in_reporting
         ,ref_conversion_business_unit
         ,ref_conversion_type
         ,ref_conversion_paid_vs_free
         ,ref_conversion_subscription_type
         ,ref_conversion_new_vs_repeat
         ,ref_conversion_package_group
         ,ref_conversion_package_name
         ,ref_notes
  FROM source
)

SELECT * FROM transformed

