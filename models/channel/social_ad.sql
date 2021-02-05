WITH facebook AS (
    SELECT * FROM {{ref('social_facebook')}}
),
linkedin AS (
    SELECT * FROM {{ref('social_linkedin')}}
),
country AS (
   SELECT * FROM {{ref('base_gdrive_country_ref')}}
)

SELECT
   STAK.date
   ,STAK.source_table
   ,STAK.account_id
   ,STAK.account_name
   ,STAK.campaign_id
   ,STAK.campaign_name
   ,STAK.placement_id
   ,STAK.placement_name
   ,STAK.ad_id
   ,STAK.ad_name
   ,{{qualify('surveymonkey.taxon_media_channel(STAK.media_channel)')}} AS media_channel
   ,{{qualify('surveymonkey.taxon_platform_group(STAK.platform_group)')}} AS platform_group
   ,{{qualify('surveymonkey.taxon_platform(STAK.platform)')}} AS platform
   ,STAK.tactic
   ,STAK.strategy
   ,STAK.targeting
   ,STAK.conversion_goal
   ,{{qualify('surveymonkey.omnichannel_business_unit_group(STAK.business_unit)')}} AS business_unit_group
   ,STAK.business_unit
   ,LOWER({{qualify('surveymonkey.omnichannel_conversion_business_unit_group(STAK.conversion_business_unit)')}}) AS conversion_business_unit_group
   ,CASE
      WHEN LOWER(STAK.conversion_business_unit) LIKE '%enterprise%' AND LOWER(STAK.business_unit) = 'enterprise/integrations emea' THEN 'enterprise/integrations emea'
      WHEN STAK.conversion_business_unit = '' THEN LOWER(STAK.business_unit)
      WHEN STAK.conversion_business_unit = 'n/a' THEN LOWER(STAK.business_unit)
      ELSE LOWER(STAK.conversion_business_unit)
    END AS conversion_business_unit
   ,CASE
      WHEN LOWER(conversion_business_unit) LIKE '%na%' OR {{qualify('surveymonkey.omnichannel_conversion_business_unit_group(STAK.conversion_business_unit)')}} = {{qualify('surveymonkey.omnichannel_business_unit_group(STAK.business_unit)')}} THEN 'matched'
      WHEN LOWER(STAK.conversion_business_unit) = LOWER(STAK.business_unit) THEN 'matched'
      WHEN LOWER(STAK.conversion_business_unit) = '' THEN 'matched'
    ELSE 'unmatched'
    END AS conversion_bu_group_matched
   ,CASE
      WHEN LOWER(STAK.conversion_business_unit) LIKE '%enterprise%' AND LOWER(STAK.business_unit) = 'enterprise/integrations emea' THEN 'matched'
      WHEN LOWER(STAK.conversion_business_unit) = LOWER(STAK.business_unit) THEN 'matched'
      WHEN LOWER(STAK.conversion_business_unit) = '' THEN 'matched'
	  WHEN LOWER(STAK.conversion_business_unit) = 'n/a' THEN 'matched'
      ELSE 'unmatched'
    END AS conversion_bu_matched
   ,STAK.conversion_source
   ,STAK.conversion_event_name
   ,STAK.conversion_type
   ,STAK.conversion_paid_vs_free
   ,STAK.conversion_customer_type
   ,STAK.conversion_new_vs_repeat
   ,STAK.conversion_package_group
   ,STAK.conversion_package_name
   ,STAK.conversion_subscription_type
   ,STAK.targeting_type
   ,STAK.audience_targeting
   ,STAK.placement
   ,STAK.placement_size
   ,STAK.device
   ,STAK.costing
   ,STAK.ad_type
   ,STAK.creative_description
   ,STAK.messaging_type
   ,STAK.imagery_type
   ,STAK.headline_cta
   ,STAK.creative_size_video
   ,STAK.month_uploaded
   ,STAK.version
   ,STAK.geo_detail
   ,LOWER(GEO_REF.geo_group) AS geo_group
   ,LOWER(GEO_REF.geo_detail_hrn) AS geo_detail_hrn
   ,LOWER(GEO_REF.region) AS region
   ,LOWER({{qualify('surveymonkey.omni_budget_item(STAK.business_unit, STAK.media_channel, STAK.platform, STAK.campaign_name, STAK.placement_name, STAK.geo_detail, STAK.conversion_goal)')}}) AS budget_item
   ,SUM(COALESCE(STAK.impressions,0)) AS impressions
   ,SUM(COALESCE(STAK.clicks,0)) AS clicks
   ,SUM(COALESCE(STAK.media_spend,0)) AS media_spend
   ,SUM(COALESCE(STAK.tracking_fee,0)) AS tracking_fee
   ,SUM(COALESCE(STAK.agency_management_fee,0)) AS agency_management_fee
   ,SUM(COALESCE(STAK.agency_tech_fee,0)) AS agency_tech_fee
   ,SUM(COALESCE(STAK.agency_management_fee,0) + COALESCE(STAK.agency_tech_fee,0) + COALESCE(STAK.tracking_fee,0) + COALESCE(STAK.media_spend,0)) AS total_spend
   ,SUM(COALESCE(STAK.click_through_conversions,0)) AS click_through_conversions
   ,SUM(COALESCE(STAK.view_through_conversions,0)) AS view_through_conversions
   ,SUM(COALESCE(adjusted_view_through_conversions,0)) AS adjusted_view_through_conversions
   ,SUM(COALESCE(STAK.total_conversions,0)) AS total_conversions
   ,SUM(COALESCE(adjusted_total_conversions,0)) AS adjusted_total_conversions
   ,SUM(COALESCE(STAK.click_through_booking,0)) AS click_through_booking
   ,SUM(COALESCE(STAK.view_through_booking,0)) AS view_through_booking
   ,SUM(COALESCE(adjusted_view_through_booking,0)) AS adjusted_view_through_booking
   ,SUM(COALESCE(STAK.total_booking,0)) AS total_booking
   ,SUM(COALESCE(adjusted_total_booking,0)) AS adjusted_total_booking
   ,CASE
       WHEN STAK.conversion_subscription_type = 'annual' THEN SUM(COALESCE(STAK.click_through_booking,0))/12
       ELSE SUM(COALESCE(STAK.click_through_booking,0))
    END AS click_through_revenue
   ,CASE
       WHEN STAK.conversion_subscription_type = 'annual' THEN SUM(COALESCE(STAK.view_through_booking,0))/12
       ELSE SUM(COALESCE(STAK.view_through_booking,0))
    END AS view_through_revenue
   ,CASE
       WHEN STAK.conversion_subscription_type = 'annual' THEN SUM(COALESCE(STAK.adjusted_view_through_booking,0))/12
       ELSE SUM(COALESCE(STAK.adjusted_view_through_booking,0))
    END AS adjusted_view_through_revenue
   ,CASE
       WHEN STAK.conversion_subscription_type = 'annual' THEN SUM(COALESCE(STAK.total_booking,0))/12
       ELSE SUM(COALESCE(STAK.total_booking,0))
    END AS total_revenue
   ,CASE
       WHEN STAK.conversion_subscription_type = 'annual' THEN SUM(COALESCE(STAK.adjusted_total_booking,0))/12
       ELSE SUM(COALESCE(STAK.adjusted_total_booking,0))
    END AS adjusted_total_revenue
   ,SUM(COALESCE(STAK.click_through_seats,0)) AS click_through_seats
   ,SUM(COALESCE(adjusted_view_through_seats,0)) AS adjusted_view_through_seats
   ,SUM(COALESCE(adjusted_total_seats,0)) AS adjusted_total_seats
FROM (
  SELECT date
       ,source_table
       ,account_id
       ,account_name
       ,campaign_id
       ,campaign_name
       ,placement_id
       ,placement_name
       ,ad_id
       ,ad_name
       ,media_channel
       ,platform_group
       ,platform
       ,tactic
       ,strategy
       ,targeting
       ,CASE
          WHEN LOWER(business_unit) LIKE '%audience%' THEN audience_conversion_goal
          ELSE {{qualify('surveymonkey.omni_conversion_goal(surveymonkey.omni_business_unit_group(business_unit))')}}
        END AS conversion_goal
       ,business_unit
       ,geo_detail
       ,audience_conversion_goal
       ,conversion_source
       ,conversion_event_name AS conversion_event_name
       ,conversion_business_unit
       ,conversion_type
       ,conversion_paid_vs_free
       ,'n/a' AS conversion_customer_type
       ,conversion_new_vs_repeat
       ,conversion_package_group
       ,'n/a' AS conversion_package_name
       ,conversion_subscription_type
       ,LOWER({{split_part(string_text='placement_name', delimiter_text='_', part_number=10)}}) AS targeting_type
       ,LOWER({{split_part(string_text='placement_name', delimiter_text='_', part_number=11)}}) AS audience_targeting
       ,LOWER({{split_part(string_text='placement_name', delimiter_text='_', part_number=12)}}) AS placement
       ,LOWER({{split_part(string_text='placement_name', delimiter_text='_', part_number=13)}}) AS placement_size
       ,LOWER({{split_part(string_text='placement_name', delimiter_text='_', part_number=14)}}) AS device
       ,LOWER({{split_part(string_text='placement_name', delimiter_text='_', part_number=15)}}) AS costing
       ,LOWER({{split_part(string_text='ad_name', delimiter_text='_', part_number=1)}}) AS ad_type
       ,LOWER({{split_part(string_text='ad_name', delimiter_text='_', part_number=2)}}) AS creative_description
       ,LOWER({{split_part(string_text='ad_name', delimiter_text='_', part_number=3)}}) AS messaging_type
       ,LOWER({{split_part(string_text='ad_name', delimiter_text='_', part_number=4)}}) AS imagery_type
       ,LOWER({{split_part(string_text='ad_name', delimiter_text='_', part_number=5)}}) AS headline_cta
       ,LOWER({{split_part(string_text='ad_name', delimiter_text='_', part_number=6)}}) AS creative_size_video
       ,LOWER({{split_part(string_text='ad_name', delimiter_text='_', part_number=7)}}) AS month_uploaded
       ,LOWER({{split_part(string_text='ad_name', delimiter_text='_', part_number=8)}}) AS version
       ,0 AS click_through_seats
       ,0 AS view_through_seats
       ,0 AS adjusted_view_through_seats
       ,0 AS adjusted_total_seats
       ,click_through_conversions
       ,view_through_conversions
       ,adjusted_view_through_conversions
       ,total_conversions
       ,adjusted_total_conversions
       ,click_through_booking
       ,view_through_booking
       ,adjusted_view_through_booking
       ,total_booking
       ,adjusted_total_booking
       ,clicks
       ,impressions
       ,media_spend
       ,tracking_fee
       ,agency_tech_fee
       ,agency_management_fee
  FROM facebook
----------------------------------------------------------------------------------------------------------------------
 UNION ALL
----------------------------------------------------------------------------------------------------------------------
  SELECT date
       ,source_table
       ,account_id
       ,account_name
       ,campaign_id
       ,campaign_name
       ,NULL placement_id
       ,NULL placement_name
       ,creative_id AS ad_id
       ,creative_name AS ad_name
       ,media_channel
       ,platform_group
       ,platform
       ,tactic
       ,strategy
       ,targeting
       ,CASE
            WHEN LOWER(business_unit) LIKE '%audience%' THEN audience_conversion_goal
              ELSE {{qualify('surveymonkey.omni_conversion_goal(surveymonkey.omni_business_unit_group(business_unit))')}}
          END AS conversion_goal
       ,business_unit
       ,geo_detail
       ,audience_conversion_goal
       ,conversion_source
       ,conversion_event_name
       ,conversion_business_unit
       ,conversion_type
       ,conversion_paid_vs_free
       ,'n/a' AS conversion_customer_type
       ,conversion_new_vs_repeat
       ,conversion_package_group
       ,'n/a' AS conversion_package_name
       ,conversion_subscription_type
       ,LOWER({{split_part(string_text='campaign_name', delimiter_text='_', part_number=10)}}) AS targeting_type
       ,LOWER({{split_part(string_text='campaign_name', delimiter_text='_', part_number=11)}}) AS audience_targeting
       ,LOWER({{split_part(string_text='campaign_name', delimiter_text='_', part_number=12)}}) AS placement
       ,LOWER({{split_part(string_text='campaign_name', delimiter_text='_', part_number=13)}}) AS placement_size
       ,LOWER({{split_part(string_text='campaign_name', delimiter_text='_', part_number=14)}}) AS device
       ,LOWER({{split_part(string_text='campaign_name', delimiter_text='_', part_number=15)}}) AS costing
       ,CASE
            WHEN LOWER(creative_name) LIKE '%delete%' THEN 'n/a'
            ELSE LOWER({{split_part(string_text='creative_name', delimiter_text='_', part_number=1)}})
        END AS ad_type
       ,CASE
            WHEN LOWER(creative_name) LIKE '%delete%' THEN 'n/a'
            ELSE LOWER({{split_part(string_text='creative_name', delimiter_text='_', part_number=2)}})
        END AS creative_description
       ,CASE
            WHEN LOWER(creative_name) LIKE '%delete%' THEN 'n/a'
            ELSE LOWER({{split_part(string_text='creative_name', delimiter_text='_', part_number=3)}})
        END AS messaging_type
       ,CASE
            WHEN LOWER(creative_name) LIKE '%delete%' THEN 'n/a'
            ELSE LOWER({{split_part(string_text='creative_name', delimiter_text='_', part_number=4)}})
        END AS imagery_type
       ,CASE
            WHEN LOWER(creative_name) LIKE '%delete%' THEN 'n/a'
            ELSE LOWER({{split_part(string_text='creative_name', delimiter_text='_', part_number=5)}})
        END AS headline_cta
       ,CASE
            WHEN LOWER(creative_name) LIKE '%delete%' THEN 'n/a'
            ELSE LOWER({{split_part(string_text='creative_name', delimiter_text='_', part_number=6)}})
        END AS creative_size_video
       ,CASE
            WHEN LOWER(creative_name) LIKE '%delete%' THEN 'n/a'
            ELSE LOWER({{split_part(string_text='creative_name', delimiter_text='_', part_number=7)}})
        END AS month_uploaded
       ,CASE
            WHEN LOWER(creative_name) LIKE '%delete%' THEN 'n/a'
            ELSE LOWER({{split_part(string_text='creative_name', delimiter_text='_', part_number=8)}})
        END AS version
       ,0 AS click_through_seats
       ,0 AS view_through_seats
       ,0 AS adjusted_view_through_seats
       ,0 AS adjusted_total_seats
       ,click_through_conversions
       ,view_through_conversions
       ,adjusted_view_through_conversions
       ,total_conversions
       ,adjusted_total_conversions
       ,click_through_booking
       ,view_through_booking
       ,0 AS adjusted_view_through_booking
       ,total_booking
       ,0 AS adjusted_total_booking
       ,clicks
       ,impressions
       ,media_spend
       ,tracking_fee
       ,agency_tech_fee
       ,agency_management_fee
  FROM linkedin

) AS STAK

LEFT JOIN country AS GEO_REF
    ON LOWER(STAK.geo_detail) = GEO_REF.geo_detail

GROUP BY date
    ,source_table
    ,account_id
    ,account_name
    ,campaign_id
    ,campaign_name
    ,placement_id
    ,placement_name
    ,ad_id
    ,ad_name
    ,media_channel
    ,platform_group
    ,platform
    ,tactic
    ,strategy
    ,targeting
    ,conversion_goal
    ,business_unit_group
    ,business_unit
    ,conversion_business_unit_group
    ,conversion_business_unit
    ,conversion_bu_group_matched
    ,conversion_bu_matched
    ,conversion_source
    ,conversion_event_name
    ,conversion_type
    ,conversion_paid_vs_free
    ,conversion_customer_type
    ,conversion_new_vs_repeat
    ,conversion_package_group
    ,conversion_package_name
    ,conversion_subscription_type
    ,targeting_type
    ,audience_targeting
    ,placement
    ,placement_size
    ,device
    ,costing
    ,ad_type
    ,creative_description
    ,messaging_type
    ,imagery_type
    ,headline_cta
    ,creative_size_video
    ,month_uploaded
    ,version
    ,STAK.geo_detail
    ,GEO_REF.geo_group
    ,GEO_REF.geo_detail_hrn
    ,GEO_REF.region
    ,budget_item
