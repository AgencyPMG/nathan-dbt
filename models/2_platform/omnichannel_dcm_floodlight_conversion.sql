{{ config(
    partition_by={
      "field": "date",
      "data_type": "timestamp"
    },
    cluster_by=["media_channel","platform","campaign_id"]
)}}

WITH raw_data AS (
    SELECT * FROM {{ref('base_cm_floodlight_9615939')}}
),
cat_dcm AS (
    SELECT * FROM {{ref('base_categorization_override_dcm')}}
    ),
fld_ref AS (
    SELECT * FROM {{ref('base_floodlight_references')}}
    ),
pck_ref AS (
    SELECT * FROM {{ref('base_package_id_reference')}}
    ),
exch AS (
    SELECT * FROM {{ref('base_exchange_rates')}}
    ),
fees AS (
    SELECT * FROM {{ref('base_gdrive_fee_schedule')}}
)
SELECT MANIP_DATA.date
       ,MANIP_DATA.source_table
       ,MANIP_DATA.account_id
       ,MANIP_DATA.account_name
       ,MANIP_DATA.campaign_id
       ,MANIP_DATA.campaign_name
       ,MANIP_DATA.placement_id
       ,MANIP_DATA.placement_name
       ,MANIP_DATA.ad_id
       ,MANIP_DATA.ad_name
       ,MANIP_DATA.media_channel
       ,MANIP_DATA.platform_group
       ,MANIP_DATA.platform
       ,MANIP_DATA.tactic
	   ,LOWER( {{ qualify('surveymonkey.omni_strategy(MANIP_DATA.media_channel,MANIP_DATA.campaign_name,MANIP_DATA.placement_name)') }} ) AS strategy
	   ,LOWER( {{ qualify('surveymonkey.omni_targeting(LOWER(surveymonkey.omni_strategy(MANIP_DATA.media_channel,MANIP_DATA.campaign_name,MANIP_DATA.placement_name)))') }} ) AS targeting
       ,MANIP_DATA.business_unit
	   ,MANIP_DATA.geo_detail
	   ,MANIP_DATA.audience_conversion_goal
	   ,MANIP_DATA.conversion_source
	   ,MANIP_DATA.conversion_event_name
	   ,MANIP_DATA.conversion_business_unit
	   ,MANIP_DATA.conversion_type
	   ,MANIP_DATA.conversion_paid_vs_free
	   ,MANIP_DATA.conversion_customer_type
	   ,MANIP_DATA.conversion_new_vs_repeat
	   ,MANIP_DATA.conversion_package_group
	   ,MANIP_DATA.conversion_package_name
	   ,MANIP_DATA.conversion_subscription_type
	   ,MANIP_DATA.click_through_seats
	   ,MANIP_DATA.view_through_seats
	   ,MANIP_DATA.view_through_seats * fees.fee AS adjusted_view_through_seats
	   ,COALESCE((MANIP_DATA.click_through_seats) + (MANIP_DATA.view_through_seats) * fees.fee,0) AS adjusted_total_seats
	   ,MANIP_DATA.click_through_conversions
	   ,MANIP_DATA.view_through_conversions
	   ,COALESCE(MANIP_DATA.view_through_conversions * fees.fee,0) AS adjusted_view_through_conversions
	   ,MANIP_DATA.total_conversions
	   ,COALESCE(MANIP_DATA.view_through_conversions * fees.fee + MANIP_DATA.click_through_conversions,0) AS adjusted_total_conversions
	   ,MANIP_DATA.click_through_booking
	   ,MANIP_DATA.view_through_booking
	   ,COALESCE(MANIP_DATA.view_through_booking * fees.fee,0) AS adjusted_view_through_booking
	   ,MANIP_DATA.total_booking
	   ,COALESCE(MANIP_DATA.view_through_booking * fees.fee + MANIP_DATA.click_through_booking,0) AS adjusted_total_booking
	   ,0 AS impressions
	   ,0 AS clicks
	   ,0 AS media_spend
	   ,0 AS agency_tech_fee
	   ,0 AS agency_management_fee
       ,{{time_current_zone()}} AS last_updated
FROM (
SELECT raw_data.date
	   ,raw_data.source_table
	   ,raw_data.account_id
	   ,raw_data.account_name
	   ,raw_data.campaign_id
	   ,raw_data.campaign_name
	   ,raw_data.placement_id
	   ,raw_data.placement_name
	   ,raw_data.ad_id
	   ,raw_data.ad_name
       ,LOWER(COALESCE(cat_dcm.media_channel,{{ qualify('surveymonkey.omni_dcm_channel(raw_data.campaign_name || raw_data.campaign,raw_data.placement_name)') }} )) AS media_channel
	   ,LOWER(COALESCE(cat_dcm.platform,{{ qualify('surveymonkey.omnichannel_platform_details(raw_data.site)') }} )) AS platform_group
	   ,LOWER(COALESCE(cat_dcm.platform,{{ qualify('surveymonkey.omnichannel_platform_details(raw_data.site)') }} )) AS platform
	   ,LOWER(COALESCE(cat_dcm.tactic,CASE
		  		 		 			WHEN LOWER( {{ qualify('surveymonkey.omni_dcm_channel(raw_data.campaign_name || raw_data.campaign,raw_data.placement_name)') }} ) = 'paid search' THEN {{ qualify('surveymonkey.search_tactic(raw_data.campaign_name)') }}
		  		 		 			WHEN LOWER( {{ qualify('surveymonkey.omni_dcm_channel(raw_data.campaign_name || raw_data.campaign,raw_data.placement_name)') }} ) = 'display' OR LOWER( {{ qualify('surveymonkey.omni_dcm_channel(raw_data.campaign_name || raw_data.campaign,raw_data.placement_name)') }} ) = 'video' THEN {{ qualify('surveymonkey.display_tactic(raw_data.campaign_name)') }}
		  		 		 			ELSE 'prospecting'
		  		 		 		 END)) AS tactic
	   ,LOWER(COALESCE(cat_dcm.business_unit,{{ qualify('surveymonkey.omni_business_unit(raw_data.account_name,raw_data.campaign_name)') }} )) AS business_unit
	   ,LOWER({{ qualify('surveymonkey.omni_geo_detail(raw_data.campaign_name||raw_data.campaign)') }} ) AS geo_detail
	   ,{{ qualify('surveymonkey.omnichannel_floodlight_audience_conversion_goal(COALESCE(cat_dcm.business_unit,surveymonkey.omni_business_unit(raw_data.account_name,raw_data.campaign_name)),raw_data.activity_id)') }} AS audience_conversion_goal
	   ,'floodlight' AS conversion_source
	   ,raw_data.activity || '[' || raw_data.activity_id || ']' AS conversion_event_name
	   ,LOWER(COALESCE(fld_ref.ref_conversion_bu,{{split_part('raw_data.activity',':',1)}})) AS conversion_business_unit
 	   ,LOWER(COALESCE(fld_ref.ref_conversion_type,CASE
 		  		 		 						 WHEN raw_data.activity LIKE '%lead%' THEN 'sales assisted leads'
 		  		 		 						 ELSE 'self serve conversions'
 		  		 		 					  END)) AS conversion_type									--can we standardize it?
 	   ,LOWER(COALESCE(fld_ref.ref_paid_free,CASE
 		  		 							WHEN raw_data.activity LIKE '%lead%' THEN 'n/a'
 		  		 							WHEN raw_data.activity LIKE '%paid%' THEN 'paid'
 		  		 							WHEN raw_data.activity LIKE '%free%' THEN 'free'
 		  		 							ELSE 'n/a'
 		                                END)) AS conversion_paid_vs_free
 	   ,'n/a' AS conversion_customer_type
 	   ,LOWER(COALESCE(fld_ref.ref_conversion_new_vs_repeat,CASE
 		  		 										 WHEN raw_data.activity LIKE '%new%' THEN 'new'
 		  		 										 WHEN raw_data.activity LIKE '%repeat%' THEN 'repeat'
 		  		 										 ELSE 'n/a'
 		  		 										END)) AS conversion_new_vs_repeat
 	   ,LOWER(COALESCE(fld_ref.ref_package_group,pck_ref.package_group,'n/a')) AS conversion_package_group
 	   ,LOWER(COALESCE(fld_ref.ref_package_name,pck_ref.description,'n/a')) AS conversion_package_name
 	   ,LOWER(COALESCE(fld_ref.ref_sub_type,CASE
 		  		 						  WHEN pck_ref.annual = '1' THEN 'annual'
 		  		 						  WHEN pck_ref.monthly = '1' THEN 'monthly'
 		  		 						  ELSE 'n/a'
 		  		 						END)) AS conversion_subscription_type
	   ,raw_data.floodlightvariablemetric7 * (raw_data.activityclickthroughconversions / NULLIF(raw_data.totalconversions,0)) AS click_through_seats
	   ,raw_data.floodlightvariablemetric7 * (raw_data.activityviewthroughconversions / NULLIF(raw_data.totalconversions,0)) AS view_through_seats
	   ,raw_data.activityclickthroughconversions AS click_through_conversions
	   ,raw_data.activityviewthroughconversions AS view_through_conversions
	   ,raw_data.totalconversions AS total_conversions
       ,raw_data.activityclickthroughrevenue / exch.rateperusd AS click_through_booking
 	   ,raw_data.activityviewthroughrevenue / exch.rateperusd AS view_through_booking
 	   ,raw_data.totalconversionsrevenue / exch.rateperusd AS total_booking
FROM raw_data
LEFT JOIN cat_dcm
    ON raw_data.placement_id = cat_dcm.placement_id
LEFT JOIN fld_ref
    ON raw_data.floodlightvariabledimension3 = fld_ref.ref_uvar3_package_id
		AND raw_data.activity_id = fld_ref.activity_id
		AND raw_data.floodlight_config_id = fld_ref.ref_floodlight_config
LEFT JOIN pck_ref
    ON raw_data.floodlightvariabledimension3 = pck_ref.package_id
        AND raw_data.activity_id <> '8936232'
LEFT JOIN exch
    ON raw_data.floodlightvariabledimension6 = exch.currency
    and raw_data.month_year = exch.month_year
    ) MANIP_DATA
LEFT JOIN fees
    ON MANIP_DATA.date BETWEEN fees.start_date AND fees.end_date
    AND LOWER(fees.fee_desc) = 'adj_fee'