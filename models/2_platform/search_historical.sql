{{ config(
    partition_by={
      "field": "date",
      "data_type": "timestamp"
    },
    cluster_by=["media_channel","platform","campaign_id"]
)}}

WITH google_ads AS (
    SELECT * FROM {{ ref('base_search_historical_conversion_types_google_ads') }}
),
ref AS (
    SELECT * FROM {{ ref('base_search_engine_conversion_name_references') }}
),
sa360 AS (
    SELECT * FROM {{ ref('base_sa360_campaign') }}
),
bing AS (
    SELECT * FROM {{ref('base_search_historical_conversion_types_bing')}}
)
SELECT RAW_DATA.date
	   ,RAW_DATA.source_table
	   ,RAW_DATA.account_id
	   ,RAW_DATA.account_name
	   ,RAW_DATA.campaign_id
	   ,RAW_DATA.campaign_name
	   ,RAW_DATA.media_channel
	   ,RAW_DATA.platform_group
	   ,RAW_DATA.platform
	   ,RAW_DATA.tactic
	   ,LOWER({{qualify('surveymonkey')}}.omni_strategy(RAW_DATA.media_channel,RAW_DATA.campaign_name,'n/a')) AS strategy
       ,LOWER({{qualify('surveymonkey')}}.omni_targeting({{qualify('surveymonkey')}}.omni_strategy(RAW_DATA.media_channel,RAW_DATA.campaign_name,'n/a'))) AS targeting
	   ,RAW_DATA.business_unit
	   ,RAW_DATA.geo_detail
	   ,RAW_DATA.audience_conversion_goal
	   ,RAW_DATA.conversion_source
	   ,RAW_DATA.conversion_event_name
	   ,RAW_DATA.conversion_business_unit
	   ,RAW_DATA.conversion_type
	   ,RAW_DATA.conversion_paid_vs_free
	   ,RAW_DATA.conversion_customer_type
	   ,RAW_DATA.conversion_new_vs_repeat
	   ,RAW_DATA.conversion_package_group
	   ,RAW_DATA.conversion_package_name
	   ,RAW_DATA.conversion_subscription_type
	   ,RAW_DATA.click_through_conversions
	   ,RAW_DATA.total_conversions
	   ,RAW_DATA.total_conversions AS adjusted_total_conversions
	   ,RAW_DATA.click_through_booking
	   ,RAW_DATA.total_booking
	   ,RAW_DATA.total_booking AS adjusted_total_booking
	   ,{{ time_current_zone() }} AS last_updated
		 
FROM (
    --Google Ads Historic Conversion Data
      SELECT google_ads.date
      		 ,google_ads.source_table
      		 ,google_ads.account_id
      		 ,google_ads.account_name
      		 ,google_ads.campaign_id
      		 ,google_ads.campaign_name
      		 ,google_ads.media_channel
      		 ,google_ads.platform_group
      		 ,google_ads.platform
      		 ,LOWER({{qualify('surveymonkey.search_tactic(google_ads.campaign_name)')}}) AS tactic
      		 ,LOWER({{qualify('surveymonkey.omni_business_unit(google_ads.account_name,google_ads.campaign_name)')}}) AS business_unit
      		 ,LOWER({{qualify('surveymonkey.omni_geo_detail(google_ads.campaign_name)')}}) AS geo_detail
      		 ,CASE
      				WHEN LOWER({{qualify('surveymonkey.omni_business_unit(google_ads.account_name,google_ads.campaign_name)')}}) = 'audience' AND LOWER(ref.ref_conversion_business_unit) = 'audience' THEN LOWER(ref.ref_conversion_type)
      				ELSE 'n/a'
      			END AS audience_conversion_goal
      		 ,'google ads historical conversion types' AS conversion_source
      		 ,google_ads.conversion_action AS conversion_event_name
      		 ,ref.ref_conversion_business_unit AS conversion_business_unit
      		 ,ref.ref_conversion_type AS conversion_type
      		 ,ref.ref_conversion_paid_vs_free AS conversion_paid_vs_free
      		 ,'n/a' AS conversion_customer_type
      		 ,ref.ref_conversion_new_vs_repeat AS conversion_new_vs_repeat
      		 ,ref.ref_conversion_package_group AS conversion_package_group
      		 ,ref.ref_conversion_package_name AS conversion_package_name
      		 ,ref.ref_conversion_subscription_type AS conversion_subscription_type
      		 ,CASE
      				WHEN LOWER(ref.ref_conversion_business_unit) = 'core' AND LOWER({{qualify('surveymonkey.omni_business_unit(google_ads.account_name,google_ads.campaign_name)')}}) = 'core' THEN 0
      				WHEN LOWER({{qualify('surveymonkey')}}.search_google_adwords_historical_conversion_action('2019-10-25',google_ads.conversion_action)) = 'yes' THEN conversions
      			END AS click_through_conversions
      		 ,CASE
      				WHEN LOWER(ref.ref_conversion_business_unit) = 'core' AND LOWER({{qualify('surveymonkey.omni_business_unit(google_ads.account_name,google_ads.campaign_name)')}}) = 'core' THEN 0
      				WHEN LOWER({{qualify('surveymonkey')}}.search_google_adwords_historical_conversion_action('2019-10-25',google_ads.conversion_action)) = 'yes' THEN conversions
      			END AS total_conversions
      		 ,CASE
      				WHEN LOWER(ref.ref_conversion_business_unit) = 'core' AND LOWER({{qualify('surveymonkey.omni_business_unit(google_ads.account_name,google_ads.campaign_name)')}}) = 'core' THEN 0
      				WHEN LOWER({{qualify('surveymonkey')}}.search_google_adwords_historical_conversion_action('2019-10-25',google_ads.conversion_action)) = 'yes' THEN conversions
      			END AS adjusted_total_conversions
      		 ,CASE
      				WHEN LOWER(ref.ref_conversion_business_unit) = 'core' AND LOWER({{qualify('surveymonkey.omni_business_unit(google_ads.account_name,google_ads.campaign_name)')}}) = 'core' THEN 0
      				WHEN LOWER({{qualify('surveymonkey')}}.search_google_adwords_historical_conversion_action('2019-10-25',google_ads.conversion_action)) = 'yes' THEN conversion_value
      				ELSE 0
      			END AS click_through_booking
      		 ,CASE
      				WHEN LOWER(ref.ref_conversion_business_unit) = 'core' AND LOWER({{qualify('surveymonkey.omni_business_unit(google_ads.account_name,google_ads.campaign_name)')}}) = 'core' THEN 0
      				WHEN LOWER({{qualify('surveymonkey')}}.search_google_adwords_historical_conversion_action('2019-10-25',google_ads.conversion_action)) = 'yes' THEN conversion_value
      				ELSE 0
      			 END AS total_booking
      		 ,CASE
      				WHEN LOWER(ref.ref_conversion_business_unit) = 'core' AND LOWER({{qualify('surveymonkey.omni_business_unit(google_ads.account_name,google_ads.campaign_name)')}}) = 'core' THEN 0
      				WHEN LOWER({{qualify('surveymonkey')}}.search_google_adwords_historical_conversion_action('2019-10-25',google_ads.conversion_action)) = 'yes' THEN conversion_value
      				ELSE 0
      			END AS adjusted_total_booking
      FROM google_ads
      LEFT JOIN ref
		ON google_ads.conversion_action = ref.ref_goalname

	  UNION ALL

-- SA360 Floodlight Conversions (Historical Core Data)

     SELECT date
     		 ,'surveymonkey.sa360_campaign' AS source_table
     		 ,RAW_DATA_SA360.accountid AS account_id
     		 ,LOWER(RAW_DATA_SA360.account) AS account_name
     		 ,RAW_DATA_SA360.campaignid AS campaign_id
     		 ,LOWER(RAW_DATA_SA360.campaign) AS campaign_name
     		 ,'paid search' AS media_channel
     		 ,LOWER(RAW_DATA_SA360.accounttype) AS platform_group
     		 ,LOWER(RAW_DATA_SA360.accounttype) AS platform
     		 ,LOWER({{qualify('surveymonkey.search_tactic(RAW_DATA_SA360.campaign)')}}) AS tactic
     		 ,LOWER({{qualify('surveymonkey.business_unit_subgroup(RAW_DATA_SA360.account,RAW_DATA_SA360.campaign)')}}) AS business_unit
     		 ,LOWER({{qualify('surveymonkey.omni_geo_detail(RAW_DATA_SA360.campaign)')}}) AS geo_detail
     		 ,'n/a' AS audience_conversion_goal
     		 ,'sa360_floodlight' AS conversion_source
     		 ,RAW_DATA_SA360.sa360_custom_conversion AS conversion_event_name
     		 ,'core' AS conversion_business_unit
     		 ,'self serve conversion' AS conversion_type
     		 ,CASE
     		 	  WHEN LOWER(sa360_custom_conversion) LIKE '%pmg_c_sm_basic_conversion%' THEN 'free'
     		 	  ELSE 'paid'
     		  END AS conversion_paid_vs_free
     		 ,'n/a' AS conversion_customer_type
     		 ,'n/a' AS conversion_new_vs_repeat
     		 ,LOWER({{qualify('surveymonkey.search_sa360_conversion_package_group(RAW_DATA_SA360.sa360_custom_conversion)')}}) AS conversion_package_group
     		 ,CASE
     		      WHEN LOWER(RAW_DATA_SA360.sa360_custom_conversion) = 'pmg_c_sm_basic_conversions' THEN 'basic'
     		      ELSE LOWER({{qualify('surveymonkey.search_sa360_conversion_package_group(RAW_DATA_SA360.sa360_custom_conversion)')}})
     		  END AS conversion_package_name
     		 ,CASE
     		 	  WHEN LOWER(RAW_DATA_SA360.sa360_custom_conversion) LIKE '%annual%' THEN 'annual'
     		 	  WHEN LOWER(RAW_DATA_SA360.sa360_custom_conversion) LIKE '%monthly%' THEN 'monthly'
     		 	  ELSE 'n/a'
     		  END AS conversion_subscription_type
     		 ,RAW_DATA_SA360.conversions AS click_through_conversions
     		 ,RAW_DATA_SA360.conversions AS total_conversions
     		 ,RAW_DATA_SA360.conversions AS adjusted_total_conversions
     		 ,0 AS click_through_booking
     		 ,RAW_DATA_SA360.conversion_value AS total_booking
     		 ,RAW_DATA_SA360.conversion_value AS adjusted_total_booking

     FROM (

           SELECT date
                 ,accountid
                 ,account
                 ,campaignid
                 ,campaign
                 ,accounttype
                 ,pmg_c_SM_Basic_Conversions AS conversions
                 ,0 AS conversion_value
                 ,'pmg_c_SM_Basic_Conversions' AS sa360_custom_conversion
           FROM sa360
           WHERE pmg_c_SM_Basic_Conversions > 0

       	   UNION ALL

           SELECT date
                  ,accountid
                  ,account
                  ,campaignid
                  ,campaign
                  ,accounttype
                  ,pmg_c_31_standard_monthly AS conversions
                  ,pmg_c_31_Standard_Monthly_rev AS conversion_value
                  ,'pmg_c_31_standard_monthly' AS sa360_custom_conversion
           FROM sa360
           WHERE pmg_c_31_standard_monthly > 0

           UNION ALL

           SELECT date
                  ,accountid
                  ,account
                  ,campaignid
                  ,campaign
                  ,accounttype
                  ,pmg_c_33_advantage_monthly AS conversions
                  ,pmg_c_33_advantage_monthly_revenue AS conversion_value
                  ,'pmg_c_33_advantage_monthly' AS sa360_custom_conversion
           FROM sa360
           WHERE pmg_c_33_advantage_monthly > 0

           UNION ALL

           SELECT date
                  ,accountid
                  ,account
                  ,campaignid
                  ,campaign
                  ,accounttype
                  ,pmg_c_34_advantage_annual AS conversions
                  ,pmg_c_34_advantage_annual_revenue AS conversion_value
                  ,'pmg_c_34_advantage_annual' AS sa360_custom_conversion
           FROM sa360
           WHERE pmg_c_34_advantage_annual > 0

       	   UNION ALL

           SELECT date
                 ,accountid
                 ,account
                 ,campaignid
                 ,campaign
                 ,accounttype
                 ,pmg_c_35_premier_monthly AS conversions
                 ,pmg_c_35_premier_monthly_revenue AS conversion_value
                 ,'pmg_c_35_premier_monthly' AS sa360_custom_conversion
           FROM sa360
           WHERE pmg_c_35_premier_monthly > 0

       	   UNION ALL

           SELECT date
                  ,accountid
                  ,account
                  ,campaignid
                  ,campaign
                  ,accounttype
                  ,pmg_c_36_premier_annual AS conversions
                  ,pmg_c_36_premier_annual_revenue AS conversion_value
                  ,'pmg_c_36_premier_annual' AS sa360_custom_conversion
           FROM sa360
           WHERE pmg_c_36_premier_annual > 0

           UNION ALL

           SELECT date
                  ,accountid
                  ,account
                  ,campaignid
                  ,campaign
                  ,accounttype
                  ,pmg_c_134_Teams_Advantage_annual AS conversions
                  ,pmg_c_134_teams_advantage_annual_revenue AS conversion_value
                  ,'pmg_c_134_Teams_Advantage_annual' AS sa360_custom_conversion
           FROM sa360
           WHERE pmg_c_134_Teams_Advantage_annual > 0

       	   UNION ALL

           SELECT date
                  ,accountid
                  ,account
                  ,campaignid
                  ,campaign
                  ,accounttype
                  ,pmg_c_136_teams_premier_annual AS conversions
                  ,pmg_c_136_teams_premier_annual_revenue AS conversion_value
                  ,'pmg_c_136_teams_premier_annual' AS sa360_custom_conversion
           FROM sa360
           WHERE pmg_c_136_teams_premier_annual > 0  

           ) AS RAW_DATA_SA360


	       UNION ALL

    --Bing Ads Engine Conversions (for 2019 Data)
          SELECT bing.date
	      	     ,bing.source_table
	      	     ,bing.account_id
	      	     ,bing.account_name
	      	     ,bing.campaign_id
	      	     ,bing.campaign_name
	      	     ,bing.media_channel
	      	     ,bing.platform_group
	      	     ,bing.platform
	      	     ,LOWER({{qualify('surveymonkey.search_tactic(bing.campaign_name)')}}) AS tactic
	      	     ,LOWER({{qualify('surveymonkey.omni_business_unit(bing.account_name,bing.campaign_name)')}}) AS business_unit
	      	     ,LOWER({{qualify('surveymonkey.omni_geo_detail(bing.campaign_name)')}}) AS geo_detail
	      	     ,CASE
	      	          WHEN LOWER({{qualify('surveymonkey.omni_business_unit(bing.account_name,bing.campaign_name)')}}) = 'audience'
	      	    				AND LOWER(ref.ref_conversion_business_unit) = 'audience'
	      	    				THEN LOWER(ref.ref_conversion_type)
	      	          ELSE 'n/a'
	      	      END AS audience_conversion_goal
	      	     ,'bing ads historical conversion types' AS conversion_source
	      	     ,bing.conversion_action AS conversion_event_name
	      	     ,ref.ref_conversion_business_unit AS conversion_business_unit
	      	     ,ref.ref_conversion_type AS conversion_type
	      	     ,ref.ref_conversion_paid_vs_free AS conversion_paid_vs_free
	      	     ,'n/a' AS conversion_customer_type
	      	     ,ref.ref_conversion_new_vs_repeat AS conversion_new_vs_repeat
	      	     ,ref.ref_conversion_package_group AS conversion_package_group
	      	     ,ref.ref_conversion_package_name AS conversion_package_name
	      	     ,ref.ref_conversion_subscription_type AS conversion_subscription_type
	      	     ,bing.conversions AS click_through_conversions
	      	     ,bing.conversions AS total_conversions
	      	     ,bing.conversions AS adjusted_total_conversions
	      	     ,bing.conversion_value AS click_through_booking
	      	     ,bing.conversion_value AS total_booking
	      	     ,bing.conversion_value AS adjusted_total_booking
          FROM bing
          LEFT JOIN ref
          ON bing.conversion_action = ref.ref_goalname 
     ) AS RAW_DATA
