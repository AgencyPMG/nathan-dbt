
WITH search AS (
    SELECT * FROM {{ref('search_ad')}}
),
social AS (
    SELECT * FROM {{ref('social_ad')}}
),
display AS (
    SELECT * FROM {{ref('display_ad')}}
),
salesforce AS (
    SELECT * FROM {{ref('salesforce')}}
),
first_party AS (
    SELECT * FROM {{ref('first_party')}}
),
budget AS (
    SELECT * FROM {{ref('budget')}}
),
geo_ref AS (
    SELECT * FROM {{ref('base_gdrive_country_ref')}}
)

    SELECT STAK.date
           ,STAK.lead_created_date
           ,STAK.mql_date
           ,STAK.sql_date
           ,STAK.qualified_opps_date
           ,STAK.opp_won_date
           ,COALESCE(STAK.source_table,'n/a') AS source_table
           ,COALESCE(STAK.account_id,'n/a') AS account_id
           ,COALESCE(STAK.account_name,'n/a') AS account_name
           ,COALESCE(STAK.campaign_id,'n/a') AS campaign_id
           ,COALESCE(STAK.campaign_name,'n/a') AS campaign_name
           ,COALESCE(STAK.placement_id,'n/a') AS placement_id
           ,COALESCE(STAK.placement_name,'n/a') AS placement_name
           ,COALESCE(STAK.ad_id,'n/a') AS ad_id
           ,COALESCE(STAK.ad_name,'n/a') AS ad_name
           ,COALESCE(STAK.media_channel,'n/a') AS media_channel
           ,LOWER(COALESCE({{qualify('surveymonkey.taxon_media_channel_rollup(STAK.media_channel)')}},'n/a')) AS media_channel_rollup
           ,COALESCE(STAK.platform_group,'n/a') AS platform_group
           ,COALESCE(STAK.platform,'n/a') AS platform
           ,COALESCE(STAK.tactic,'n/a') AS tactic
           ,STAK.strategy                         --removed COALESCE command as NULL was needed in the final output - 20200406 - GD
           ,STAK.targeting
           ,COALESCE(STAK.conversion_goal,'n/a') AS conversion_goal
           ,COALESCE(STAK.business_unit_group,'n/a') AS business_unit_group
           ,COALESCE(STAK.business_unit,'n/a') AS business_unit
           ,LOWER(COALESCE({{qualify('surveymonkey.omni_business_unit_pillar(STAK.business_unit)')}},'n/a')) AS business_unit_pillar
           ,COALESCE(STAK.conversion_business_unit_group,'n/a') AS conversion_business_unit_group
           ,COALESCE(STAK.conversion_business_unit,'n/a') AS conversion_business_unit
           ,COALESCE(STAK.conversion_bu_group_matched,'n/a') AS conversion_bu_group_matched
           ,COALESCE(STAK.conversion_bu_matched,'n/a') AS conversion_bu_matched
           ,COALESCE(STAK.conversion_source,'n/a') AS conversion_source
           ,COALESCE(STAK.conversion_event_name,'n/a') AS conversion_event_name
           ,COALESCE(STAK.conversion_type,'n/a') AS conversion_type
           ,COALESCE(STAK.conversion_paid_vs_free,'n/a') AS conversion_paid_vs_free
           ,COALESCE(STAK.conversion_customer_type,'n/a') AS conversion_customer_type
           ,COALESCE(STAK.conversion_new_vs_repeat,'n/a') AS conversion_new_vs_repeat
           ,COALESCE(STAK.conversion_package_group,'n/a') AS conversion_package_group
           ,COALESCE(STAK.conversion_package_name,'n/a') AS conversion_package_name
           ,COALESCE(STAK.conversion_subscription_type,'n/a') AS conversion_subscription_type
           ,COALESCE(STAK.targeting_type,'n/a') AS targeting_type
           ,COALESCE(STAK.audience_targeting,'n/a') AS audience_targeting
           ,COALESCE(STAK.placement,'n/a') AS placement
           ,COALESCE(STAK.placement_size,'n/a') AS placement_size
           ,COALESCE(STAK.device,'n/a') AS device
           ,COALESCE(STAK.costing,'n/a') AS costing
           ,COALESCE(STAK.ad_type,'n/a') AS ad_type
           ,COALESCE(STAK.creative_description,'n/a') AS creative_description
           ,COALESCE(STAK.messaging_type,'n/a') AS messaging_type
           ,COALESCE(STAK.imagery_type,'n/a') AS imagery_type
           ,COALESCE(STAK.headline_cta,'n/a') AS headline_cta
           ,COALESCE(STAK.creative_size_video,'n/a') AS creative_size_video
           ,COALESCE(STAK.month_uploaded,'n/a') AS month_uploaded
           ,COALESCE(STAK.version,'n/a') AS version
           ,COALESCE(STAK.geo_detail,'n/a') AS geo_detail
		   ,LOWER(COALESCE(GEO_REF.geo_group,'n/a')) AS geo_group
		   ,LOWER(COALESCE(GEO_REF.geo_detail_hrn,'n/a')) AS geo_detail_hrn
		   ,LOWER(COALESCE(GEO_REF.region,'n/a')) AS region
           ,LOWER(COALESCE(STAK.conversion_country,'n/a')) AS conversion_country
           ,LOWER(COALESCE(CAST((STAK.conversion_country_hrn) AS {{dbt_utils.type_string()}}),GEO_REF.geo_detail_hrn,'n/a')) AS conversion_country_hrn
           ,LOWER(COALESCE(CAST((STAK.conversion_country_group) AS {{dbt_utils.type_string()}}),GEO_REF.geo_group,'n/a')) AS conversion_country_group
           ,LOWER(COALESCE(CAST((STAK.conversion_country_region) AS {{dbt_utils.type_string()}}),GEO_REF.region,'n/a')) AS conversion_country_region
           ,COALESCE(STAK.budget_item,'n/a') AS budget_item
           ,COALESCE(STAK.opportunity_stage,'n/a') AS opportunity_stage
           ,COALESCE(STAK.opportunity_type,'n/a') AS opportunity_type
           ,COALESCE(STAK.lead_status,'No Status Assigned') AS lead_status
           ,COALESCE(STAK.lead_priority,'No Priority Assigned') AS lead_priority
           ,COALESCE(STAK.audience_transaction_type,'n/a') AS audience_transaction_type
           ,SUM(COALESCE(impressions,0)) AS impressions
           ,SUM(COALESCE(clicks,0)) AS clicks
           ,SUM(COALESCE(media_spend,0)) AS media_spend
           ,SUM(COALESCE(tracking_fee,0)) AS tracking_fee
           ,SUM(COALESCE(agency_management_fee,0)) AS agency_management_fee
           ,SUM(COALESCE(agency_tech_fee,0)) AS agency_tech_fee
           ,SUM(COALESCE(total_spend,0)) AS total_spend
           ,SUM(COALESCE(click_through_conversions,0)) AS click_through_conversions
           ,SUM(COALESCE(view_through_conversions,0)) AS view_through_conversions
           ,SUM(COALESCE(adjusted_view_through_conversions,0)) AS adjusted_view_through_conversions
           ,SUM(COALESCE(total_conversions,0)) AS total_conversions
           ,SUM(COALESCE(adjusted_total_conversions,0)) AS adjusted_total_conversions
           ,SUM(COALESCE(click_through_booking,0)) AS click_through_booking
           ,SUM(COALESCE(view_through_booking,0)) AS view_through_booking
           ,SUM(COALESCE(adjusted_view_through_booking,0)) AS adjusted_view_through_booking
           ,SUM(COALESCE(total_booking,0)) AS total_booking
           ,SUM(COALESCE(adjusted_total_booking,0)) AS adjusted_total_booking
           ,SUM(COALESCE(booking_transactions,0)) AS booking_transactions
           ,SUM(COALESCE(revenue_transactions,0)) AS revenue_transactions
           ,SUM(COALESCE(click_through_revenue,0)) AS click_through_revenue
           ,SUM(COALESCE(view_through_revenue,0)) AS view_through_revenue
           ,SUM(COALESCE(adjusted_view_through_revenue,0)) AS adjusted_view_through_revenue
           ,SUM(COALESCE(total_revenue,0)) AS total_revenue
           ,SUM(COALESCE(adjusted_total_revenue,0)) AS adjusted_total_revenue
           ,SUM(COALESCE(click_through_seats,0)) AS click_through_seats
           ,SUM(COALESCE(adjusted_view_through_seats,0)) AS adjusted_view_through_seats
           ,SUM(COALESCE(adjusted_total_seats,0)) AS adjusted_total_seats
           ,COALESCE(CAST((STAK.primary_kpi) AS {{dbt_utils.type_string()}}),'n/a') AS primary_kpi
           ,COALESCE(CAST((STAK.secondary_kpi) AS {{dbt_utils.type_string()}}),'n/a') AS secondary_kpi
           ,SUM(COALESCE(STAK.primary_goal,0)) AS primary_goal
           ,SUM(COALESCE(STAK.secondary_goal,0)) AS secondary_goal
           ,SUM(COALESCE(media_spend_budget,0)) AS media_spend_budget
           ,SUM(COALESCE(total_spend_budget,0)) AS total_spend_budget
           ,SUM(COALESCE(returning_seats,0)) AS returning_seats_1p
           ,SUM(COALESCE(first_time_seats,0)) AS first_time_seats_1p
           ,SUM(COALESCE(returning_conversions,0)) AS returning_conversions_1p
           ,SUM(COALESCE(first_time_conversions,0)) AS first_time_conversions_1p
           ,SUM(COALESCE(new_booking_1p,0)) AS new_booking_1p
           ,SUM(COALESCE(new_revenue_1p,0)) AS new_revenue_1p
           ,SUM(COALESCE(olf_logic_new_paid_seats,0)) AS olf_logic_new_paid_seats
           ,SUM(COALESCE(olf_logic_new_paid_bookings_usd,0)) AS olf_logic_new_paid_bookings_usd
           ,SUM(COALESCE(olf_logic_first_time_new_paid_seats,0)) AS olf_logic_first_time_new_paid_seats
           ,SUM(COALESCE(olf_logic_first_time_new_paid_bookings_usd,0)) AS olf_logic_first_time_new_paid_bookings_usd
           ,SUM(COALESCE(olf_logic_returning_new_paid_seats,0)) AS olf_logic_returning_new_paid_seats
           ,SUM(COALESCE(olf_logic_returning_new_paid_bookings_usd,0)) AS olf_logic_returning_new_paid_bookings_usd
           ,SUM(COALESCE(upsell_seats,0)) AS upsell_seats
           ,SUM(COALESCE(upsell_bookings_usd,0)) AS upsell_bookings_usd
           ,SUM(COALESCE(overage_seats,0)) AS overage_seats
           ,SUM(COALESCE(overage_bookings_usd,0)) AS overage_bookings_usd
           ,SUM(COALESCE(renewal_seats,0)) AS renewal_seats
           ,SUM(COALESCE(renewal_bookings_usd,0)) AS renewal_bookings_usd
           ,SUM(COALESCE(upgrade_seats,0)) AS upgrade_seats
           ,SUM(COALESCE(upgrade_bookings_usd,0)) AS upgrade_bookings_usd
           ,SUM(COALESCE(sales_qualified_leads,0)) AS sales_qualified_leads
           ,SUM(COALESCE(qualified_opportunities,0)) AS qualified_opportunities
           ,SUM(COALESCE(opportunities_won,0)) AS opportunities_won
           ,SUM(COALESCE(inquiries,0)) AS inquiries
           ,SUM(COALESCE(marketing_qualified_leads,0)) AS marketing_qualified_leads
           ,SUM(COALESCE(pipeline,0)) AS pipeline
           ,SUM(COALESCE(new_free_signup,0)) AS new_free_signup
    FROM (
          --search data
          SELECT date
           ,NULL AS lead_created_date
           ,NULL AS mql_date
           ,NULL AS sql_date
           ,NULL AS qualified_opps_date
           ,NULL AS opp_won_date
           ,source_table
           ,account_id
           ,account_name
           ,campaign_id
           ,campaign_name
           ,placement_id
           ,placement_name
           ,NULL AS ad_id
           ,NULL AS ad_name
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
           ,geo_detail
           ,geo_group
           ,geo_detail AS conversion_country
           ,NULL AS conversion_country_hrn
           ,NULL AS conversion_country_group
           ,NULL AS conversion_country_region
           ,budget_item
           ,'n/a' AS opportunity_stage
           ,'n/a' AS opportunity_type
           ,'n/a' AS lead_status
           ,'n/a' AS lead_priority
           ,'n/a' AS audience_transaction_type
           ,impressions
           ,clicks
           ,media_spend
           ,tracking_fee
           ,agency_management_fee
           ,agency_tech_fee
           ,total_spend
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
           ,0 AS booking_transactions
           ,0 AS revenue_transactions
           ,click_through_revenue
           ,view_through_revenue
           ,adjusted_view_through_revenue
           ,total_revenue
           ,adjusted_total_revenue
           ,click_through_seats
           ,adjusted_view_through_seats
           ,adjusted_total_seats
           ,NULL AS primary_kpi
           ,NULL AS secondary_kpi
           ,0 AS primary_goal
           ,0 AS secondary_goal
           ,0 AS media_spend_budget
           ,0 AS total_spend_budget
           ,0 AS returning_seats
           ,0 AS first_time_seats
           ,0 AS returning_conversions
           ,0 AS first_time_conversions
           ,0 AS new_booking_1p
           ,0 AS new_revenue_1p
           ,0 AS olf_logic_new_paid_seats
           ,0 AS olf_logic_new_paid_bookings_usd
           ,0 AS olf_logic_first_time_new_paid_seats
           ,0 AS olf_logic_first_time_new_paid_bookings_usd
           ,0 AS olf_logic_returning_new_paid_seats
           ,0 AS olf_logic_returning_new_paid_bookings_usd
           ,0 AS upsell_seats
           ,0 AS upsell_bookings_usd
           ,0 AS overage_seats
           ,0 AS overage_bookings_usd
           ,0 AS renewal_seats
           ,0 AS renewal_bookings_usd
           ,0 AS upgrade_seats
           ,0 AS upgrade_bookings_usd
           ,0 AS sales_qualified_leads
           ,0 AS qualified_opportunities
           ,0 AS opportunities_won
           ,0 AS inquiries
           ,0 AS marketing_qualified_leads
           ,0 AS pipeline
           ,0 AS new_free_signup
    FROM search
------------------------------------------------------------------------------------------------------------------------
    UNION ALL
------------------------------------------------------------------------------------------------------------------------
    --social data
    SELECT date
           ,NULL AS lead_created_date
           ,NULL AS mql_date
           ,NULL AS sql_date
           ,NULL AS qualified_opps_date
           ,NULL AS opp_won_date
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
           ,geo_detail
           ,geo_group
           ,geo_detail AS conversion_country
           ,NULL AS conversion_country_hrn
           ,NULL AS conversion_country_group
           ,NULL AS conversion_country_region
           ,budget_item
           ,'n/a' AS opportunity_stage
           ,'n/a' AS opportunity_type
           ,'n/a' AS lead_status
           ,'n/a' AS lead_priority
           ,'n/a' AS audience_transaction_type
           ,impressions
           ,clicks
           ,media_spend
           ,tracking_fee
           ,agency_management_fee
           ,agency_tech_fee
           ,total_spend
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
           ,0 AS booking_transactions
           ,0 AS revenue_transactions
           ,click_through_revenue
           ,view_through_revenue
           ,adjusted_view_through_revenue
           ,total_revenue
           ,adjusted_total_revenue
           ,click_through_seats
           ,adjusted_view_through_seats
           ,adjusted_total_seats
           ,NULL AS primary_kpi
           ,NULL AS secondary_kpi
           ,0 AS primary_goal
           ,0 AS secondary_goal
           ,0 AS media_spend_budget
           ,0 AS total_spend_budget
           ,0 AS returning_seats
           ,0 AS first_time_seats
           ,0 AS returning_conversions
           ,0 AS first_time_conversions
           ,0 AS new_booking_1p
           ,0 AS new_revenue_1p
           ,0 AS olf_logic_new_paid_seats
           ,0 AS olf_logic_new_paid_bookings_usd
           ,0 AS olf_logic_first_time_new_paid_seats
           ,0 AS olf_logic_first_time_new_paid_bookings_usd
           ,0 AS olf_logic_returning_new_paid_seats
           ,0 AS olf_logic_returning_new_paid_bookings_usd
           ,0 AS upsell_seats
           ,0 AS upsell_bookings_usd
           ,0 AS overage_seats
           ,0 AS overage_bookings_usd
           ,0 AS renewal_seats
           ,0 AS renewal_bookings_usd
           ,0 AS upgrade_seats
           ,0 AS upgrade_bookings_usd
           ,0 AS sales_qualified_leads
           ,0 AS qualified_opportunities
           ,0 AS opportunities_won
           ,0 AS inquiries
           ,0 AS marketing_qualified_leads
           ,0 AS pipeline
           ,0 AS new_free_signup
    FROM social
------------------------------------------------------------------------------------------------------------------------
    UNION ALL
------------------------------------------------------------------------------------------------------------------------
    --display data
    SELECT date
           ,NULL AS lead_created_date
           ,NULL AS mql_date
           ,NULL AS sql_date
           ,NULL AS qualified_opps_date
           ,NULL AS opp_won_date
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
           ,geo_detail
           ,geo_group
           ,geo_detail AS conversion_country
           ,NULL AS conversion_country_hrn
           ,NULL AS conversion_country_group
           ,NULL AS conversion_country_region
           ,budget_item
           ,'n/a' AS opportunity_stage
           ,'n/a' AS opportunity_type
           ,'n/a' AS lead_status
           ,'n/a' AS lead_priority
           ,'n/a' AS audience_transaction_type
           ,impressions
           ,clicks
           ,media_spend
           ,tracking_fee
           ,agency_management_fee
           ,agency_tech_fee
           ,total_spend
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
           ,0 AS booking_transactions
           ,0 AS revenue_transactions
           ,click_through_revenue
           ,view_through_revenue
           ,adjusted_view_through_revenue
           ,total_revenue
           ,adjusted_total_revenue
           ,click_through_seats
           ,adjusted_view_through_seats
           ,adjusted_total_seats
           ,NULL AS primary_kpi
           ,NULL AS secondary_kpi
           ,0 AS primary_goal
           ,0 AS secondary_goal
           ,0 AS media_spend_budget
           ,0 AS total_spend_budget
           ,0 AS returning_seats
           ,0 AS first_time_seats
           ,0 AS returning_conversions
           ,0 AS first_time_conversions
           ,0 AS new_booking_1p
           ,0 AS new_revenue_1p
           ,0 AS olf_logic_new_paid_seats
           ,0 AS olf_logic_new_paid_bookings_usd
           ,0 AS olf_logic_first_time_new_paid_seats
           ,0 AS olf_logic_first_time_new_paid_bookings_usd
           ,0 AS olf_logic_returning_new_paid_seats
           ,0 AS olf_logic_returning_new_paid_bookings_usd
           ,0 AS upsell_seats
           ,0 AS upsell_bookings_usd
           ,0 AS overage_seats
           ,0 AS overage_bookings_usd
           ,0 AS renewal_seats
           ,0 AS renewal_bookings_usd
           ,0 AS upgrade_seats
           ,0 AS upgrade_bookings_usd
           ,0 AS sales_qualified_leads
           ,0 AS qualified_opportunities
           ,0 AS opportunities_won
           ,0 AS inquiries
           ,0 AS marketing_qualified_leads
           ,0 AS pipeline
           ,0 AS new_free_signup
    FROM display
------------------------------------------------------------------------------------------------------------------------
    UNION ALL
------------------------------------------------------------------------------------------------------------------------
    --1p data
    SELECT date
           ,NULL AS lead_created_date
           ,NULL AS mql_date
           ,NULL AS sql_date
           ,NULL AS qualified_opps_date
           ,NULL AS opp_won_date
           ,source_table
           ,'n/a' AS account_id
           ,'n/a' AS account_name
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
           ,'n/a' AS conversion_bu_group_matched
           ,'n/a' AS conversion_bu_matched
           ,'1p attribution' AS conversion_source
           ,'n/a' AS conversion_event_name
           ,'n/a' AS conversion_type
           ,'n/a' AS conversion_paid_vs_free
           ,'n/a' AS conversion_customer_type
           ,'n/a' AS conversion_new_vs_repeat
           ,package_group AS conversion_package_group
           ,package_group AS conversion_package_name
           ,package_type AS conversion_subscription_type
           ,'n/a' AS targeting_type
           ,'n/a' AS audience_targeting
           ,'n/a' AS placement
           ,'n/a' AS placement_size
           ,'n/a' AS device
           ,'n/a' AS costing
           ,'n/a' AS ad_type
           ,'n/a' AS creative_description
           ,'n/a' AS messaging_type
           ,'n/a' AS imagery_type
           ,'n/a' AS headline_cta
           ,'n/a' AS creative_size_video
           ,'n/a' AS month_uploaded
           ,'n/a' AS version
           ,geo_detail
           ,geo_group
           ,conversion_country
           ,conversion_country_hrn
           ,conversion_country_group
           ,conversion_country_region
           ,budget_item
           ,'n/a' AS opportunity_stage
           ,'n/a' AS opportunity_type
           ,'n/a' AS lead_status
           ,'n/a' AS lead_priority
           ,'n/a' AS audience_transaction_type
           ,0 AS impressions
           ,0 AS clicks
           ,0 AS media_spend
           ,0 AS tracking_fee
           ,0 AS agency_management_fee
           ,0 AS agency_tech_fee
           ,0 AS total_spend
           ,0 AS click_through_conversions
           ,0 AS view_through_conversions
           ,0 AS adjusted_view_through_conversions
           ,0 AS total_conversions
           ,0 AS adjusted_total_conversions
           ,0 AS click_through_booking
           ,0 AS view_through_booking
           ,0 AS adjusted_view_through_booking
           ,0 AS total_booking
           ,total_booking AS adjusted_total_booking
           ,booking_transactions
           ,revenue_transactions
           ,0 AS click_through_revenue
           ,0 AS view_through_revenue
           ,0 AS adjusted_view_through_revenue
           ,0 AS total_revenue
           ,revenue AS adjusted_total_revenue
           ,0 AS click_through_seats
           ,0 AS adjusted_view_through_seats
           ,0 AS adjusted_total_seats
           ,'n/a' AS primary_kpi
           ,'n/a' AS secondary_kpi
           ,0 AS primary_goal
           ,0 AS secondary_goal
           ,0 AS media_spend_budget
           ,0 AS total_spend_budget
           ,0 AS returning_seats
           ,0 AS first_time_seats
           ,0 AS returning_conversions
           ,0 AS first_time_conversions
           ,0 AS new_booking_1p
           ,CASE
               WHEN LOWER(conversion_business_unit) = 'core' AND LOWER(package_type) = 'annual' THEN total_booking / 12
               WHEN LOWER(conversion_business_unit) = 'audience' AND LOWER(package_group) LIKE '%annual%' THEN total_booking / 12
               ELSE total_booking
            END AS new_revenue_1p
           ,olf_logic_new_paid_seats
           ,olf_logic_new_paid_bookings_usd
           ,olf_logic_first_time_new_paid_seats
           ,olf_logic_first_time_new_paid_bookings_usd
           ,olf_logic_returning_new_paid_seats
           ,olf_logic_returning_new_paid_bookings_usd
           ,upsell_seats
           ,upsell_bookings_usd
           ,overage_seats
           ,overage_bookings_usd
           ,renewal_seats
           ,renewal_bookings_usd
           ,upgrade_seats
           ,upgrade_bookings_usd
           ,0 AS sales_qualified_leads
           ,0 AS qualified_opportunities
           ,0 AS opportunities_won
           ,0 AS inquiries
           ,0 AS marketing_qualified_leads
           ,0 AS pipeline
           ,new_free_signup
    FROM first_party
------------------------------------------------------------------------------------------------------------------------
    UNION ALL
------------------------------------------------------------------------------------------------------------------------
    --salesforce data
    SELECT date
           ,lead_created_date
           ,mql_date
           ,sql_date
           ,qualified_opps_date
           ,opp_won_date
           ,source_table
           ,'n/a' AS account_id
           ,'n/a' AS account_name
           ,campaign_id
           ,campaign_name
           ,placement_id
           ,placement_name
           ,ad_id
           ,ad_name
           ,media_channel
           ,platform AS platform_group
           ,platform
           ,tactic
           ,strategy
           ,targeting
           ,conversion_goal
           ,'n/a' AS business_unit_group
           ,business_unit
           ,conversion_business_unit AS conversion_business_unit_group
           ,conversion_business_unit AS conversion_business_unit
           ,'n/a' AS conversion_bu_group_matched
           ,'n/a' AS conversion_bu_matched
           ,'n/a' AS conversion_source
           ,'salesforce attribution' AS conversion_event_name
           ,'n/a' AS conversion_type
           ,'n/a' AS conversion_paid_vs_free
           ,'n/a' AS conversion_customer_type
           ,'n/a' AS conversion_new_vs_repeat
           ,'n/a' AS conversion_package_group
           ,'n/a' AS conversion_package_name
           ,'n/a' AS conversion_subscription_type
           ,'n/a' AS targeting_type
           ,'n/a' AS audience_targeting
           ,'n/a' AS placement
           ,'n/a' AS placement_size
           ,'n/a' AS device
           ,'n/a' AS costing
           ,'n/a' AS ad_type
           ,'n/a' AS creative_description
           ,'n/a' AS messaging_type
           ,'n/a' AS imagery_type
           ,'n/a' AS headline_cta
           ,'n/a' AS creative_size_video
           ,'n/a' AS month_uploaded
           ,'n/a' AS version
           ,LOWER(geo_detail) AS geo_detail
           ,'n/a' AS geo_group
           ,conversion_country
           ,conversion_country_hrn
           ,conversion_country_group
           ,conversion_country_region
           ,budget_item
           ,opportunity_stage
           ,opportunity_type
           ,lead_status
           ,lead_priority
           ,audience_transaction_type
           ,0 AS impressions
           ,0 AS clicks
           ,0 AS media_spend
           ,0 AS tracking_fee
           ,0 AS agency_management_fee
           ,0 AS agency_tech_fee
           ,0 AS total_spend
           ,0 AS click_through_conversions
           ,0 AS view_through_conversions
           ,0 AS adjusted_view_through_conversions
           ,0 AS total_conversions
           ,0 AS adjusted_total_conversions
           ,0 AS click_through_booking
           ,0 AS view_through_booking
           ,0 AS adjusted_view_through_booking
           ,pipeline_won AS total_booking
           ,0 AS adjusted_total_booking
           ,0 AS booking_transactions
           ,0 AS revenue_transactions
           ,0 AS click_through_revenue
           ,0 AS view_through_revenue
           ,0 AS adjusted_view_through_revenue
           ,0 AS total_revenue
           ,0 AS adjusted_total_revenue
           ,0 AS click_through_seats
           ,0 AS adjusted_view_through_seats
           ,0 AS adjusted_total_seats
           ,'n/a' AS primary_kpi
           ,'n/a' AS secondary_kpi
           ,0 AS primary_goal
           ,0 AS secondary_goal
           ,0 AS media_spend_budget
           ,0 AS total_spend_budget
           ,0 AS returning_seats
           ,0 AS first_time_seats
           ,0 AS returning_conversions
           ,0 AS first_time_conversions
           ,0 AS new_booking_1p
           ,0 AS new_revenue_1p
           ,0 AS olf_logic_new_paid_seats
           ,0 AS olf_logic_new_paid_bookings_usd
           ,0 AS olf_logic_first_time_new_paid_seats
           ,0 AS olf_logic_first_time_new_paid_bookings_usd
           ,0 AS olf_logic_returning_new_paid_seats
           ,0 AS olf_logic_returning_new_paid_bookings_usd
           ,0 AS upsell_seats
           ,0 AS upsell_bookings_usd
           ,0 AS overage_seats
           ,0 AS overage_bookings_usd
           ,0 AS renewal_seats
           ,0 AS renewal_bookings_usd
           ,0 AS upgrade_seats
           ,0 AS upgrade_bookings_usd
           ,sales_qualified_leads
           ,qualified_opportunities
           ,opportunities_won
           ,inquiries
           ,marketing_qualified_leads
           ,pipeline
           ,0 AS new_free_signup
    FROM salesforce
------------------------------------------------------------------------------------------------------------------------
    UNION ALL
------------------------------------------------------------------------------------------------------------------------
    --budget data
    SELECT date
           ,NULL AS lead_created_date
           ,NULL AS mql_date
           ,NULL AS sql_date
           ,NULL AS qualified_opps_date
           ,NULL AS opp_won_date
           ,source_table
           ,'n/a' AS account_id
           ,'n/a' AS account_name
           ,'n/a' AS campaign_id
           ,'n/a' AS campaign_name
           ,'n/a' AS placement_id
           ,'n/a' AS placement_name
           ,'n/a' AS ad_id
           ,'n/a' AS ad_name
           ,media_channel
           ,'n/a' AS platform_group
           ,'n/a' AS platform
           ,'n/a' AS tactic
           ,'n/a' AS strategy
           ,'n/a' AS targeting
           ,'n/a' AS conversion_goal
           ,'n/a' AS business_unit_group
           ,business_unit
           ,'n/a' AS conversion_business_unit_group
           ,'n/a' AS conversion_business_unit
           ,'n/a' AS conversion_bu_group_matched
           ,'n/a' AS conversion_bu_matched
           ,'n/a' AS conversion_source
           ,'n/a' AS conversion_event_name
           ,'n/a' AS conversion_type
           ,'n/a' AS conversion_paid_vs_free
           ,'n/a' AS conversion_customer_type
           ,'n/a' AS conversion_new_vs_repeat
           ,'n/a' AS conversion_package_group
           ,'n/a' AS conversion_package_name
           ,'n/a' AS conversion_subscription_type
           ,'n/a' AS targeting_type
           ,'n/a' AS audience_targeting
           ,'n/a' AS placement
           ,'n/a' AS placement_size
           ,'n/a' AS device
           ,'n/a' AS costing
           ,'n/a' AS ad_type
           ,'n/a' AS creative_description
           ,'n/a' AS messaging_type
           ,'n/a' AS imagery_type
           ,'n/a' AS headline_cta
           ,'n/a' AS creative_size_video
           ,'n/a' AS month_uploaded
           ,'n/a' AS version
           ,'unclassified' AS geo_detail
           ,'unclassified' AS geo_group
           ,'unclassified' AS conversion_country
           ,'unclassified' AS conversion_country_hrn
           ,'unclassified' AS conversion_country_group
           ,'unclassified' AS conversion_country_region
           ,budget_item
           ,'n/a' AS opportunity_stage
           ,'n/a' AS opportunity_type
           ,'n/a' AS lead_status
           ,'n/a' AS lead_priority
           ,'n/a' AS audience_transaction_type
           ,0 AS impressions
           ,0 AS clicks
           ,0 AS media_spend
           ,0 AS tracking_fee
           ,0 AS agency_management_fee
           ,0 AS agency_tech_fee
           ,0 AS total_spend
           ,0 AS click_through_conversions
           ,0 AS view_through_conversions
           ,0 AS adjusted_view_through_conversions
           ,0 AS total_conversions
           ,0 AS adjusted_total_conversions
           ,0 AS click_through_booking
           ,0 AS view_through_booking
           ,0 AS adjusted_view_through_booking
           ,0 AS total_booking
           ,0 AS adjusted_total_booking
           ,0 AS booking_transactions
           ,0 AS revenue_transactions
           ,0 AS click_through_revenue
           ,0 AS view_through_revenue
           ,0 AS adjusted_view_through_revenue
           ,0 AS total_revenue
           ,0 AS adjusted_total_revenue
           ,0 AS click_through_seats
           ,0 AS adjusted_view_through_seats
           ,0 AS adjusted_total_seats
           ,primary_kpi
           ,secondary_kpi
           ,primary_goal
           ,secondary_goal
           ,media_spend_budget
           ,total_spend_budget
           ,0 AS returning_seats
           ,0 AS first_time_seats
           ,0 AS returning_conversions
           ,0 AS first_time_conversions
           ,0 AS new_booking_1p
           ,0 AS new_revenue_1p
           ,0 AS olf_logic_new_paid_seats
           ,0 AS olf_logic_new_paid_bookings_usd
           ,0 AS olf_logic_first_time_new_paid_seats
           ,0 AS olf_logic_first_time_new_paid_bookings_usd
           ,0 AS olf_logic_returning_new_paid_seats
           ,0 AS olf_logic_returning_new_paid_bookings_usd
           ,0 AS upsell_seats
           ,0 AS upsell_bookings_usd
           ,0 AS overage_seats
           ,0 AS overage_bookings_usd
           ,0 AS renewal_seats
           ,0 AS renewal_bookings_usd
           ,0 AS upgrade_seats
           ,0 AS upgrade_bookings_usd
           ,0 AS sales_qualified_leads
           ,0 AS qualified_opportunities
           ,0 AS opportunities_won
           ,0 AS inquiries
           ,0 AS marketing_qualified_leads
           ,0 AS pipeline
           ,0 AS new_free_signup
    FROM budget
    ) AS STAK
		LEFT JOIN geo_ref
				ON LOWER(STAK.geo_detail) = geo_ref.geo_detail
    GROUP BY STAK.date
             ,STAK.lead_created_date
             ,STAK.mql_date
             ,STAK.sql_date
             ,STAK.qualified_opps_date
             ,STAK.opp_won_date
             ,STAK.source_table
             ,STAK.account_id
             ,STAK.account_name
             ,STAK.campaign_id
             ,STAK.campaign_name
             ,STAK.placement_id
             ,STAK.placement_name
             ,STAK.ad_id
             ,STAK.ad_name
             ,STAK.media_channel
             ,media_channel_rollup
             ,STAK.platform_group
             ,STAK.platform
             ,STAK.tactic
             ,STAK.strategy
             ,STAK.targeting
             ,STAK.conversion_goal
             ,STAK.business_unit_group
             ,STAK.business_unit
             ,business_unit_pillar
             ,STAK.conversion_business_unit_group
             ,STAK.conversion_business_unit
             ,STAK.conversion_bu_group_matched
             ,STAK.conversion_bu_matched
             ,STAK.conversion_source
             ,STAK.conversion_event_name
             ,STAK.conversion_type
             ,STAK.conversion_paid_vs_free
             ,STAK.conversion_customer_type
             ,STAK.conversion_new_vs_repeat
             ,STAK.conversion_package_group
             ,STAK.conversion_package_name
             ,STAK.conversion_subscription_type
             ,COALESCE(STAK.targeting_type,'n/a')
             ,COALESCE(STAK.audience_targeting,'n/a')
             ,COALESCE(STAK.placement,'n/a')
             ,COALESCE(STAK.placement_size,'n/a')
             ,COALESCE(STAK.device,'n/a')
             ,COALESCE(STAK.costing,'n/a')
             ,COALESCE(STAK.ad_type,'n/a')
             ,COALESCE(STAK.creative_description,'n/a')
             ,COALESCE(STAK.messaging_type,'n/a')
             ,COALESCE(STAK.imagery_type,'n/a')
             ,COALESCE(STAK.headline_cta,'n/a')
             ,COALESCE(STAK.creative_size_video,'n/a')
             ,COALESCE(STAK.month_uploaded,'n/a')
             ,COALESCE(STAK.version,'n/a')
             ,COALESCE(STAK.geo_detail,'n/a')
		     ,LOWER(COALESCE(GEO_REF.geo_group,'n/a'))
		     ,LOWER(COALESCE(GEO_REF.geo_detail_hrn,'n/a'))
		     ,LOWER(COALESCE(GEO_REF.region,'n/a'))
             ,STAK.conversion_country
             ,LOWER(COALESCE(CAST((STAK.conversion_country_hrn) AS {{dbt_utils.type_string()}}),GEO_REF.geo_detail_hrn,'n/a'))
             ,LOWER(COALESCE(CAST((STAK.conversion_country_group) AS {{dbt_utils.type_string()}}),GEO_REF.geo_group,'n/a'))
             ,LOWER(COALESCE(CAST((STAK.conversion_country_region) AS {{dbt_utils.type_string()}}),GEO_REF.region,'n/a'))
             ,STAK.budget_item
             ,STAK.opportunity_stage
             ,STAK.opportunity_type
             ,STAK.lead_status
             ,STAK.lead_priority
             ,STAK.audience_transaction_type
             ,STAK.primary_kpi
             ,STAK.secondary_kpi
