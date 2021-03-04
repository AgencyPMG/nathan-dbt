WITH source AS  (
    SELECT * FROM {{source('surveymonkey','sa360_campaign')}}
),

transformed AS (
  SELECT date
       ,CAST((accountid) AS {{dbt_utils.type_string()}}) AS accountid
       ,account
       ,campaignid
       ,campaign
       ,accounttype
       ,pmg_c_SM_Basic_Conversions
       ,pmg_c_31_standard_monthly
       ,pmg_c_31_Standard_Monthly_rev
       ,pmg_c_33_advantage_monthly
       ,pmg_c_33_advantage_monthly_revenue
       ,pmg_c_34_advantage_annual
       ,pmg_c_34_advantage_annual_revenue
       ,pmg_c_35_premier_monthly
       ,pmg_c_35_premier_monthly_revenue
       ,pmg_c_36_premier_annual
       ,pmg_c_36_premier_annual_revenue
       ,pmg_c_134_Teams_Advantage_annual
       ,pmg_c_134_teams_advantage_annual_revenue
       ,pmg_c_136_teams_premier_annual
       ,pmg_c_136_teams_premier_annual_revenue
       ,'pmg_c_SM_Basic_Conversions' AS sa360_custom_conversion
  FROM source
)

SELECT *
FROM transformed
WHERE date <= '2019-10-25'