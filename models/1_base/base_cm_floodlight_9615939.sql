WITH source AS (
    SELECT * FROM {{source('surveymonkey','cm_floodlight_9615939')}}
),

tranformed AS (
    SELECT date
           ,{{month_year('date')}} AS month_year
	       ,'surveymonkey.cm_floodlight_9615939' AS source_table
	       ,CASE
                WHEN LOWER(paidsearchengineaccountid) LIKE '' THEN advertiserid
	       		ELSE LOWER(paidsearchengineaccountid)
	       	END AS account_id
	       ,CASE
	       		WHEN LOWER(paidsearchengineaccount) LIKE '' THEN  advertiser
	       		ELSE LOWER(paidsearchengineaccount)
	       	END AS account_name
	       ,CASE
	       		WHEN LOWER(paidsearchcampaignid) LIKE '' THEN campaignid
	       		ELSE LOWER(paidsearchcampaignid)
	       	END AS campaign_id
	       ,CASE
	       		WHEN LOWER(paidsearchcampaign) LIKE '' THEN campaign
	       		ELSE LOWER(paidsearchcampaign)
	       	END AS campaign_name
	       ,campaign																--this column is added only to be referred in the categorizations for identifying paid search campaigns
	       ,CASE
	       		WHEN LOWER(paidsearchadgroupid) LIKE '' THEN placementid
	       		ELSE LOWER(paidsearchadgroupid)
	       	END AS placement_id
	       ,CASE
	       		WHEN LOWER(paidsearchadgroup) LIKE '' THEN placement
	       		ELSE LOWER(paidsearchadgroup)
	       	END AS placement_name
	       ,adid AS ad_id
	       ,LOWER(ad) AS ad_name
	       ,LOWER(adtype) AS ad_type
	       ,LOWER(siteid) AS site_id
	       ,LOWER(site) AS site
	       ,sitedirectoryid AS site_directory_id
	       ,LOWER(sitedirectory) AS site_directory
	       ,activityid AS activity_id
	       ,LOWER(activity) AS activity
           ,{% if target.type == 'bigquery' %}
               SPLIT(activity,':') AS conversion_business_unit
            {% elif target.type == 'redshift' %}
                SPLIT_PART(activity,':',1) AS conversion_business_unit
            {% endif %}
	       ,floodlightconfigid AS floodlight_config_id
	       ,activityclickthroughrevenue
	       ,activityviewthroughrevenue
	       ,totalconversionsrevenue
	       ,LOWER(floodlightvariabledimension3) AS floodlightvariabledimension3
	       ,LOWER(floodlightvariabledimension6) AS floodlightvariabledimension6
	       ,floodlightvariablemetric7
	       ,activityclickthroughconversions
	       ,activityviewthroughconversions
	       ,totalconversions
    FROM source
    WHERE campaign <> ''
)

SELECT * FROM tranformed