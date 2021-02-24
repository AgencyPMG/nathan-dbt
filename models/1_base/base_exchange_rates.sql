WITH source AS (
    SELECT * FROM {{source('surveymonkey','exchange_rates')}}
),

transformed AS (
    SELECT DISTINCT {{month_year('pubdate')}} AS month_year
		   ,LOWER(currency) AS currency
		   ,rateperusd
		   ,ratepereuro
		   ,rateperbrl
	FROM source
)

SELECT * FROM transformed
