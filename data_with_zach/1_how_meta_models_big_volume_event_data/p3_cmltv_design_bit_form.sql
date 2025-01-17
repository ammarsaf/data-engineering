-- Using table user_cumulated, create bit dtype for compute power optimization
-- Reduce 10 years churn analysis execution that consume a week to complete to couple of hours

WITH users AS (
	SELECT *
	FROM users_cumulated
	WHERE date = DATE('2023-01-20')
), 
series AS (
	SELECT * 
	FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') as series_date
),
placeholder_ints AS (
	SELECT 
		CASE 
			WHEN dates_active @> ARRAY [DATE(series_date)] -- date active in list series date
				THEN date - DATE(series_date) -- 2^(32-date_series)
			ELSE 0
		END AS last_days_active, 
		CAST(CASE 
			WHEN dates_active @> ARRAY [DATE(series_date)] -- date active in list series date
				THEN CAST(POW(2, 32 - (date - DATE(series_date))) AS BIGINT) -- 2^(32-date_series)
			ELSE 0
		END AS BIT(32)) AS  last_days_active_bit, 
		CASE 
			WHEN dates_active @> ARRAY [DATE(series_date)] -- date active in list series date
				THEN CAST(POW(2, 32 - (date - DATE(series_date))) AS BIGINT) -- 2^(32-date_series)
			ELSE 0
		END AS  last_days_active_bit_n, 
		*
	FROM users  CROSS JOIN series 
	-- WHERE user_id = '5792789928190028000'
)
SELECT 
	user_id, SUM(last_days_active) as total_days_active,
	CAST(CAST(SUM(last_days_active_bit_n) AS BIGINT) AS BIT(32)) as total_active_bit,
	BIT_COUNT(CAST(CAST(SUM(last_days_active_bit_n) AS BIGINT) AS BIT(32))) > 0 as dim_active_monthly, 
	BIT_COUNT(CAST('11111110000000000000000000000000' AS BIT(32)) & -- bitwise AND operator (gate comparing and activate if 1-1)
		CAST(CAST(SUM(last_days_active_bit_n) AS BIGINT) AS BIT(32))) > 0 as dim_active_weekly, 
	BIT_COUNT(CAST('10000000000000000000000000000000' AS BIT(32)) & -- bitwise AND operator
		CAST(CAST(SUM(last_days_active_bit_n) AS BIGINT) AS BIT(32))) > 0 as dim_active_daily
FROM placeholder_ints
GROUP BY user_id
--SELECT date, dates_active, last_days_active, last_days_active_bit 
--FROM placeholder_ints 
--GROUP BY date, last_days_active_bit, dates_active, last_days_active
--HAVING last_days_active < 10
--ORDER BY last_days_active DESC;

SELECT CAST(CAST(POW(2, 13) AS BIGINT) as BIT(32)); -- placeholter_int_value
