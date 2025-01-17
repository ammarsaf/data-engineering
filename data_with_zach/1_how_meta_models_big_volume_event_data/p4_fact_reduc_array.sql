-- Building reduced fact table into a single array

CREATE TABLE array_metrics(
	user_id NUMERIC,  
	month_start DATE, 
	metric_name TEXT,
	metrics_array INTEGER[],
	PRIMARY KEY (user_id, month_start, metric_name)
)

DO $$
DECLARE i INT := 0;
BEGIN
	WHILE i < 30 LOOP
	i := i + 1;
		INSERT INTO array_metrics 
		WITH 
		daily_aggregate AS (
			SELECT 
				user_id, 
				DATE(event_time) as date, 
				COUNT(1) AS num_site_hits
			FROM events
			WHERE DATE(event_time) = DATE(FORMAT('2023-01-%s', i + 1)) AND user_id IS NOT NULL
			GROUP BY user_id, DATE(event_time)
		), 
		yesterday_array AS (
			-- Note: Table has no value at all, before join
			SELECT * FROM array_metrics
			WHERE month_start = DATE('2023-01-01')
		)
		SELECT 
			COALESCE (da.user_id, ya.user_id) as user_id, 
			COALESCE (ya.month_start, DATE_TRUNC('month', da.date)) as month_start, 
			'site_hits' AS metrics_name,
			-- building Array 
			CASE 
				WHEN ya.metrics_array is NULL -- CASE when the yesterday is completely empty (starting)
					THEN ARRAY_FILL(0,  ARRAY[COALESCE(da.date - DATE(DATE_TRUNC('month', da.date)), 0)]) 
											||  
										ARRAY[COALESCE(da.num_site_hits, 0)]
				WHEN ya.metrics_array is NOT NULL -- CASE for the day after the first day (yestreday is not empty)
					THEN ya.metrics_array || ARRAY[COALESCE(da.num_site_hits, 0)]
			END AS metrics_array -- array of value of visit from the 1st, 2nd, third, 
		FROM daily_aggregate da
		FULL OUTER JOIN yesterday_array ya
		ON da.user_id = ya.user_id
		ON CONFLICT (user_id, month_start, metric_name) -- ON CONFLICT - how to handle conflict (DO NOTHING, UPDATE, ERROR)
		DO 
			UPDATE SET metrics_array = EXCLUDED.metrics_array; -- EXCLUDED use on the value attempt to insert
			-- if CONFLICT happened to (user_id, month_start, metric_name), set new value for metric_array
END LOOP;
END $$;

DELETE FROM array_metrics  ;
SELECT * FROM array_metrics;

SELECT CARDINALITY (metrics_array), COUNT(1) FROM array_metrics GROUP BY 1 ;
SELECT * FROM array_metrics am ;

--- 
WITH agg as (
SELECT metric_name, month_start ,
	ARRAY[SUM(metrics_array[1]), SUM(metrics_array[2]), SUM(metrics_array[3])] as summed_array
FROM array_metrics am 
GROUP BY metric_name, month_start 
)
SELECT * , metric_name, 
month_start + CAST(CAST (index - 1 AS TEXT) || 'day' as INTERVAL) as adjusted_date
FROM agg 
CROSS JOIN UNNEST(agg.summed_array) WITH ORDINALITY AS a(elem, index); ---????
