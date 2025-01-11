--- CUMULATIVE TABLE DESIGN + DATE LIST 
--- CREATE NEW TABLE WITH 
	-- user_id (unique)
	-- date_active (list of active time)
	-- date (current date for the list of active time)

SELECT * FROM events e;

SELECT MAX(event_time), MIN(event_time)
FROM events e;

SELECT user_id , MAX(event_time), MIN(event_time)
FROM events e 
GROUP BY user_id ;

DROP TABLE users_cumulated ;
DELETE FROM users_cumulated ;

CREATE TABLE users_cumulated (
	user_id TEXT, 
	-- list of dates in the past where user active
	dates_active DATE[], 
	-- current date of user
	date DATE, 
	PRIMARY KEY (user_id, date)
);

---- CREATE CUMULATIVE TABLE DESIGN + DATE LIST 
DO $$
DECLARE i INT := 0;
BEGIN
	WHILE i < 30 LOOP
	i := i + 1;
		INSERT INTO users_cumulated 
		WITH 
		yesterday AS (
			SELECT 
				* 
			FROM users_cumulated 
			WHERE date = DATE(FORMAT('2023-01-%s', i))
		), 
		today AS (
				SELECT 
					CAST(user_id as TEXT) as user_id, 
					DATE(CAST(event_time as TIMESTAMP)) as date_active
				FROM events e 
				WHERE 
					DATE(CAST(event_time AS TIMESTAMP)) = DATE(FORMAT('2023-01-%s', i + 1))
					AND user_id IS NOT NULL
				GROUP BY user_id, DATE(CAST(event_time AS TIMESTAMP))
		)
		SELECT COALESCE (t.user_id, y.user_id) as user_id , 
			CASE 
				WHEN y.dates_active IS NULL THEN ARRAY[t.date_active]
				WHEN t.date_active IS NULL THEN y.dates_active
				ELSE ARRAY[t.date_active] || y.dates_active
				END AS dates_active, 
				-- t.date_active as tdate, y.date AS ydate,
			COALESCE (t.date_active, y.date + INTERVAL '1 day') as date
		FROM today t 
		FULL OUTER JOIN yesterday y 
		ON t.user_id = y.user_id;
END LOOP;
END $$;

SELECT * FROM users_cumulated 
ORDER BY date;


