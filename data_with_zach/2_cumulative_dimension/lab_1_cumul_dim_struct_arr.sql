
-- Temporal problem
-- When joining table, shuffle happen, lose compression
-- TODO: A table with 1 row per player, has array per season
-- NOTE
-- The seed query for cumulation
-- Give ability for faster historical analysis (almost no group by)


-- Struct
CREATE TYPE seasons_stats AS (
	season INTEGER, 
	gp INTEGER , 
	pts REAL, 
	reb REAL, 
	ast REAL 
)
-- like dataclass 
CREATE TYPE scoring_class AS ENUM ('star', 'good', 'average', 'bad');

CREATE TABLE players (
	player_name TEXT, 
	height TEXT, 
	college TEXT, 
	country TEXT, 
	draft_year TEXT, 
	draft_round TEXT, 
	draft_number TEXT, 
	seasons_stats seasons_stats[],
	scoring_class scoring_class ,
	years_since_last_season INTEGER , 
	current_season INTEGER, 
	PRIMARY KEY (player_name, current_season)
)

-- DROP TABLE players;

SELECT MIN(season) FROM player_seasons as ps ;

DO $$
DECLARE i INT := 1994;
BEGIN
	WHILE i < 2001 LOOP
	i := i + 1; ---------------------- gereate cumulative table start form 1995 to 2001
INSERT INTO players
WITH yesterday AS (
	-- The seed query for cumulationm (null on the first pipeline iteration)
	SELECT * FROM players 
	WHERE current_season = i
), 
today AS (
	SELECT * FROM player_seasons as ps
	WHERE season = i + 1
)
SELECT 
	COALESCE (t.player_name, y.player_name) AS player_name, 
	COALESCE (t.height, y.height) AS height,
	COALESCE (t.college, y.college) AS college,
	COALESCE (t.country, y.country) AS country,
	COALESCE (t.draft_year, y.draft_year) AS draft_year,
	COALESCE (t.draft_round, y.draft_round) AS draft_round,
	COALESCE (t.draft_number, y.draft_number) AS draft_number,
	CASE 
		WHEN y.seasons_stats IS NULL 
			THEN ARRAY [ROW(
						t.season, 
						t.gp, 
						t.pts, 
						t.reb, 
						t.ast
					)::seasons_stats]
		WHEN t.season IS NOT NULL -- not taking retired players
			THEN y.seasons_stats || ARRAY [ROW(
						t.season, 
						t.gp, 
						t.pts, 
						t.reb, 
						t.ast
					)::seasons_stats]
		ELSE y.seasons_stats
	END as season_stats, 
	CASE 
		WHEN t.season IS NOT NULL THEN 
		CASE
			WHEN t.pts > 20 THEN 'star'
			WHEN t.pts > 15 THEN 'good'
			WHEN t.pts > 10 THEN 'average'
			ELSE 'bad'
		END::scoring_class 
		ELSE y.scoring_class
	END, 
	CASE 
		WHEN t.season IS NOT NULL THEN 0 -- player still playing
		ELSE y.years_since_last_season + 1 -- incrmenting as the player retired
	END as years_since_last_season , 	
	COALESCE (t.season, y.current_season + 1) AS current_season
FROM today t FULL OUTER JOIN yesterday y
ON t.player_name = y.player_name;
END LOOP;
END $$;

SELECT * FROM players;

------------------------------------------------------------------
---- ANALYSIS
SELECT 
	player_name,
	(UNNEST(seasons_stats)::seasons_stats).*  AS seasons_stats
FROM players WHERE current_season = 2001
AND player_name = 'Michael Jordan';
-------------------------------------------------------------------
--- Create scoring class player 
SELECT * FROM players WHERE current_season = 2001
AND player_name = 'Michael Jordan';
-------------------------------------------------------------------
--- Create analytics table for player that has improvement from last season
SELECT player_name, 
	(seasons_stats[1]::seasons_stats).pts AS first_season, 
	(seasons_stats[CARDINALITY(seasons_stats)]::seasons_stats).pts as latest_season, -- CARDINALITY - len of unique
	(seasons_stats[CARDINALITY(seasons_stats)]::seasons_stats).pts/
	CASE -- avoid divide by 0
		WHEN (seasons_stats[1]::seasons_stats).pts = 0 THEN 1 
		ELSE (seasons_stats[1]::seasons_stats).pts
	END as improve_pct -- > 0 improve, < regress
FROM players as ps
WHERE current_season = 2001;
