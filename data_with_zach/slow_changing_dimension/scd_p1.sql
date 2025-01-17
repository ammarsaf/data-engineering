-- Slowly Changing Dimensional (SCD)

-- Setup table

DROP TABLE players ;
 CREATE TYPE season_stats AS (
                         season Integer,
                         pts REAL,
                         ast REAL,
                         reb REAL,
                         weight INTEGER
                       );
 CREATE TYPE scoring_class AS
     ENUM ('bad', 'average', 'good', 'star');


CREATE TABLE players (
     player_name TEXT,
     height TEXT,
     college TEXT,
     country TEXT,
     draft_year TEXT,
     draft_round TEXT,
     draft_number TEXT,
     seasons season_stats[],
     scoring_class scoring_class,
     years_since_last_active INTEGER,
     is_active BOOLEAN,
     current_season INTEGER,
     PRIMARY KEY (player_name, current_season)
 );


INSERT INTO players
WITH years AS (
    SELECT *
    FROM GENERATE_SERIES(1996, 2022) AS season
), p AS (
    SELECT
        player_name,
        MIN(season) AS first_season
    FROM player_seasons
    GROUP BY player_name
), players_and_seasons AS (
    SELECT *
    FROM p
    JOIN years y
        ON p.first_season <= y.season
), windowed AS (
    SELECT
        pas.player_name,
        pas.season,
        ARRAY_REMOVE(
            ARRAY_AGG(
                CASE
                    WHEN ps.season IS NOT NULL
                        THEN ROW(
                            ps.season,
                            ps.gp,
                            ps.pts,
                            ps.reb,
                            ps.ast
                        )::season_stats
                END)
            OVER (PARTITION BY pas.player_name ORDER BY COALESCE(pas.season, ps.season)),
            NULL
        ) AS seasons
    FROM players_and_seasons pas
    LEFT JOIN player_seasons ps
        ON pas.player_name = ps.player_name
        AND pas.season = ps.season
    ORDER BY pas.player_name, pas.season
), static AS (
    SELECT
        player_name,
        MAX(height) AS height,
        MAX(college) AS college,
        MAX(country) AS country,
        MAX(draft_year) AS draft_year,
        MAX(draft_round) AS draft_round,
        MAX(draft_number) AS draft_number
    FROM player_seasons
    GROUP BY player_name
)
SELECT
    w.player_name,
    s.height,
    s.college,
    s.country,
    s.draft_year,
    s.draft_round,
    s.draft_number,
    seasons AS season_stats,
    CASE
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 20 THEN 'star'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 15 THEN 'good'
        WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 10 THEN 'average'
        ELSE 'bad'
    END::scoring_class AS scoring_class,
    w.season - (seasons[CARDINALITY(seasons)]::season_stats).season as years_since_last_active,
    (seasons[CARDINALITY(seasons)]::season_stats).season = season AS is_active, 
    w.season
FROM windowed w
JOIN static s
    ON w.player_name = s.player_name;
    
   
--- LESSON START

-- SCD table
   -- To track change of certain columns
   
DROP TABLE players_scd;

CREATE TABLE players_scd (
		player_name TEXT, 
		scoring_class scoring_class , 
		is_active BOOLEAN , 
		start_season INTEGER, 
		end_season INTEGER, 
		current_season INTEGER , 
		PRIMARY KEY (player_name, start_season)
)
  

INSERT INTO players_scd 
WITH with_previous AS (
SELECT 
	player_name, 
	scoring_class, 
	is_active, 
	current_season ,
	LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class, -- shift lag 
	LAG(is_active, 1) OVER (PARTITION BY player_name ORDER BY current_season) AS previous_is_active -- shift lag 
FROM players
WHERE current_season <= 2021
), 
with_indicator AS (
SELECT *, 
	CASE 
		WHEN scoring_class <> previous_scoring_class THEN 1
		WHEN is_active <> previous_is_active THEN 1
		ELSE 0
	END AS change_indicator
FROM with_previous
), 
with_streaks AS (
SELECT *, 
	SUM(change_indicator) OVER (PARTITION BY player_name 
			ORDER BY current_season) AS streak_identifier -- how many time changges happen
FROM with_indicator
)
SELECT 
	player_name, 
	scoring_class, 
	is_active, 
	MIN(current_season) AS start_season, 
	MAX(current_season) AS end_season, 
	2021 as current_season
FROM with_streaks 
GROUP BY player_name, streak_identifier , is_active, scoring_class
ORDER BY player_name, start_season;
