CREATE TABLE input_table (
    Match_ID INT PRIMARY KEY,
    Team_1 VARCHAR(50),
    Team_2 VARCHAR(50),
    Result VARCHAR(50)
);
INSERT INTO input_table (Match_ID, Team_1, Team_2, Result) VALUES
(1, 'India', 'Australia', 'India'),
(2, 'England', 'India', 'England'),
(3, 'Australia', 'England', 'Australia'),
(4, 'India', 'England', 'India'),
(5, 'Australia', 'India', 'Australia'),
(6, 'England', 'Australia', 'Draw');


WITH all_matches AS (
	SELECT team_1 AS team, Result
	FROM input_table
	UNION ALL
	SELECT team_2 AS team, Result
	FROM input_table
),
grouped_table AS
(
	SELECT team,
		COUNT(*) AS MatchesPlayed,
		SUM(CASE WHEN team = RESULT THEN 1 ELSE 0 END) AS MatchesWon,
        SUM(CASE WHEN RESULT = 'Draw'  THEN 1 ELSE 0 END) AS MatchesDraw
	FROM all_matches
	GROUP BY team
)
SELECT * FROM grouped_table
ORDER BY MatchesWon DESC;