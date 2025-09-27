-- Final Project

-- Assignment 1
-- Task 1
SELECT * FROM schools;
SELECT * FROM school_details;

SELECT ROUND(yearID, -1) AS decade, COUNT(DISTINCT schoolID)
FROM schools
GROUP BY decade
ORDER BY decade;

-- Task 2
SELECT schoolID, COUNT(DISTINCT playerID) as count
FROM schools
GROUP BY schoolID;

WITH od AS (
	SELECT sd.name_full, COUNT(DISTINCT playerID) as num_of_players
	FROM schools s
	INNER JOIN school_details sd
	ON sd.schoolID = s.schoolID
	GROUP BY s.schoolID
),
rd AS (
	SELECT *, 
			ROW_NUMBER() OVER(ORDER BY num_of_players DESC) AS row_num
	FROM od
)
SELECT name_full, num_of_players
FROM rd
WHERE row_num <=5;

-- Task 3
WITH od AS (
SELECT sd.name_full, COUNT(DISTINCT playerID) as num_of_players,  ROUND(s.yearID, -1) AS decade
	FROM schools s
	INNER JOIN school_details sd
	ON sd.schoolID = s.schoolID
	GROUP BY s.schoolID, decade
),
rd AS (
	SELECT *, 
			ROW_NUMBER() OVER(PARTITION BY decade ORDER BY num_of_players DESC) AS row_num
	FROM od
)
SELECT decade, name_full, num_of_players
FROM rd
WHERE row_num <=5
ORDER BY decade DESC;

-- Assignment 2
SELECT * FROM salaries;

-- Task-1
WITH ts AS (
SELECT teamID, yearID, SUM(salary) AS total_spend
FROM salaries
GROUP BY teamID, yearID
),
sp AS
(
SELECT teamID, AVG(total_spend) as avg_spend,
		NTILE(5) OVER(ORDER BY AVG(total_spend) DESC) AS spend_percentage
FROM ts
GROUP BY teamID
)
SELECT teamID, ROUND(avg_spend / 1000000, 1) AS avg_spend_in_mil
FROM sp
WHERE spend_percentage = 1
ORDER BY avg_spend_in_mil DESC;

-- Task 2

WITH ts AS (
SELECT teamID, yearID, SUM(salary) AS total_spend
FROM salaries
GROUP BY teamID, yearID
)
SELECT teamID, yearID, total_spend,
		ROUND((SUM(total_spend) OVER(PARTITION BY teamID ORDER BY yearID ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW))/1000000, 1) AS cumilative_sum_in_millions
FROM ts;

-- Task 3
WITH ts AS (
SELECT teamID, yearID, SUM(salary) AS total_spend
FROM salaries
GROUP BY teamID, yearID
), 
cs AS (
SELECT teamID, yearID, total_spend,
		SUM(total_spend) OVER(PARTITION BY teamID ORDER BY yearID ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)AS cumilative_sum
FROM ts
),
rk AS (
SELECT * FROM cs WHERE cumilative_sum > 1000000000
),
ft AS (
SELECT teamID, yearID, total_spend, cumilative_sum,
		ROW_NUMBER() OVER(PARTITION BY teamID ORDER BY yearID) AS row_num
FROM rk
)
SELECT * FROM ft
WHERE row_num = 1


