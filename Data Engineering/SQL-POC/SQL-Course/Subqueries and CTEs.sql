-- Subquery and Common Table Expressions (CTE)

-- All the queries inside the subquery are executed first before the actual query
-- A sub query always starts with Select statement

USE maven_advanced_sql;

SELECT * FROM happiness_scores;


SELECT AVG(happiness_score) FROM happiness_scores;

SELECT year, country, happiness_score,
(SELECT AVG(happiness_score) FROM happiness_scores) AS avg_hs,
happiness_score - (SELECT AVG(happiness_score) FROM happiness_scores) AS diff_from_avg
FROM happiness_scores;

-- Assignment

SELECT * FROM products;

SELECT product_id, product_name, unit_price,
	(SELECT AVG(unit_price) FROM products) AS avg_unit_price,
    (unit_price - (SELECT AVG(unit_price) FROM products)) AS diff_from_avg
FROM products
ORDER BY diff_from_avg DESC;


-- Subqueries in the FROM clause

SELECT * FROM country_stats;
SELECT * FROM happiness_scores;

-- Average Happiness score for each country
SELECT  country, AVG(happiness_score) as avg_hs, COUNT(happiness_score) AS total_rec,
CEIL((AVG(happiness_score) * COUNT(happiness_score))) AS total_happiness
FROM happiness_scores 
GROUP BY country;

-- Return each countries happiness score for the year alongside the countries happiness score

SELECT hs.year, hs.country, hs.happiness_score,
	cs.avg_happiness_score AS avg_country_happiness_score
FROM happiness_scores hs
LEFT JOIN 
	(SELECT country, AVG(happiness_score) AS avg_happiness_score
    FROM happiness_scores
    GROUP BY country) AS cs
    ON hs.country = cs.country
ORDER BY hs.country, hs.year;


-- MULTIPLE Subquieries
SELECT DISTINCT year FROM happiness_scores;
SELECT * FROM happiness_scores_current;

SELECT year, country, happiness_score FROM happiness_scores
UNION ALL
SELECT 2024, country, ladder_score FROM happiness_scores_current;


SELECT hs.year, hs.country, hs.happiness_score,
	cs.avg_happiness_score AS avg_country_happiness_score
FROM 
	(
	SELECT year, country, happiness_score FROM happiness_scores
	UNION ALL
	SELECT 2024, country, ladder_score FROM happiness_scores_current
	) hs
LEFT JOIN 
	(
    SELECT country, AVG(happiness_score) AS avg_happiness_score
    FROM happiness_scores
    GROUP BY country
    ) AS cs
ON hs.country = cs.country
ORDER BY hs.country, hs.year;


SELECT * FROM
(
SELECT hs.year, hs.country, hs.happiness_score,
	cs.avg_happiness_score AS avg_country_happiness_score
FROM 
	(
	SELECT year, country, happiness_score FROM happiness_scores
	UNION ALL
	SELECT 2024, country, ladder_score FROM happiness_scores_current
	) hs
LEFT JOIN 
	(
    SELECT country, AVG(happiness_score) AS avg_happiness_score
    FROM happiness_scores
    GROUP BY country
    ) AS cs
ON hs.country = cs.country
ORDER BY hs.country, hs.year
) AS total_hs
WHERE happiness_score > avg_country_happiness_score + 1;

-- ASSIGNMENT
SELECT * FROM products;


SELECT p1.factory, p2.product_name, p1.total_num_of_products FROM
(
SELECT pd1.factory, 'No Value' AS product_name, count(pd1.product_name) as total_num_of_products
FROM products pd1
GROUP BY pd1.factory
UNION ALL
SELECT pd2.factory, pd2.product_name, count(pd2.product_name) as total_num_of_products
FROM products pd2
GROUP BY pd2.product_name, pd2.factory
) p1
LEFT JOIN
(SELECT pd2.factory, pd2.product_name, count(pd2.product_name) as total_num_of_products
FROM products pd2
GROUP BY pd2.product_name, pd2.factory) p2
ON p1.factory = p2.factory

WHERE p1.total_num_of_products > 1
ORDER BY p1.factory;


-- """"""""""""""""""""""""""""""""""

SELECT factory, product_name
FROM products;


SELECT factory, COUNT(product_id)
FROM products
GROUP BY factory;

-- Actual Result
SELECT fp.factory, fp.product_name, fn.number_of_products FROM
(SELECT factory, product_name
FROM products) fp
LEFT JOIN
(SELECT factory, COUNT(product_id) AS number_of_products
FROM products
GROUP BY factory) fn
ON fp.factory = fn.factory
ORDER BY fp.factory;

-- Subqueries inside WHERE and HAVING
SELECT AVG(happiness_score) FROM happiness_scores;

SELECT * FROM happiness_scores
WHERE happiness_score > (SELECT AVG(happiness_score) FROM happiness_scores);

SELECT region, AVG(happiness_score) AS avg_hs
FROM happiness_scores
GROUP BY region;

SELECT region, AVG(happiness_score) AS avg_hs
FROM happiness_scores
GROUP BY region
HAVING avg_hs > (SELECT AVG(happiness_score) FROM happiness_scores);

-- ANY and ALL
SELECT *
FROM happiness_scores
WHERE happiness_score >
	ANY(SELECT ladder_score
    FROM happiness_scores_current);
    
    
SELECT *
FROM happiness_scores
WHERE happiness_score >
	ALL (SELECT ladder_score
    FROM happiness_scores_current);
    
-- EXISTS
SELECT * FROM happiness_scores;
SELECT * FROM inflation_rates;

SELECT *
FROM happiness_scores h
WHERE EXISTS (
		SELECT i.country_name
        FROM inflation_rates i
		WHERE i.country_name = h.country
        );

SELECT *
FROM happiness_scores h
	INNER JOIN inflation_rates i
    ON i.country_name = h.country AND i.year = h.year;

-- Assignment
SELECT * FROM products;

SELECT MIN(unit_price) AS total_price FROM products WHERE factory = "Wicked Choccy's";

SELECT product_id, product_name, factory, division, unit_price
FROM products
WHERE unit_price <
		(SELECT MIN(unit_price) AS total_price FROM products WHERE factory = "Wicked Choccy's")
 AND  factory != "Wicked Choccy's"  ;
 
 
 -- Common Table Expression CTE
 
 SELECT hs.year, hs.country, hs.happiness_score,
	cs.avg_happiness_score AS avg_country_happiness_score
FROM happiness_scores hs
LEFT JOIN 
	(SELECT country, AVG(happiness_score) AS avg_happiness_score
    FROM happiness_scores
    GROUP BY country) AS cs
    ON hs.country = cs.country;


WITH country_hs AS 
(
SELECT country, AVG(happiness_score) AS avg_hs_by_country
    FROM happiness_scores
    GROUP BY country
)
SELECT hs.year, hs.country, hs.happiness_score,
	cs.avg_hs_by_country
FROM happiness_scores hs
LEFT JOIN country_hs cs
ON hs.country = cs.country;

-- CTEs: Reusability

WITH 2023_data AS (
SELECT * FROM happiness_scores WHERE year = 2023
)
SELECT hs1.region, hs1.country, hs1.happiness_score,
		hs2.region, hs2.country, hs2.happiness_score 
FROM 2023_data hs1
INNER JOIN 2023_data hs2
ON hs1.region = hs2.region
WHERE hs1.country > hs2.country; 

-- Assignment
SELECT * FROM orders;
SELECT * FROM products;

SELECT COUNT(*) as total_products, order_id FROM orders
GROUP BY order_id;

WITH total_orders AS
(
SELECT od.order_id, od.product_id, od.units, pd.unit_price, (od.units * pd.unit_price) as total_amount_spend
FROM orders od
INNER JOIN products pd
ON pd.product_id = od.product_id
),
final_table AS (
SELECT order_id, SUM(total_amount_spend) AS total_amount_spend
FROM total_orders
GROUP BY order_id
HAVING SUM(total_amount_spend) > 200
ORDER BY total_amount_spend DESC
)
SELECT COUNT(*) FROM final_table;
 
 
 -- ****************************
 
 SELECT * FROM happiness_scores WHERE year = 2023;
 SELECT * FROM happiness_scores ;
 
SELECT * FROM
(
WITH hs23 AS (
 SELECT * FROM happiness_scores WHERE year = 2023
 ),
hs24 AS (
 SELECT * FROM happiness_scores_current
 )

SELECT hs23.country,
	hs23.happiness_score AS hs_2023,
    hs24.ladder_score AS hs_2024
FROM hs23 LEFT JOIN hs24
	ON hs23.country = hs24.country) as hs_23_24

where hs_2024 > hs_2023;




    

 
