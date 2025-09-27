-- Window Functions...
USE maven_advanced_sql;

SELECT country, year, happiness_score
FROM happiness_scores;

SELECT country, year, happiness_score
FROM happiness_scores
ORDER BY country, year;

SELECT country, year, happiness_score,
	ROW_NUMBER() OVER() AS row_num
FROM happiness_scores
ORDER BY country, year;

SELECT country, year, happiness_score,
	ROW_NUMBER() OVER(PARTITION BY  country ) AS row_num
FROM happiness_scores
ORDER BY country, year;

SELECT country, year, happiness_score,
	ROW_NUMBER() OVER(PARTITION BY country ORDER BY year ) AS row_num
FROM happiness_scores
ORDER BY country, year;

SELECT country, year, happiness_score,
	ROW_NUMBER() OVER(PARTITION BY country ORDER BY happiness_score DESC ) AS row_num
FROM happiness_scores
ORDER BY country, year;

SELECT country, year, happiness_score,
	ROW_NUMBER() OVER(PARTITION BY country ORDER BY happiness_score DESC ) AS row_num
FROM happiness_scores
ORDER BY country, row_num;

SELECT country, year, happiness_score,
	ROW_NUMBER() OVER(PARTITION BY country ORDER BY happiness_score ASC ) AS row_num
FROM happiness_scores
ORDER BY country, row_num;

-- Assignment
SELECT * FROM orders;

SELECT customer_id, order_id, order_date, transaction_id
FROM orders;

SELECT customer_id, order_id, order_date, transaction_id,
	ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY transaction_id) AS transaction_number
FROM orders
ORDER BY customer_id, transaction_number;

-- ROW_NUMBER, RANK, DENSE_RANK

CREATE TABLE  baby_girl_names
(
name VARCHAR(255),
babies INT
);

INSERT INTO baby_girl_names (name, babies)
VALUES
('Olivia', 99),
('Emma',80),
("Chalotte", 80),
("Amelia",75),
("Sophia", 70),
("Isabella", 70),
("Ava", 70),
("Mia", 64);

SELECT * FROM baby_girl_names;

SELECT name, babies,
	ROW_NUMBER() OVER(ORDER BY babies DESC) AS babies_rn,
    RANK() OVER(ORDER BY babies DESC) AS babies_rank,
    DENSE_RANK() OVER(ORDER BY babies DESC) AS babies_dense_rank
FROM baby_girl_names;

-- Assignment
SELECT * FROM orders;

SELECT order_id, product_id, units,
	ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY units DESC) AS product_rn
FROM orders
ORDER BY order_id, product_rn;

SELECT order_id, product_id, units,
	RANK() OVER(PARTITION BY order_id ORDER BY units DESC) AS product_rank
FROM orders
ORDER BY order_id, product_rank;

SELECT order_id, product_id, units,
	DENSE_RANK() OVER(PARTITION BY order_id ORDER BY units DESC) AS product_rank
FROM orders
ORDER BY order_id, product_rank;


-- FIRST_VALUE, LAST_VALUE, NTH_VALUE
CREATE TABLE baby_names
(gender VARCHAR(50),
name VARCHAR(50),
babies INT
);

INSERT INTO baby_names (gender, name, babies)
VALUES
('Female', 'Charlotte', 80),
('Female', 'Emma', 82),
('Female', 'Olivia', 99),
('Male', 'James', 85),
('Male', 'Liam', 110),
('Male', 'Noah', 95);
    
SELECT * FROM baby_names;

SELECT gender, name, babies,
		FIRST_VALUE(name) OVER(PARTITION BY gender ORDER BY babies DESC) AS top_name
FROM baby_names;

SELECT * FROM
(SELECT gender, name, babies,
		FIRST_VALUE(name) OVER(PARTITION BY gender ORDER BY babies DESC) AS top_name
FROM baby_names) AS sq
WHERE name=top_name;

WITH top_name_tb AS
(
SELECT gender, name, babies,
		FIRST_VALUE(name) OVER(PARTITION BY gender ORDER BY babies DESC) AS top_name
FROM baby_names
)
SELECT *
FROM top_name_tb
WHERE name = top_name;

WITH bottom_name_tb AS (
    SELECT gender, name, babies,
           FIRST_VALUE(name) OVER (PARTITION BY gender ORDER BY babies ASC) AS bottom_name
    FROM baby_names
)
SELECT *
FROM bottom_name_tb
WHERE name = bottom_name;

SELECT gender, name, babies,
		NTH_VALUE(name, 2) OVER(PARTITION BY gender ORDER BY babies DESC) AS second_top_name
FROM baby_names;

SELECT * FROM
(
SELECT gender, name, babies,
		NTH_VALUE(name, 2) OVER(PARTITION BY gender ORDER BY babies DESC) AS second_top_name
FROM baby_names
) AS stb
WHERE name = second_top_name;

SELECT gender, name, babies,
		ROW_NUMBER() OVER(PARTITION BY gender ORDER BY babies DESC) AS popularity
FROM baby_names;

SELECT * FROM
(
SELECT gender, name, babies,
		ROW_NUMBER() OVER(PARTITION BY gender ORDER BY babies DESC) AS popularity
FROM baby_names
) AS rstb
WHERE popularity <= 2;

-- Assignment
SELECT order_id, product_id, units FROM orders;

WITH order_table AS (
SELECT order_id, product_id, units,
	ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY units DESC) AS popularity
FROM orders
ORDER BY order_id, popularity
)
SELECT * FROM order_table
WHERE popularity = 2;


WITH order_table AS (
SELECT order_id, product_id, units,
	NTH_VALUE(product_id, 2) OVER(PARTITION BY order_id ORDER BY units DESC) AS second_popularity
FROM orders
ORDER BY order_id, second_popularity
)
SELECT * FROM order_table
WHERE product_id = second_popularity;

WITH order_table AS (
SELECT order_id, product_id, units,
	DENSE_RANK() OVER(PARTITION BY order_id ORDER BY units DESC) AS dense_rk
FROM orders
ORDER BY order_id, dense_rk
)
SELECT * FROM order_table
WHERE dense_rk = 2;

-- Lag and Lead
SELECT country, year, happiness_score,
	ROW_NUMBER() OVER(PARTITION BY country ORDER BY year DESC) AS row_num
FROM happiness_scores;

SELECT country, year, happiness_score,
	LAG(happiness_score) OVER(PARTITION BY country ORDER BY year ASC) AS prior_happiness
FROM happiness_scores;

WITH prior_table AS 
(
SELECT country, year, happiness_score,
	LAG(happiness_score) OVER(PARTITION BY country ORDER BY year ASC) AS prior_happiness
FROM happiness_scores
)
SELECT *, (happiness_score - prior_happiness) AS deviation
FROM prior_table;


WITH prior_table AS 
(
SELECT country, year, happiness_score,
	LAG(happiness_score,2) OVER(PARTITION BY country ORDER BY year ASC) AS prior_happiness_shift_by_2
FROM happiness_scores
)
SELECT *, (happiness_score - prior_happiness_shift_by_2) AS deviation
FROM prior_table;

-- Assignment
SELECT * FROM orders;

SELECT customer_id, order_id, MIN(transaction_id) AS min_tx_id, MAX(transaction_id) AS max_tx_id,  COUNT(product_id) AS total_products, SUM(units) AS total_units 
FROM orders
GROUP BY customer_id, order_id
ORDER BY customer_id;

WITH ordered_table AS
(
SELECT customer_id, order_id,
 MIN(transaction_id) AS min_tx_id, MAX(transaction_id) AS max_tx_id,
 COUNT(product_id) AS total_products, SUM(units) AS total_units
FROM orders
GROUP BY customer_id, order_id
ORDER BY customer_id
),
pre_processed_tbl AS
(
SELECT *,
		LAG(total_units) OVER(PARTITION BY customer_id ORDER BY order_id) AS prior_order
FROM ordered_table
ORDER BY customer_id
)
SELECT *, 
COALESCE((total_units - prior_order),'No Result') AS diff_units
FROM pre_processed_tbl;

SELECT customer_id, SUM(units)
FROM orders
GROUP BY customer_id;

-- NTILE Function;to yield the precentile

SELECT region, country, happiness_score
FROM happiness_scores;


SELECT region, country, happiness_score,
		NTILE(4) OVER(PARTITION BY region ORDER BY happiness_score DESC) AS hs_percentile
FROM happiness_scores
WHERE year = 2023
ORDER BY region, happiness_score DESC;

WITH happiness_precentile AS
(
SELECT region, country, happiness_score,
		NTILE(4) OVER(PARTITION BY region ORDER BY happiness_score DESC) AS hs_percentile
FROM happiness_scores
WHERE year = 2023

)
SELECT * FROM
happiness_precentile
WHERE hs_percentile = 1
ORDER BY region, happiness_score DESC;

-- SAME without NTILE
WITH ranked_happiness AS (
  SELECT 
    region,
    country,
    happiness_score,
    RANK() OVER (PARTITION BY region ORDER BY happiness_score ASC) AS rnk,
    COUNT(*) OVER (PARTITION BY region) AS total_count
  FROM happiness_scores
  WHERE year = 2023
),
happiness_percentile AS (
  SELECT 
    region,
    country,
    happiness_score,
    rnk,
    total_count,
    (100 * rnk / total_count) AS percentile_rank
  FROM ranked_happiness
)
SELECT * 
FROM happiness_percentile
-- WHERE percentile_rank <= 0.25
ORDER BY region, happiness_score DESC;

-- Assignment
SELECT * FROM customers;
SELECT * FROM orders;

-- My way of thinking
WITH joined_tb AS
(
SELECT cs.customer_id, os.units
FROM customers cs
INNER JOIN orders os
WHERE cs.customer_id = os.customer_id
),
ranked_tb AS
(
SELECT *,
	DENSE_RANK() OVER(ORDER BY units DESC) AS rank_num,
    COUNT(*) OVER() AS total_count
FROM joined_tb
)

SELECT *,
		(100*(rank_num/total_count)) AS percentile
FROM ranked_tb
ORDER BY units DESC;


-- Using inbuild function
WITH joined_tb AS
(
SELECT cs.customer_id, os.units
FROM customers cs
INNER JOIN orders os
WHERE cs.customer_id = os.customer_id
),
ranked_tb AS
(
SELECT *,
	NTILE(4) OVER(ORDER BY units DESC) AS percentile
FROM joined_tb
)
SELECT * FROM ranked_tb
WHERE percentile = 1
ORDER BY units DESC;



-- ***********
WITH joined_tb AS (
SELECT o.customer_id,
	SUM(o.units * p.unit_price) AS total_spend
FROM orders o
LEFT JOIN products p
ON o.product_id = p.product_id
GROUP BY o.customer_id

)
SELECT *,
	NTILE(100) OVER(ORDER BY total_spend DESC) AS percentile
FROM joined_tb
ORDER BY total_spend DESC;


WITH joined_tb AS (
SELECT o.customer_id,
	SUM(o.units * p.unit_price) AS total_spend
FROM orders o
LEFT JOIN products p
ON o.product_id = p.product_id
GROUP BY o.customer_id

),
row_table AS
(
SELECT *,
	ROW_NUMBER() OVER(ORDER BY total_spend) AS row_num,
    COUNT(*) OVER() AS total_count
    
FROM joined_tb
),
final_tb AS 
(
SELECT *,
	(100* (row_num/total_count)) AS percentile
FROM row_table
)
SELECT * FROM final_tb
WHERE percentile >= 99.0
ORDER BY total_spend DESC;

WITH ranked_tb AS (
	SELECT customer_id,
			SUM(units) AS total_units,
			COUNT(*) OVER() AS total_count,
			ROW_NUMBER() OVER(ORDER BY SUM(units)) AS row_num
	FROM orders
	GROUP BY customer_id
	ORDER BY SUM(units) DESC
),
proc_table AS(
	SELECT customer_id,
			ROUND((row_num/total_count) * 100,2) AS  percentile    
	FROM ranked_tb
	
)

SELECT *,
		CASE
        WHEN percentile >= 90 THEN 'O Grade'
        WHEN percentile >= 80 THEN 'A+ Grade'
        WHEN percentile >= 70 THEN 'A Grade'
        WHEN percentile >= 60 THEN 'B+ Grade'
        WHEN percentile >= 50 THEN 'C+ Grade'
        WHEN percentile >= 40 THEN 'D+ Grade'
        ELSE 'Pass' END AS grade_credit
FROM proc_table
ORDER BY percentile DESC

