-- Connect to a database
USE maven_advanced_sql;

-- Joins
SELECT * FROM happiness_scores;

SELECT * FROM country_stats;

SELECT * 
FROM happiness_scores
JOIN country_stats; -- Default JOIN is INNER JOIN

SELECT *
FROM happiness_scores
INNER JOIN country_stats;

SELECT *
FROM happiness_scores hs
INNER JOIN country_stats cs
ON hs.country = cs.country;

-- INNER JOIN
SELECT hs.year, hs.country, hs.happiness_score,
		cs.country, cs.continent
FROM 	happiness_scores hs
		INNER JOIN country_stats cs
		ON hs.country = cs.country;
        
 -- LEFT OUTER JOIN       
SELECT hs.year, hs.country, hs.happiness_score,
		cs.country, cs.continent
FROM 	happiness_scores hs
		LEFT JOIN country_stats cs
		ON hs.country = cs.country;

 -- RIGHT OUTER JOIN          
SELECT hs.year, hs.country, hs.happiness_score,
		cs.country, cs.continent
FROM 	happiness_scores hs
		RIGHT JOIN country_stats cs
		ON hs.country = cs.country;
        
SELECT DISTINCT hs.country
FROM 	happiness_scores hs
		LEFT JOIN country_stats cs
		ON hs.country = cs.country
        WHERE cs.country IS NULL;

SELECT DISTINCT cs.country
FROM 	happiness_scores hs
		RIGHT JOIN country_stats cs
		ON hs.country = cs.country
        WHERE hs.country IS NULL;
        
        
-- Assignement

SELECT * FROM orders;
SELECT * FROM products;

SELECT COUNT(DISTINCT product_id) FROM orders;
SELECT COUNT(DISTINCT product_id) FROM products;

SELECT COUNT(*)
FROM orders od 
LEFT JOIN products pd
ON pd.product_id = od.product_id; -- 8549

SELECT COUNT(*)
FROM orders od 
RIGHT JOIN products pd
ON pd.product_id = od.product_id;  -- 8552

SELECT COUNT(*)
FROM orders od 
LEFT JOIN products pd
ON pd.product_id = od.product_id
WHERE pd.product_id IS NULL; -- 0

SELECT COUNT(*)
FROM orders od 
RIGHT JOIN products pd
ON pd.product_id = od.product_id
WHERE od.product_id IS NULL;  -- 3


-- Final Query
SELECT DISTINCT pd.product_name, pd.product_id
FROM products pd
LEFT JOIN orders od
ON pd.product_id = od.product_id
WHERE od.order_id IS NULL;


-- JOIN on Multiple Columns

SELECT * FROM happiness_scores;
SELECT * FROM country_stats;
SELECT * FROM inflation_rates;


SELECT *
FROM happiness_scores hs
INNER JOIN inflation_rates ir
ON hs.country = ir.country_name;
      
      
SELECT *
FROM happiness_scores hs
INNER JOIN inflation_rates ir
ON hs.country = ir.country_name AND hs.year = ir.year;

-- JOINING MULTIPLE TABLES
SELECT * FROM happiness_scores;
SELECT * FROM country_stats;
SELECT * FROM inflation_rates;

SELECT hs.year, hs.country, hs.happiness_score,
		cs.continent, ir.inflation_rate
FROM happiness_scores hs
		LEFT JOIN country_stats cs
			ON hs.country = cs.country
		LEFT JOIN inflation_rates ir
			ON hs.year = ir.year AND hs.country = ir.country_name;
            
-- SELF JOINS
CREATE TABLE IF NOT EXISTS employees (
	employee_id INT PRIMARY KEY,
    employee_name VARCHAR(100),
    salary INT,
    manager_id INT
);

INSERT INTO employees (employee_id, employee_name, salary, manager_id)
VALUES
(1,'Ava',85000, NULL),
(2, 'BOB',72000,1),
(3,'Cat',59000,1),
(4,'Dan',85000,2);

SELECT * FROM employees;

-- Employees with same salary

SELECT e1.employee_id, e1.employee_name, e1.salary, 
		e2.employee_id, e2.employee_name, e2.salary
FROM employees e1
INNER JOIN employees e2
ON e1.salary = e2.salary
WHERE e1.employee_name <> e2.employee_name
AND e1.employee_id < e2.employee_id;


-- Employee with greater salary
SELECT e1.employee_id, e1.employee_name, e1.salary, 
		e2.employee_id, e2.employee_name, e2.salary
FROM employees e1
INNER JOIN employees e2
ON e1.salary > e2.salary
ORDER BY e1.employee_id;

-- Employees and their managers

SELECT e1.employee_id, e1.employee_name,e1.manager_id,
		e2.employee_name AS manager_name
FROM employees e1 LEFT JOIN employees e2
		ON e1.manager_id = e2.employee_id;
        
-- Assignment

SELECT * FROM products;

SELECT p1.product_name, p1.unit_price,
		p2.product_name, p2.unit_price,
        p1.unit_price - p2.unit_price AS price_diff
FROM products p1
INNER JOIN products p2
ON p1.unit_price = p2.unit_price
WHERE p1.product_name != p2.product_name;


SELECT p1.product_name, p1.unit_price,
		p2.product_name, p2.unit_price,
        p1.unit_price - p2.unit_price AS price_diff
FROM products p1
INNER JOIN products p2
ON p1.product_id <> p2.product_id
WHERE ABS(p1.unit_price - p2.unit_price) < 0.25
AND p1.product_name < p2.product_name
ORDER BY price_diff DESC;


-- CROSS JOIN
SELECT count(*) from customers; -- 3192
SELECT count(*) FROM inflation_rates; -- 189

SELECT COUNT(*) FROM customers
CROSS JOIN inflation_rates;  -- 603288 = 3192*189 : Cartersian Product of two sets A x B


SELECT p1.product_name, p1.unit_price,
		p2.product_name, p2.unit_price,
        p1.unit_price - p2.unit_price AS price_diff
FROM products p1
CROSS JOIN products p2
WHERE ABS(p1.unit_price - p2.unit_price) < 0.25
AND p1.product_name < p2.product_name
-- AND p1.product_id <> p2.product_id
ORDER BY price_diff DESC;


-- UNION and UNION ALL
SELECT * FROM happiness_scores;
SELECT * FROM country_stats;


SELECT * FROM happiness_scores
UNION
SELECT * FROM country_stats;

SELECT * FROM happiness_scores
UNION ALL
SELECT * FROM country_stats;

-- Union with different column names

SELECT * FROM happiness_scores;
SELECT * FROM happiness_scores_current;

SELECT year, country, happiness_score
FROM happiness_scores
UNION
SELECT 2025, country, ladder_score FROM happiness_scores_current;

SELECT year, country, happiness_score
FROM happiness_scores
UNION ALL
SELECT 2025, country, ladder_score FROM happiness_scores_current;
 
 
