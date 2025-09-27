USE maven_advanced_sql;

-- Duplicate Values
CREATE TABLE employee_details (
    region VARCHAR(50),
    employee_name VARCHAR(50),
    salary INTEGER
);

INSERT INTO employee_details (region, employee_name, salary) VALUES
	('East', 'Ava', 85000),
	('East', 'Ava', 85000),
	('East', 'Bob', 72000),
	('East', 'Cat', 59000),
	('West', 'Cat', 63000),
	('West', 'Dan', 85000),
	('West', 'Eve', 72000),
	('West', 'Eve', 75000);

-- View the employee details table
SELECT * FROM employee_details;

SELECT employee_name, COUNT(*) AS dup_count
FROM employee_details
GROUP BY employee_name
HAVING COUNT(*) > 1;

SELECT region, employee_name, salary, COUNT(*) AS dup_count
FROM employee_details
GROUP BY employee_name, region, salary
HAVING COUNT(*) > 1;

-- Exclude Fully duplicate Rows
SELECT DISTINCT region, employee_name, salary
FROM employee_details;


-- Using window function
SELECT region, employee_name, salary,
		ROW_NUMBER() OVER(PARTITION BY employee_name ORDER BY salary DESC) AS row_nm
FROM employee_details;

SELECT * FROM
(
SELECT region, employee_name, salary,
		ROW_NUMBER() OVER(PARTITION BY employee_name ORDER BY salary DESC) AS row_nm
FROM employee_details
) AS st
WHERE row_nm = 1;

SELECT * FROM
(
SELECT region, employee_name, salary,
		ROW_NUMBER() OVER(PARTITION BY employee_name, region ORDER BY salary DESC) AS row_nm
FROM employee_details
) AS st
WHERE row_nm = 1;

-- Assignment
SELECT * FROM students;

SELECT id, student_name, email
FROM students;

SELECT id, student_name, email, COUNT(*) AS rec_num
FROM students
GROUP BY id;

SELECT * FROM
(
SELECT id, student_name, email,
		ROW_NUMBER() OVER(PARTITION BY student_name ORDER BY id DESC) AS row_num
FROM students
) AS d_tb
WHERE row_num =1;

-- Min and Max
CREATE TABLE sales (
    id INT PRIMARY KEY,
    sales_rep VARCHAR(50),
    date DATE,
    sales INT
);

INSERT INTO sales (id, sales_rep, date, sales) VALUES 
    (1, 'Emma', '2024-08-01', 6),
    (2, 'Emma', '2024-08-02', 17),
    (3, 'Jack', '2024-08-02', 14),
    (4, 'Emma', '2024-08-04', 20),
    (5, 'Jack', '2024-08-05', 5),
    (6, 'Emma', '2024-08-07', 1);

-- View the sales table
SELECT * FROM sales;

SELECT sales_rep, MAX(sales) AS recent_sale
FROM sales
GROUP BY sales_rep;

WITH date_tb AS(
SELECT sales_rep, MAX(date) AS recent_sale_date
FROM sales
GROUP BY sales_rep
)
SELECT dt.sales_rep, dt.recent_sale_date, s.sales
FROM date_tb dt
LEFT JOIN sales s
ON dt.sales_rep = s.sales_rep
AND dt.recent_sale_date = s.date;


WITH ce AS (
SELECT sales_rep, date, sales,
	ROW_NUMBER() OVER(PARTITION BY sales_rep ORDER BY date DESC) as rn
FROM sales)
SELECT sales_rep, date, sales
FROM ce
WHERE rn = 1;

-- Assignment
SELECT * FROM students;
SELECT * FROM student_grades;

WITH gt AS (
SELECT s.id, s.student_name, sg.class_name, sg.final_grade
FROM students s
INNER JOIN student_grades sg
ON s.id = sg.student_id
),
od AS (
SELECT *,
		ROW_NUMBER() OVER(PARTITION BY student_name ORDER BY final_grade DESC) AS rn
FROM gt
)
SELECT id, student_name, final_grade AS top_grade, class_name 
FROM od
WHERE rn = 1
ORDER BY top_grade DESC;

-- Pivoting
-- Allows to transform rows to summarized columns

CREATE TABLE pizza_table (
    category VARCHAR(50),
    crust_type VARCHAR(50),
    pizza_name VARCHAR(100),
    price DECIMAL(5, 2)
);

INSERT INTO pizza_table (category, crust_type, pizza_name, price) VALUES
    ('Chicken', 'Gluten-Free Crust', 'California Chicken', 21.75),
    ('Chicken', 'Thin Crust', 'Chicken Pesto', 20.75),
    ('Classic', 'Standard Crust', 'Greek', 21.50),
    ('Classic', 'Standard Crust', 'Hawaiian', 19.50),
    ('Classic', 'Standard Crust', 'Pepperoni', 18.75),
    ('Supreme', 'Standard Crust', 'Spicy Italian', 22.75),
    ('Veggie', 'Thin Crust', 'Five Cheese', 18.50),
    ('Veggie', 'Thin Crust', 'Margherita', 19.50),
    ('Veggie', 'Gluten-Free Crust', 'Garden Delight', 21.50);

-- View the pizza table
SELECT * FROM pizza_table;


SELECT *,
		CASE WHEN crust_type = 'Standard Crust' THEN 1 ELSE 0 END AS standard_crust,
        CASE WHEN crust_type = 'Thin Crust' THEN 1 ELSE 0 END AS thin_crust,
        CASE WHEN crust_type = 'Gluten-Free Crust' THEN 1 ELSE 0 END AS gluten_free_crsut
FROM pizza_table;

WITH ct AS
(
SELECT *,
		CASE WHEN crust_type = 'Standard Crust' THEN 1 ELSE 0 END AS standard_crust,
        CASE WHEN crust_type = 'Thin Crust' THEN 1 ELSE 0 END AS thin_crust,
        CASE WHEN crust_type = 'Gluten-Free Crust' THEN 1 ELSE 0 END AS gluten_free_crsut
FROM pizza_table
)

SELECT category,
		SUM(standard_crust) AS total_standard_crust,
        SUM(thin_crust) AS total_thin_crust,
        SUM(gluten_free_crsut) AS total_gluten_free_crsut
FROM ct
GROUP BY category;

-- Assignment
SELECT * FROM student_grades;

SELECT * FROM students;

WITH gt AS (
	SELECT s.grade_level, sg.department, sg.final_grade
		FROM students s
		INNER JOIN student_grades sg
		ON sg.student_id = s.id
),
dt AS
(
	SELECT *,
			CASE WHEN grade_level = 9 THEN 'Freshman'
			WHEN grade_level = 10 THEN 'Sophomore'
			WHEN grade_level = 11 THEN 'Junior'
			WHEN grade_level = 12 THEN 'Senior'
			ELSE grade_level END AS designation
	FROM gt
),
ct AS
(
	SELECT *,
			CASE WHEN designation = 'Sophomore' THEN final_grade END AS so_tb,
            CASE WHEN designation = 'Freshman' THEN final_grade END AS fr_tb,
			CASE WHEN designation = 'Junior' THEN final_grade END AS jn_tb,
            CASE WHEN designation = 'Senior' THEN final_grade END AS sn_tb
	FROM dt            
)
SELECT department, 
		CEIL(AVG(so_tb)) AS Sophomore ,
        CEIL(AVG(fr_tb)) AS Freshman,
        CEIL(AVG(jn_tb)) AS Junior, 
        CEIL(AVG(sn_tb)) AS Senior
FROM ct
GROUP BY department;

-- Rolling Calculations
-- Create a pizza orders table
CREATE TABLE pizza_orders (
    order_id INT PRIMARY KEY,
    customer_name VARCHAR(50),
    order_date DATE,
    pizza_name VARCHAR(100),
    price DECIMAL(5, 2)
);

INSERT INTO pizza_orders (order_id, customer_name, order_date, pizza_name, price) VALUES
    (1, 'Jack', '2024-12-01', 'Pepperoni', 18.75),
    (2, 'Jack', '2024-12-02', 'Pepperoni', 18.75),
    (3, 'Jack', '2024-12-03', 'Pepperoni', 18.75),
    (4, 'Jack', '2024-12-04', 'Pepperoni', 18.75),
    (5, 'Jack', '2024-12-05', 'Spicy Italian', 22.75),
    (6, 'Jill', '2024-12-01', 'Five Cheese', 18.50),
    (7, 'Jill', '2024-12-03', 'Margherita', 19.50),
    (8, 'Jill', '2024-12-05', 'Garden Delight', 21.50),
    (9, 'Jill', '2024-12-05', 'Greek', 21.50),
    (10, 'Tom', '2024-12-02', 'Hawaiian', 19.50),
    (11, 'Tom', '2024-12-04', 'Chicken Pesto', 20.75),
    (12, 'Tom', '2024-12-05', 'Spicy Italian', 22.75),
    (13, 'Jerry', '2024-12-01', 'California Chicken', 21.75),
    (14, 'Jerry', '2024-12-02', 'Margherita', 19.50),
    (15, 'Jerry', '2024-12-04', 'Greek', 21.50);
    
-- View the table
SELECT * FROM pizza_orders;

SELECT customer_name, order_date, price
FROM pizza_orders;

SELECT customer_name, order_date, SUM(price) AS total_sales
FROM pizza_orders
GROUP BY customer_name, order_date;

SELECT customer_name, order_date, SUM(price) AS total_sales
FROM pizza_orders
GROUP BY customer_name, order_date WITH ROLLUP;

SELECT customer_name, order_date, COUNT(price) AS total_units
FROM pizza_orders
GROUP BY customer_name, order_date WITH ROLLUP;

SELECT order_date, price
FROM pizza_orders
ORDER BY order_date;

SELECT order_date, SUM(price) AS total_sales
FROM pizza_orders
GROUP BY order_date
ORDER BY order_date;
        
WITH ts AS
(	
	SELECT order_date, SUM(price) AS total_sales
FROM pizza_orders
GROUP BY order_date
ORDER BY order_date
)
SELECT order_date, total_sales,
		ROW_NUMBER() OVER(ORDER BY order_date) AS row_num
FROM ts;

WITH ts AS
(	
	SELECT order_date, SUM(price) AS total_sales
FROM pizza_orders
GROUP BY order_date
ORDER BY order_date
)
SELECT order_date, total_sales,
		SUM(total_sales) OVER(ORDER BY order_date) AS cumilative_sales
FROM ts;

WITH ts AS
(	
	SELECT order_date, SUM(price) AS total_sales
FROM pizza_orders
GROUP BY order_date
ORDER BY order_date
)
SELECT order_date, total_sales,
		SUM(total_sales) OVER(ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumilative_sales
FROM ts;

SELECT country, year, happiness_score
FROM happiness_scores
ORDER BY country, year;

SELECT country, year, happiness_score,
		ROW_NUMBER() OVER(PARTITION BY country ORDER BY year) AS row_num 
FROM happiness_scores
ORDER BY country, year;

SELECT country, year, happiness_score,
		AVG(happiness_score) OVER(PARTITION BY country ORDER BY year) AS avg_happiness_score 
FROM happiness_scores
ORDER BY country, year;

SELECT country, year, happiness_score,
		CEIL(AVG(happiness_score) OVER(PARTITION BY country ORDER BY year
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )) AS avg_happiness_score 
FROM happiness_scores
ORDER BY country, year;

SELECT country, year, happiness_score,
		ROUND(AVG(happiness_score) OVER(PARTITION BY country ORDER BY year
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW ),3) AS avg_happiness_score 
FROM happiness_scores
ORDER BY country, year;

-- Assignment
SELECT * FROM products;
SELECT * FROM orders;

with ct AS (
	SELECT YEAR(o.order_date) AS year, MONTH(o.order_date) AS month, (p.unit_price* o.units) AS total_sales
	FROM orders o
	INNER JOIN products p
	ON o.product_id = p.product_id
), mt AS
(
SELECT year as yr, month as mnth,
		SUM(total_sales) AS total_sales  
			
FROM ct
GROUP BY year, month
)
SELECT yr, mnth, total_sales,
SUM(total_sales) OVER(PARTITION BY yr ORDER BY mnth ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumilative_sum,
            AVG(total_sales) OVER(PARTITION BY yr ORDER BY mnth ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS six_month_ma
FROM mt
ORDER BY yr, mnth
            