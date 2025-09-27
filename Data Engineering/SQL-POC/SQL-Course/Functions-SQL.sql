-- Functions by Data Type

USE maven_advanced_sql;

-- Function Basics
SELECT COUNT(*)
FROM  inflation_rates;

SELECT CURRENT_DATE();

SELECT COUNT(DISTINCT country)
FROM  country_stats;

SELECT UPPER( country)
FROM  country_stats;

SELECT ROUND(physicians_per_thousand,2), CEIL(physicians_per_thousand)
FROM country_stats;


-- Numeric Functions
SELECT *
FROM country_stats;

SELECT country, population,
		LOG(population) AS log_pop,
        ROUND(LOG(population), 2) AS rounded_log_pop
FROM country_stats;


WITH pm_tb AS
(
SELECT country, population,
		FLOOR(population/1000) AS pm
FROM country_stats
)
SELECT pm, COUNT(country) AS total_count
FROM pm_tb
GROUP BY pm
ORDER BY pm;


CREATE TABLE miles_run (
    name VARCHAR(50),
    q1 INT,
    q2 INT,
    q3 INT,
    q4 INT
);

INSERT INTO miles_run (name, q1, q2, q3, q4) VALUES
	('Ali', 100, 200, 150, NULL),
	('Bolt', 350, 400, 380, 300),
	('Jordan', 200, 250, 300, 320);

SELECT * FROM miles_run;

-- Greatest Value for each column

SELECT MAX(q1), MAX(q2), MAX(q3), MAX(q4)
FROM miles_run;

SELECT GREATEST(q1,q2,q3, COALESCE(q4,0)) AS most_miles
FROM miles_run;

-- CAST and Convert

-- Create a sample table
CREATE TABLE sample_table (
    id INT,
    str_value CHAR(50)
);

INSERT INTO sample_table (id, str_value) VALUES
	(1, '100.2'),
	(2, '200.4'),
	(3, '300.6');
    
SELECT * FROM sample_table;

SELECT id,
		ROUND(CAST(str_value AS FLOAT) * 2,2) AS float_val
	FROM sample_table;
    
SELECT id,
		ROUND(CAST(str_value AS DECIMAL(5,2)) * 2,2) AS float_val
	FROM sample_table;
    
-- Assignment

WITH j_tb AS 
(
SELECT o.customer_id, o.units,
		p.unit_price
FROM orders o
INNER JOIN products p
ON o.product_id = p.product_id
),
g_tb AS
(
SELECT customer_id, SUM((units * unit_price)) AS total_spend
FROM j_tb
GROUP BY customer_id
ORDER BY total_spend DESC
)
SELECT  FLOOR(total_spend/10) *10 AS total_spend_bin, COUNT(customer_id) AS num_customer
FROM g_tb
GROUP BY total_spend_bin
ORDER BY total_spend_bin ASC;

-- Datetime Function
SELECT CURRENT_DATE(), CURRENT_TIMESTAMP();


CREATE TABLE my_events (
    event_name VARCHAR(50),
    event_date DATE,
    event_datetime DATETIME,
    event_type VARCHAR(20),
    event_desc TEXT);

INSERT INTO my_events (event_name, event_date, event_datetime, event_type, event_desc) VALUES
('New Year\'s Day', '2025-01-01', '2025-01-01 00:00:00', 'Holiday', 'A global celebration to mark the beginning of the New Year. Festivities often include fireworks, parties, and various cultural traditions as people reflect on the past year and set resolutions for the upcoming one.'),
('Lunar New Year', '2025-01-29', '2025-01-29 10:00:00', 'Holiday', 'A significant cultural event in many Asian countries, the Lunar New Year, also known as the Spring Festival, involves family reunions, feasts, and various rituals to welcome good fortune and happiness for the year ahead.'),
('Persian New Year', '2025-03-20', '2025-03-20 12:00:00', 'Holiday', 'Known as Nowruz, this celebration marks the first day of spring and the beginning of the year in the Persian calendar. It is a time for family gatherings, traditional foods, and cultural rituals to symbolize renewal and rebirth.'),
('Birthday', '2025-05-13', '2025-05-13 18:00:00', ' Personal!', 'A personal celebration marking the anniversary of one\'s birth. This special day often involves gatherings with family and friends, cake, gifts, and reflecting on personal growth and achievements over the past year.'),
('Last Day of School', '2025-06-12', '2025-06-12 15:30:00', ' Personal!', 'The final day of the academic year, celebrated by students and teachers alike. It often includes parties, awards, and a sense of excitement for the upcoming summer break, marking the end of a year of hard work and learning.'),
('Vacation', '2025-08-01', '2025-08-01 08:00:00', ' Personal!', 'A much-anticipated break from daily routines, this vacation period allows individuals and families to relax, travel, and create memories. It is a time for adventure and exploration, often enjoyed with loved ones.'),
('First Day of School', '2025-08-18', '2025-08-18 08:30:00', ' Personal!', 'An exciting and sometimes nerve-wracking day for students, marking the beginning of a new academic year. This day typically involves meeting new teachers, reconnecting with friends, and setting goals for the year ahead.'),
('Halloween', '2025-10-31', '2025-10-31 18:00:00', 'Holiday', 'A festive occasion celebrated with costumes, trick-or-treating, and various spooky activities. Halloween is a time for fun and creativity, where people of all ages dress up and participate in themed events, parties, and community gatherings.'),
('Thanksgiving', '2025-11-27', '2025-11-27 12:00:00', 'Holiday', 'A holiday rooted in gratitude and family, Thanksgiving is celebrated with a large feast that typically includes turkey, stuffing, and various side dishes. It is a time to reflect on the blessings of the year and spend quality time with loved ones.'),
('Christmas', '2025-12-25', '2025-12-25 09:00:00', 'Holiday', 'A major holiday celebrated around the world, Christmas commemorates the birth of Jesus Christ. It is marked by traditions such as gift-giving, festive decorations, and family gatherings, creating a warm and joyous atmosphere during the holiday season.');

SELECT * FROM my_events;

SELECT event_name, event_type, event_date,
		DAY(event_date) AS event_day_num,
        MONTH(event_date) AS event_month,
        YEAR(event_date) AS event_year,
        DAYOFWEEK(event_date) AS event_day
FROM my_events;


WITH num_data_tb AS (
SELECT event_name, event_type, event_date,
		DAY(event_date) AS event_day_num,
        MONTH(event_date) AS event_month,
        YEAR(event_date) AS event_year,
        DAYOFWEEK(event_date) AS event_day
FROM my_events
)
SELECT event_name, event_type, event_date, event_month, event_year, event_day_num,
		CASE 
			WHEN event_day = 1 THEN 'Monday'
            WHEN event_day = 2 THEN 'Tuesday'
            WHEN event_day = 3 THEN 'Wednesday'
            WHEN event_day = 4 THEN 'Thursday'
            WHEN event_day = 5 THEN 'Friday'
            WHEN event_day = 6 THEN 'Saturday'
            WHEN event_day = 7 THEN 'Sunday'
		ELSE
			'Invalid Data'
		END
        AS event_day
FROM num_data_tb;

-- Calculate the interval for datetime values
SELECT event_name, event_date, CURRENT_DATE() AS date_now,
		(event_date-current_date()) AS diff
FROM my_events;

SELECT event_name, event_date, CURRENT_DATE() AS date_now,
		DATEDIFF(event_date, current_date()) AS diff_day
FROM my_events;

-- ADD/ Subract date

SELECT event_name, event_datetime,
		DATE_ADD(event_datetime ,  INTERVAL 1 HOUR) AS plus_one_hour,
        DATE_ADD(event_datetime ,  INTERVAL 1 Minute) AS plus_one_minute,
        DATE_ADD(event_datetime ,  INTERVAL 1 day) AS plus_one_day,
        DATE_ADD(event_datetime ,  INTERVAL 1 month) AS plus_one_month,
        DATE_ADD(event_datetime ,  INTERVAL 1 year) AS plus_one_year
FROM my_events;

-- Assignment
SelECT order_id, order_date FROM orders;

SelECT order_id, order_date,
		DATE_ADD(order_date, INTERVAL 2 DAY) AS ship_date
FROM orders
WHERE YEAR(order_date) = 2024 AND MONTH(order_date) BETWEEN 4 AND 6;

-- String Functions
SELECT * FROM my_events;

SELECT event_name, UPPER(event_name), LOWER(event_name) FROM my_events;

SELECT event_name, 
UPPER(event_name), 
LOWER(event_name),
TRIM(event_type) AS event_type_clean
FROM my_events;

SELECT event_name, 
UPPER(event_name), 
LOWER(event_name),
REPLACE(TRIM(event_type), '!', '') AS event_type_clean
FROM my_events;

SELECT event_name, 
UPPER(event_name), 
LOWER(event_name),
TRIM(REPLACE(event_type, '!', '')) AS event_type_clean
FROM my_events;


SELECT event_name, 
UPPER(event_name), 
LOWER(event_name),
TRIM(REPLACE(event_type, '!', '')) AS event_type_clean,
event_desc,
LENGTH(event_desc) AS leng
FROM my_events;


WITH my_table AS
(
	SELECT event_name, 
	UPPER(event_name), 
	LOWER(event_name),
	TRIM(REPLACE(event_type, '!', '')) AS event_type_clean,
	event_desc,
	LENGTH(event_desc) AS leng
	FROM my_events
)
SELECT event_name, event_type_clean, event_desc, leng,
	CONCAT(event_name, ':  ', event_desc) AS combined
FROM my_table;


-- Assignment

SELECT * FROM products;


WITH new_table AS 
(
SELECT factory, product_id,
		REPLACE(REPLACE(factory, ' ', '-'),"'","") AS new_factory_id
FROM products
)
SELECT factory, product_id,
		CONCAT(new_factory_id, '-', product_id) AS factory_product_id
FROM new_table
ORDER BY factory, product_id;


-- Pattern Matching
SELECT event_name,
	SUBSTR(event_name, 1,4)
    FROM my_events;

SELECT event_name,
	INSTR(event_name, ' ')
FROM my_events;

WITH my_tb AS 
(
SELECT event_name,
 SUBSTR(event_name, 1, INSTR(event_name, ' ') -1) AS se_word
 FROM my_events
 )
 SELECT event_name,
	CASE WHEN INSTR(event_name, ' ') = 0 THEN event_name
    ELSE se_word END
    AS first_word
FROM my_tb;

SELECT *
FROM my_events
WHERE event_desc LIKE '%family%';

SELECT *
FROM my_events
WHERE event_desc LIKE 'A %';

SELECT *
FROM students
WHERE student_name LIKE '___%';

SELECT event_desc,
		REGEXP_SUBSTR(event_desc, 'celebration|family|holiday') AS celebration_word
FROM my_events
WHERE event_desc LIKE '%celebration%'
	OR event_desc LIKE '%family%'
    OR event_desc LIKE '%holiday%';
    
SELECT event_desc,
		REGEXP_SUBSTR(event_desc, 'celebration|family|holiday') AS celebration_word
FROM my_events;

-- Assignment
SELECT product_name
FROM products
ORDER BY product_name;

SELECT product_name,
	REPLACE(product_name, 'Wonka Bar -' , '') AS new_product_name
FROM products
ORDER BY product_name;

SELECT product_name,
	CASE WHEN INSTR(product_name, '-')  = 0 THEN product_name
    ELSE SUBSTR(product_name,INSTR(product_name, '-') + 2)
    END AS new_product_name_1,
	REPLACE(product_name, 'Wonka Bar -' , '') AS new_product_name
FROM products
ORDER BY product_name;


-- Null Functions
CREATE TABLE contacts (
    name VARCHAR(50),
    email VARCHAR(100),
    alt_email VARCHAR(100));

INSERT INTO contacts (name, email, alt_email) VALUES
	('Anna', 'anna@example.com', NULL),
	('Bob', NULL, 'bob.alt@example.com'),
	('Charlie', NULL, NULL),
	('David', 'david@example.com', 'david.alt@example.com');

SELECT * FROM contacts;

SELECT email, alt_email,
		CASE WHEN email IS NULL THEN 'No Email'
        ELSE email END AS proc_e_mail,
        CASE WHEN alt_email IS NULL THEN 'No Email'
        ELSE alt_email END AS proc_alt_email
FROM contacts;

SELECT email, alt_email,
		IFNULL(email, alt_email) AS pro_email
FROM contacts;

SELECT email,alt_email,
		COALESCE(email, alt_email, 'No Mail') AS proc_email,
        COALESCE(alt_email, email, 'No Mail') AS proc_alt_email
FROM contacts;

-- Assignment
SELECT *
FROM products;

SELECT product_name, factory, division
FROM products
ORDER BY factory, division;

SELECT product_name, factory, division,
		COALESCE(division, 'Other') AS divison_other
FROM products
ORDER BY factory, division;

SELECT factory, division, COUNT(product_name) AS num_products
FROM products
WHERE division IS NOT NULL
GROUP BY factory, division;


-- Final Result
WITH num_products AS
(SELECT factory, division, COUNT(product_name) AS num_products
FROM products
WHERE division IS NOT NULL
GROUP BY factory, division
),
np_rank AS (
SELECT factory, division, num_products,
	ROW_NUMBER() OVER(PARTITION BY factory ORDER BY num_products DESC) AS np_rank
FROM num_products
),
top_div AS
(
SELECT factory, division FROM np_rank
WHERE np_rank=1
)
SELECT p.product_name, p.factory, p.division, td.division AS top_division,
		COALESCE(p.division, 'Other') AS divison_other,
        COALESCE(p.division, td.division, 'Other') AS divison_other_top
FROM products p
LEFT JOIN top_div td
ON p.factory = td.factory 
ORDER BY factory, division;
