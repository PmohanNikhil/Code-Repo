USE maven_advanced_sql;

SELECT * FROM students;

SELECT grade_level, AVG(gpa) AS avg_gpa 
FROM students 
WHERE school_lunch = 'Yes' 
GROUP BY  grade_level
HAVING AVG(gpa) < 3.3
ORDER BY grade_level;


SELECT COUNT(DISTINCT grade_level) 
FROM students;


SELECT MAX(gpa) - MIN(gpa) AS gpa_range
FROM students;

SELECT * 
FROM students
WHERE grade_level < 12 AND school_lunch = 'Yes';


SELECT * 
FROM students
WHERE grade_level IN (9,10,11);

SELECT * 
FROM students
WHERE email IS NULL;


SELECT * 
FROM students
WHERE email IS NOT NULL;


SELECT * 
FROM students
WHERE email LIKE '%.com';

SELECT * 
FROM students
WHERE email LIKE '%.edu';

SELECT * 
FROM students
ORDER BY gpa;

SELECT * 
FROM students
ORDER BY gpa DESC;

SELECT *
FROM students
LIMIT 5;

SELECT student_name, grade_level,
	CASE WHEN grade_level = 9 THEN 'Freshman'
	WHEN grade_level = 10 THEN 'Sophomore'
	WHEN grade_level = 11 THEN 'Junior'
	ELSE 'Senior'
	END AS student_class
FROM students;