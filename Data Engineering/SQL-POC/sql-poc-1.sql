SELECT dept, SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS active_count,
         SUM(CASE WHEN status = 'inactive' THEN 1 ELSE 0 END) AS inactive_count
FROM Employee
GROUP BY dept ;

SELECT employee_name, sales_amount, ROW_NUMBER() OVER( ORDER BY sales_amount DESC) AS row_num,
        RANK() OVER( ORDER BY sales_amount DESC) AS rank_num,
        DENSE_RANK() OVER( ORDER BY sales_amount DESC) as dense_rank
    FROM sales;

SELECT customer_id,MAX(order_date) as last_purchase, MIN(order_date) as first_purchase
FROM orders
GROUP BY customer_id;

MERGE INTO TARGET_TABLE AS T
USING SOURCE_TABLE AS S
ON T.id=S.id
WHEN MATCHED THEN
    UPDATE SET 
    T.name=S.name,
    T.value=S.value,
    T.last_updated=S.last_updated
WHEN NOT MATCHED THEN
    INSERT (id, value, name, last_updated)
    VALUES (S.id, S.value, S.name, S.last_updated);
