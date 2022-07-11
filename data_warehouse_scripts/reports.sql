#### REPORT 1 
#### WHAT ARE THE MOST POPULAR USED DEVICES FOR B2B CLIENTS (TOP 5)

SELECT 
	device, 
    COUNT(*) AS used_count 
FROM dwh_b2b_platform.dwh_weblogs
GROUP BY device
ORDER BY COUNT(*) DESC
LIMIT 5
;

#### REPORT 2 
#### WHAT ARE THE MOST POPULAR PRODUCTS IN THE COUNTRY

SELECT 
	`product_id`,
    `product_name`,
    `number_of_times_purchased`,
    `quantity`
FROM
(
	SELECT 
		product_id,
        product_name,
		COUNT(*) AS `number_of_times_purchased`,
		SUM(quantity) AS `quantity` 
	FROM
	(	
		SELECT a.product_id,b.product_name,a.quantity
		FROM dwh_b2b_platform.fact_b2b_sales a
        INNER JOIN dwh_b2b_platform.dim_company_products b
        ON a.product_id=b.row_key
		UNION ALL
		SELECT a.product_id,b.product_name,a.quantity
		FROM dwh_b2b_platform.fact_b2c_sales a
        INNER JOIN dwh_b2b_platform.dim_company_products b
        ON a.product_id=b.row_key
	) a
	GROUP BY 
		product_id,
        product_name
) b
ORDER BY `number_of_times_purchased` DESC,
`quantity` DESC
;

#### REPORT 3 
#### ALL SALES OF B2B PLATFORM DISPLAYED MONTHLY FOR THE LAST YEAR

SELECT 
	CONCAT(`year`,'-',`month`) AS `year_month`,
    `sales`
FROM
(
	SELECT 
		YEAR(datetime_of_order) AS `year`,
		MONTH(datetime_of_order) AS `month`,
		SUM(total_order_value) AS `sales` 
	FROM
	(	
		SELECT datetime_of_order,total_order_value
		FROM dwh_b2b_platform.fact_b2b_sales
		WHERE datetime_of_order >= '2021-01-01'
		AND datetime_of_order < '2022-01-01'
		UNION ALL
		SELECT datetime_of_order,total_order_value
		FROM dwh_b2b_platform.fact_b2c_sales
		WHERE datetime_of_order >= '2021-01-01'
		AND datetime_of_order < '2022-01-01'
	) a
	GROUP BY 
		YEAR(datetime_of_order),
		MONTH(datetime_of_order)
	ORDER BY 
		YEAR(datetime_of_order),
    MONTH(datetime_of_order)
) b
;