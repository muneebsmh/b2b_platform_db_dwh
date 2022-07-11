USE `dwh_b2b_platform`;

DROP PROCEDURE IF EXISTS `sp__load_dim_company_products`;

DELIMITER $$
CREATE PROCEDURE `sp__load_dim_company_products`()
BEGIN
	DECLARE err_no INT;
    DECLARE err_msg VARCHAR(1000);
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
		GET CURRENT DIAGNOSTICS CONDITION 1 err_no = MYSQL_ERRNO;
        GET CURRENT DIAGNOSTICS CONDITION 1 err_msg = MESSAGE_TEXT;
        ROLLBACK;
        
        UPDATE `job_details_log`
		SET step_end_time=CURRENT_TIMESTAMP,
			status='Failed - Rolling back changes',
			rows_affected=@rc
		WHERE step_id=@step_id;
		
		UPDATE `etl_process_log`
		SET etl_end_time=CURRENT_TIMESTAMP,
			status='Failed - Rolling back changes'
		WHERE job_id=@job_id;
        
        INSERT INTO `etl_exceptions` (error_code,error_message,job_id,job_name,step_id,step_name,log_date)
        VALUES (err_no,err_msg,@job_id,@job_name,@step_id,@step_name,CURRENT_DATE);
    END;
	
    SET autocommit = 0;
    START TRANSACTION;
    
    SET @job_name := 'LOAD DIM_COMPANY_PRODUCTS';
	INSERT INTO `etl_process_log` (job_name,log_date,etl_start_time,etl_end_time,status)
	VALUES (@job_name,CURRENT_DATE,CURRENT_TIMESTAMP,NULL,'Started');
	SET @job_id := (SELECT MAX(job_id) FROM `etl_process_log` where job_name=@job_name);

	SET @step_name := 'Truncating Dim Company Products Staging Table';
	INSERT INTO `job_details_log` (step_name,job_id,job_name,log_date,step_start_time,step_end_time,status,rows_affected)
	VALUES (@step_name,@job_id,@job_name,CURRENT_DATE,CURRENT_TIMESTAMP,NULL,'Started',NULL);
	SET @step_id := (SELECT MAX(step_id) FROM `job_details_log` WHERE step_name=@step_name);

	DELETE FROM `dwh_b2b_platform`.`stg_dim_company_products`;
	SET @rc := ROW_COUNT();
    
	UPDATE `job_details_log`
	SET step_end_time=CURRENT_TIMESTAMP,
		status='Completed',
		rows_affected=@rc
	WHERE step_id=@step_id;
	
	SET @step_name := 'Populating Dim Company Products Staging Table';
	INSERT INTO `job_details_log` (step_name,job_id,job_name,log_date,step_start_time,step_end_time,status,rows_affected)
	VALUES (@step_name,@job_id,@job_name,CURRENT_DATE,CURRENT_TIMESTAMP,NULL,'Started',NULL);
	SET @step_id := (SELECT MAX(step_id) FROM `job_details_log` WHERE step_name=@step_name);
    
	INSERT INTO `dwh_b2b_platform`.`stg_dim_company_products`
	(	
		company_cuit,
        product_id,
        product_name,
		product_category,
        uom,
        stock_quantity,
        price
	)
	SELECT 
		a.company_cuit,
		b.product_id,
        b.product_name,
        c.category_name as product_category,
        a.uom,
        a.stock_quantity,
        a.price
	FROM `b2b_platform`.`company_products` a
    INNER JOIN `b2b_platform`.`products` b
    ON a.product_id=b.product_id
    INNER JOIN `b2b_platform`.`categories` c
    ON b.product_category=c.category_id
	;
    
	SET @rc := ROW_COUNT();
	UPDATE `job_details_log`
	SET step_end_time=CURRENT_TIMESTAMP,
		status='Completed',
		rows_affected=@rc
	WHERE step_id=@step_id;
    
	SET @step_name := 'Applying SCD-II on the Dim Company Products Table';
    INSERT INTO `job_details_log` (step_name,job_id,job_name,log_date,step_start_time,step_end_time,status,rows_affected)
	VALUES (@step_name,@job_id,@job_name,CURRENT_DATE,CURRENT_TIMESTAMP,NULL,'Started',NULL);
	SET @step_id := (SELECT MAX(step_id) FROM `job_details_log` WHERE step_name=@step_name);
    
	UPDATE `dwh_b2b_platform`.`dim_company_products` a
	INNER JOIN 
	(
		SELECT * FROM
		`dwh_b2b_platform`.`stg_dim_company_products`
	) b
	ON a.company_cuit = b.company_cuit
    AND a.product_id = b.product_id
	SET
		a.is_active=0,
		a.end_date=CURRENT_TIMESTAMP,
		a.updated_by=CURRENT_USER,
		a.updated_at=CURRENT_TIMESTAMP
	WHERE 
		a.is_active=1
		AND	(a.product_name<>b.product_name
        OR a.product_category<>b.product_category
        OR a.uom<>b.uom
        OR a.stock_quantity<>b.stock_quantity
        OR a.price<>b.price)
	;
    
	SET @rc := ROW_COUNT();
	UPDATE `job_details_log`
	SET step_end_time=CURRENT_TIMESTAMP,
		status='Completed',
		rows_affected=@rc
	WHERE step_id=@step_id;
	
	SET @step_name := 'Inserting New Records in the Dim Company Products Table';
	INSERT INTO `job_details_log` (step_name,job_id,job_name,log_date,step_start_time,step_end_time,status,rows_affected)
	VALUES (@step_name,@job_id,@job_name,CURRENT_DATE,CURRENT_TIMESTAMP,NULL,'Started',NULL);
	SET @step_id := (SELECT MAX(step_id) FROM `job_details_log` WHERE step_name=@step_name);
    
	INSERT INTO `dwh_b2b_platform`.`dim_company_products`
	(	
		company_cuit,
        product_id,
        product_name,
        product_category,
        uom,
        stock_quantity,
        price,
		created_by,
		is_active,
		start_date
	)
	SELECT 
		a.company_cuit,
        a.product_id,
        a.product_name,
        a.product_category,
        a.uom,
        a.stock_quantity,
        a.price,
		CURRENT_USER,
		1,
		CURRENT_DATE
	FROM `dwh_b2b_platform`.`stg_dim_company_products` a
	INNER JOIN 
	(
		SELECT * FROM
		`dwh_b2b_platform`.`dim_company_products`
		WHERE is_active=0
	) b
	ON a.company_cuit = b.company_cuit
    AND a.product_id = b.product_id
	WHERE 
		a.product_name<>IFNULL(b.product_name,'')
        OR a.product_category<>IFNULL(b.product_category,'')
        OR a.uom<>IFNULL(b.uom,'')
        OR a.stock_quantity<>IFNULL(b.stock_quantity,'')
        OR a.price<>IFNULL(b.price,'')
	UNION
	SELECT 
		a.company_cuit,
        a.product_id,
        a.product_name,
        a.product_category,
        a.uom,
        a.stock_quantity,
        a.price,
		CURRENT_USER,
		1,
		'1900-01-01'
	FROM `dwh_b2b_platform`.`stg_dim_company_products` a
	LEFT JOIN 
	(
		SELECT * FROM
		`dwh_b2b_platform`.`dim_company_products`
		WHERE is_active=1
	) b
	ON a.company_cuit = b.company_cuit
    WHERE b.company_cuit IS NULL
	;
    
	SET @rc := ROW_COUNT();
	UPDATE `job_details_log`
	SET step_end_time=CURRENT_TIMESTAMP,
		status='Completed',
		rows_affected=@rc
	WHERE step_id=@step_id;

	UPDATE `etl_process_log`
	SET etl_end_time=CURRENT_TIMESTAMP,
		status='Completed'
	WHERE job_id=@job_id;

	COMMIT;
END$$
DELIMITER ;
