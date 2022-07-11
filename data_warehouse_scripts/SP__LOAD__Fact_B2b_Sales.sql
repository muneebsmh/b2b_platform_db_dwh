USE `dwh_b2b_platform`;

DROP PROCEDURE IF EXISTS `sp__load_fact_b2b_sales`;

DELIMITER $$
CREATE PROCEDURE `sp__load_fact_b2b_sales`()
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
    
    SET @job_name := 'LOAD FACT_B2B_SALES';
	INSERT INTO `etl_process_log` (job_name,log_date,etl_start_time,etl_end_time,status)
	VALUES (@job_name,CURRENT_DATE,CURRENT_TIMESTAMP,NULL,'Started');
	SET @job_id := (SELECT MAX(job_id) FROM `etl_process_log` where job_name=@job_name);
	
	SET @step_name := 'Truncating Fact B2B Sales Staging Table';
	INSERT INTO `job_details_log` (step_name,job_id,job_name,log_date,step_start_time,step_end_time,status,rows_affected)
	VALUES (@step_name,@job_id,@job_name,CURRENT_DATE,CURRENT_TIMESTAMP,NULL,'Started',NULL);
	SET @step_id := (SELECT MAX(step_id) FROM `job_details_log` WHERE step_name=@step_name);

	DELETE FROM `dwh_b2b_platform`.`stg_fact_b2b_sales`;
	SET @rc := ROW_COUNT();
    
	UPDATE `job_details_log`
	SET step_end_time=CURRENT_TIMESTAMP,
		status='Completed',
		rows_affected=@rc
	WHERE step_id=@step_id;
	
	SET @step_name := 'Populating Fact B2B Sales Staging Table';
	INSERT INTO `job_details_log` (step_name,job_id,job_name,log_date,step_start_time,step_end_time,status,rows_affected)
	VALUES (@step_name,@job_id,@job_name,CURRENT_DATE,CURRENT_TIMESTAMP,NULL,'Started',NULL);
	SET @step_id := (SELECT MAX(step_id) FROM `job_details_log` WHERE step_name=@step_name);
    
	INSERT INTO `dwh_b2b_platform`.`stg_fact_b2b_sales`
    (
		order_id,
        order_item_id,
        company_id,
        datetime_of_order,
        total_order_value,
        supplier_id,
        product_id,
        price,
        quantity,
        total_item_value
    )
    SELECT 
		a.order_id,
        b.order_item_id,
        a.company_id,
        a.datetime_of_order,
        a.total_order_value,
        b.supplier_id,
        b.product_id,
        b.price,
        b.quantity,
        b.total_item_value
	FROM `b2b_platform`.`b2b_orders` a
    INNER JOIN `b2b_platform`.`b2b_order_items` b
    ON a.order_id=b.order_id;
    
    SET @rc := ROW_COUNT();
	UPDATE `job_details_log`
	SET step_end_time=CURRENT_TIMESTAMP,
		status='Completed',
		rows_affected=@rc
	WHERE step_id=@step_id;
    
	SET @step_name := 'Truncating Fact B2B Sales Table';
	INSERT INTO `job_details_log` (step_name,job_id,job_name,log_date,step_start_time,step_end_time,status,rows_affected)
	VALUES (@step_name,@job_id,@job_name,CURRENT_DATE,CURRENT_TIMESTAMP,NULL,'Started',NULL);
	SET @step_id := (SELECT MAX(step_id) FROM `job_details_log` WHERE step_name=@step_name);

	DELETE FROM `dwh_b2b_platform`.`fact_b2b_sales`;
	SET @rc := ROW_COUNT();
    
	UPDATE `job_details_log`
	SET step_end_time=CURRENT_TIMESTAMP,
		status='Completed',
		rows_affected=@rc
	WHERE step_id=@step_id;
	
	SET @step_name := 'Populating Fact B2B Sales Table';
	INSERT INTO `job_details_log` (step_name,job_id,job_name,log_date,step_start_time,step_end_time,status,rows_affected)
	VALUES (@step_name,@job_id,@job_name,CURRENT_DATE,CURRENT_TIMESTAMP,NULL,'Started',NULL);
	SET @step_id := (SELECT MAX(step_id) FROM `job_details_log` WHERE step_name=@step_name);
    
	INSERT INTO `dwh_b2b_platform`.`fact_b2b_sales`
	(	
		order_id,
        order_item_id,
        company_id,
        datetime_of_order,
        total_order_value,
        supplier_id,
        product_id,
        price,
        quantity,
        total_item_value
	)
	SELECT 
		a.order_id,
        a.order_item_id,
        b.row_key as company_id,
        a.datetime_of_order,
        a.total_order_value,
        a.supplier_id,
        c.row_key as product_id,
        a.price,
        a.quantity,
        a.total_item_value
	FROM `dwh_b2b_platform`.`stg_fact_b2b_sales` a
    LEFT JOIN `dwh_b2b_platform`.`dim_companies` b
    ON a.company_id=b.cuit_number
    AND a.datetime_of_order>=b.start_date
    AND a.datetime_of_order<b.end_date
    LEFT JOIN `dwh_b2b_platform`.`dim_company_products` c
    ON a.supplier_id=c.company_cuit
    AND a.product_id=c.product_id
    AND a.datetime_of_order>=c.start_date
    AND a.datetime_of_order<c.end_date
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
