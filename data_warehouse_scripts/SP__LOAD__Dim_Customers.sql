USE `dwh_b2b_platform`;

DROP PROCEDURE IF EXISTS `sp__load_dim_customers`;

DELIMITER $$
CREATE PROCEDURE `sp__load_dim_customers`()
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
    
    SET @job_name := 'LOAD DIM_CUSTOMERS';
	INSERT INTO `etl_process_log` (job_name,log_date,etl_start_time,etl_end_time,status)
	VALUES (@job_name,CURRENT_DATE,CURRENT_TIMESTAMP,NULL,'Started');
	SET @job_id := (SELECT MAX(job_id) FROM `etl_process_log` WHERE job_name=@job_name);

	SET @step_name := 'Truncating Dim Customers Staging Table';
	INSERT INTO `job_details_log` (step_name,job_id,job_name,log_date,step_start_time,step_end_time,status,rows_affected)
	VALUES (@step_name,@job_id,@job_name,CURRENT_DATE,CURRENT_TIMESTAMP,NULL,'Started',NULL);
	SET @step_id := (SELECT MAX(step_id) FROM `job_details_log` WHERE step_name=@step_name);

	DELETE FROM `dwh_b2b_platform`.`stg_dim_customers`;
	SET @rc := ROW_COUNT();
    
	UPDATE `job_details_log`
	SET step_end_time=CURRENT_TIMESTAMP,
		status='Completed',
		rows_affected=@rc
	WHERE step_id=@step_id;
	
	SET @step_name := 'Populating Dim Customers Staging Table';
	INSERT INTO `job_details_log` (step_name,job_id,job_name,log_date,step_start_time,step_end_time,status,rows_affected)
	VALUES (@step_name,@job_id,@job_name,CURRENT_DATE,CURRENT_TIMESTAMP,NULL,'Started',NULL);
	SET @step_id := (SELECT MAX(step_id) FROM `job_details_log` WHERE step_name=@step_name);
    
	INSERT INTO `dwh_b2b_platform`.`stg_dim_customers`
	(	
		document_id,
		full_name,
		date_of_birth,
		phone,
		email,
		address,
		country,
		region,
		city
	)
	SELECT 
		document_id,
		full_name,
		date_of_birth,
		phone,
		email,
		address,
		country,
		region,
		city
	FROM `b2b_platform`.`customers`
	;
	SET @rc := ROW_COUNT();
	UPDATE `job_details_log`
	SET step_end_time=CURRENT_TIMESTAMP,
		status='Completed',
		rows_affected=@rc
	WHERE step_id=@step_id;
    
	SET @step_name := 'Applying SCD-II on the Dim Customers Table';
    INSERT INTO `job_details_log` (step_name,job_id,job_name,log_date,step_start_time,step_end_time,status,rows_affected)
	VALUES (@step_name,@job_id,@job_name,CURRENT_DATE,CURRENT_TIMESTAMP,NULL,'Started',NULL);
	SET @step_id := (SELECT MAX(step_id) FROM `job_details_log` WHERE step_name=@step_name);
    
	UPDATE `dwh_b2b_platform`.`dim_customers` a
	INNER JOIN 
	(
		SELECT * FROM
		`dwh_b2b_platform`.`stg_dim_customers`
	) b
	ON a.document_id = b.document_id
	SET
		a.is_active=0,
		a.end_date=CURRENT_TIMESTAMP,
		a.updated_by=CURRENT_USER,
		a.updated_at=CURRENT_TIMESTAMP
	WHERE 
		a.is_active=1
		AND	(a.full_name<>b.full_name
		OR a.date_of_birth<>b.date_of_birth
		OR a.phone<>b.phone
		OR a.email<>b.email
		OR a.address<>b.address
		OR a.country<>b.country
		OR a.region<>b.region
		OR a.city<>b.city)
	;
	SET @rc := ROW_COUNT();
	UPDATE `job_details_log`
	SET step_end_time=CURRENT_TIMESTAMP,
		status='Completed',
		rows_affected=@rc
	WHERE step_id=@step_id;
	
	SET @step_name := 'Inserting New Records in the Dim Customers Table';
	INSERT INTO `job_details_log` (step_name,job_id,job_name,log_date,step_start_time,step_end_time,status,rows_affected)
	VALUES (@step_name,@job_id,@job_name,CURRENT_DATE,CURRENT_TIMESTAMP,NULL,'Started',NULL);
	SET @step_id := (SELECT MAX(step_id) FROM `job_details_log` WHERE step_name=@step_name);
    
	INSERT INTO `dwh_b2b_platform`.`dim_customers`
	(	
		document_id,
		full_name,
		date_of_birth,
		phone,
		email,
		address,
		country,
		region,
		city,
		created_by,
		is_active,
		start_date
	)
	SELECT 
		a.document_id,
		a.full_name,
		a.date_of_birth,
		a.phone,
		a.email,
		a.address,
		a.country,
		a.region,
		a.city,
		CURRENT_USER,
		1,
		CURRENT_DATE
	FROM `dwh_b2b_platform`.`stg_dim_customers` a
	INNER JOIN 
	(
		SELECT * FROM
		`dwh_b2b_platform`.`dim_customers`
		WHERE is_active=0
	) b
	ON a.document_id = b.document_id
	WHERE 
		a.full_name<>IFNULL(b.full_name,'')
		OR a.date_of_birth<>IFNULL(b.date_of_birth,'')
		OR a.phone<>IFNULL(b.phone,'')
		OR a.email<>IFNULL(b.email,'')
		OR a.address<>IFNULL(b.address,'')
		OR a.country<>IFNULL(b.country,'')
		OR a.region<>IFNULL(b.region,'')
		OR a.city<>IFNULL(b.city,'')
	UNION
	SELECT 
		a.document_id,
		a.full_name,
		a.date_of_birth,
		a.phone,
		a.email,
		a.address,
		a.country,
		a.region,
		a.city,
		CURRENT_USER,
		1,
		'1900-01-01'
	FROM `dwh_b2b_platform`.`stg_dim_customers` a
	LEFT JOIN 
	(
		SELECT * FROM
		`dwh_b2b_platform`.`dim_customers`
		WHERE is_active=1
	) b
	ON a.document_id = b.document_id
	WHERE 
		a.full_name<>IFNULL(b.full_name,'')
		OR a.date_of_birth<>IFNULL(b.date_of_birth,'')
		OR a.phone<>IFNULL(b.phone,'')
		OR a.email<>IFNULL(b.email,'')
		OR a.address<>IFNULL(b.address,'')
		OR a.country<>IFNULL(b.country,'')
		OR a.region<>IFNULL(b.region,'')
		OR a.city<>IFNULL(b.city,'')
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
