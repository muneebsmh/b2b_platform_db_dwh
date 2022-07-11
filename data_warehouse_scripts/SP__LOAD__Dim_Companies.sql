USE `dwh_b2b_platform`;

DROP PROCEDURE IF EXISTS `sp__load_dim_companies`;

DELIMITER $$
CREATE PROCEDURE `sp__load_dim_companies`()
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
    
    SET @job_name := 'LOAD DIM_COMPANIES';
	INSERT INTO `etl_process_log` (job_name,log_date,etl_start_time,etl_end_time,status)
	VALUES (@job_name,CURRENT_DATE,CURRENT_TIMESTAMP,NULL,'Started');
	SET @job_id := (SELECT MAX(job_id) FROM `etl_process_log` where job_name=@job_name);

	SET @step_name := 'Truncating Dim Companies Staging Table';
	INSERT INTO `job_details_log` (step_name,job_id,job_name,log_date,step_start_time,step_end_time,status,rows_affected)
	VALUES (@step_name,@job_id,@job_name,CURRENT_DATE,CURRENT_TIMESTAMP,NULL,'Started',NULL);
	SET @step_id := (SELECT MAX(step_id) FROM `job_details_log` WHERE step_name=@step_name);

	DELETE FROM `dwh_b2b_platform`.`stg_dim_companies`;
	SET @rc := ROW_COUNT();
    
	UPDATE `job_details_log`
	SET step_end_time=CURRENT_TIMESTAMP,
		status='Completed',
		rows_affected=@rc
	WHERE step_id=@step_id;
	
	SET @step_name := 'Populating Dim Companies Staging Table';
	INSERT INTO `job_details_log` (step_name,job_id,job_name,log_date,step_start_time,step_end_time,status,rows_affected)
	VALUES (@step_name,@job_id,@job_name,CURRENT_DATE,CURRENT_TIMESTAMP,NULL,'Started',NULL);
	SET @step_id := (SELECT MAX(step_id) FROM `job_details_log` WHERE step_name=@step_name);
    
	INSERT INTO `dwh_b2b_platform`.`stg_dim_companies`
	(	
		cuit_number,
		name
	)
	SELECT 
		cuit_number,
		name
	FROM `b2b_platform`.`companies`
	;
	SET @rc := ROW_COUNT();
	UPDATE `job_details_log`
	SET step_end_time=CURRENT_TIMESTAMP,
		status='Completed',
		rows_affected=@rc
	WHERE step_id=@step_id;
    
	SET @step_name := 'Applying SCD-II on the Dim Companies Table';
    INSERT INTO `job_details_log` (step_name,job_id,job_name,log_date,step_start_time,step_end_time,status,rows_affected)
	VALUES (@step_name,@job_id,@job_name,CURRENT_DATE,CURRENT_TIMESTAMP,NULL,'Started',NULL);
	SET @step_id := (SELECT MAX(step_id) FROM `job_details_log` WHERE step_name=@step_name);
    
	UPDATE `dwh_b2b_platform`.`dim_companies` a
	INNER JOIN 
	(
		SELECT * FROM
		`dwh_b2b_platform`.`stg_dim_companies`
	) b
	ON a.cuit_number = b.cuit_number
	SET
		a.is_active=0,
		a.end_date=CURRENT_TIMESTAMP,
		a.updated_by=CURRENT_USER,
		a.updated_at=CURRENT_TIMESTAMP
	WHERE 
		a.is_active=1
		AND	(a.name<>b.name)
	;
	SET @rc := ROW_COUNT();
	UPDATE `job_details_log`
	SET step_end_time=CURRENT_TIMESTAMP,
		status='Completed',
		rows_affected=@rc
	WHERE step_id=@step_id;
	
	SET @step_name := 'Inserting New Records in the Dim Companies Table';
	INSERT INTO `job_details_log` (step_name,job_id,job_name,log_date,step_start_time,step_end_time,status,rows_affected)
	VALUES (@step_name,@job_id,@job_name,CURRENT_DATE,CURRENT_TIMESTAMP,NULL,'Started',NULL);
	SET @step_id := (SELECT MAX(step_id) FROM `job_details_log` WHERE step_name=@step_name);
    
	INSERT INTO `dwh_b2b_platform`.`dim_companies`
	(	
		cuit_number,
        name,
		created_by,
		is_active,
		start_date
	)
	SELECT 
		a.cuit_number,
        a.name,
		CURRENT_USER,
		1,
		CURRENT_DATE
	FROM `dwh_b2b_platform`.`stg_dim_companies` a
	INNER JOIN 
	(
		SELECT * FROM
		`dwh_b2b_platform`.`dim_companies`
		WHERE is_active=0
	) b
	ON a.cuit_number = b.cuit_number
	WHERE 
		a.name<>IFNULL(b.name,'')
	UNION
	SELECT 
		a.cuit_number,
        a.name,
		CURRENT_USER,
		1,
		'1900-01-01'
	FROM `dwh_b2b_platform`.`stg_dim_companies` a
	LEFT JOIN 
	(
		SELECT * FROM
		`dwh_b2b_platform`.`dim_companies`
		WHERE is_active=1
	) b
	ON a.cuit_number = b.cuit_number
	WHERE 
		a.name<>IFNULL(b.name,'')
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
