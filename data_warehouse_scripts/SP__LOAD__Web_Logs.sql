USE `dwh_b2b_platform`;

DROP PROCEDURE IF EXISTS `sp__load_web_logs`;

DELIMITER $$
CREATE PROCEDURE `sp__load_web_logs`()
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
    
    SET @job_name := 'LOAD DWH_WEBLOGS';
	INSERT INTO `etl_process_log` (job_name,log_date,etl_start_time,etl_end_time,status)
	VALUES (@job_name,CURRENT_DATE,CURRENT_TIMESTAMP,NULL,'Started');
	SET @job_id := (SELECT MAX(job_id) FROM `etl_process_log` where job_name=@job_name);
	
	SET @step_name := 'Truncating DWH Weblogs Staging Table';
	INSERT INTO `job_details_log` (step_name,job_id,job_name,log_date,step_start_time,step_end_time,status,rows_affected)
	VALUES (@step_name,@job_id,@job_name,CURRENT_DATE,CURRENT_TIMESTAMP,NULL,'Started',NULL);
	SET @step_id := (SELECT MAX(step_id) FROM `job_details_log` WHERE step_name=@step_name);

	DELETE FROM `dwh_b2b_platform`.`stg_dwh_weblogs`;
	SET @rc := ROW_COUNT();
    
	UPDATE `job_details_log`
	SET step_end_time=CURRENT_TIMESTAMP,
		status='Completed',
		rows_affected=@rc
	WHERE step_id=@step_id;
	
	SET @step_name := 'Populating DWH Weblogs Staging Table';
	INSERT INTO `job_details_log` (step_name,job_id,job_name,log_date,step_start_time,step_end_time,status,rows_affected)
	VALUES (@step_name,@job_id,@job_name,CURRENT_DATE,CURRENT_TIMESTAMP,NULL,'Started',NULL);
	SET @step_id := (SELECT MAX(step_id) FROM `job_details_log` WHERE step_name=@step_name);
    
	INSERT INTO `dwh_b2b_platform`.`stg_dwh_weblogs`(logs)
	SELECT logs
	FROM `b2b_platform`.`weblogs`;
    
    SET @rc := ROW_COUNT();
	UPDATE `job_details_log`
	SET step_end_time=CURRENT_TIMESTAMP,
		status='Completed',
		rows_affected=@rc
	WHERE step_id=@step_id;
    
	SET @step_name := 'Truncating DWH Weblogs Table';
	INSERT INTO `job_details_log` (step_name,job_id,job_name,log_date,step_start_time,step_end_time,status,rows_affected)
	VALUES (@step_name,@job_id,@job_name,CURRENT_DATE,CURRENT_TIMESTAMP,NULL,'Started',NULL);
	SET @step_id := (SELECT MAX(step_id) FROM `job_details_log` WHERE step_name=@step_name);

	DELETE FROM `dwh_b2b_platform`.`dwh_weblogs`;
	SET @rc := ROW_COUNT();
    
	UPDATE `job_details_log`
	SET step_end_time=CURRENT_TIMESTAMP,
		status='Completed',
		rows_affected=@rc
	WHERE step_id=@step_id;
	
	SET @step_name := 'Populating DWH Weblogs Table';
	INSERT INTO `job_details_log` (step_name,job_id,job_name,log_date,step_start_time,step_end_time,status,rows_affected)
	VALUES (@step_name,@job_id,@job_name,CURRENT_DATE,CURRENT_TIMESTAMP,NULL,'Started',NULL);
	SET @step_id := (SELECT MAX(step_id) FROM `job_details_log` WHERE step_name=@step_name);

	INSERT INTO `dwh_b2b_platform`.`dwh_weblogs`
    (
		client_ip,
        user_name,
        time,
        request,
        url,
        response,
        request_size,
        user_agent,
        device,
        logs
    )
	SELECT 
		client_ip,
		user_name,
		DATE_ADD(STR_TO_DATE(SUBSTRING_INDEX(a.time,' ',2),'%d/%b/%Y %T'),INTERVAL time_to_sec(STR_TO_DATE(SUBSTRING_INDEX(a.time,' ',-1),'+%h00')) SECOND) AS time,
		request,
		url,
		response,
		request_size,
		user_agent,
		device,
		logs
	FROM
	(
	SELECT
		TRIM(substring_index(logs,' ',1)) AS client_ip,
		TRIM(substring_index(substring_index(logs,'[',1),' ',-2)) AS user_name,
		TRIM(substring_index(substring_index(logs,']',1),'[',-1)) AS time,
		TRIM(substring_index(substring_index(logs,'"',2),'"',-1)) AS request,
		TRIM(substring_index(substring_index(logs,'"',4),'"',-1)) AS url,
		TRIM(substring_index(substring_index(logs,'"',-5),' ',2)) AS response,
		TRIM(substring_index(substring_index(logs,'"',3),' ',-2)) AS request_size,
		TRIM(substring_index(substring_index(logs,'"',6),'"',-1)) AS user_agent,
		TRIM(substring_index(substring_index(substring_index(substring_index(logs,'"',6),'"',-1),";",1),"(",-1)) AS device,
		logs
	FROM `dwh_b2b_platform`.`stg_dwh_weblogs`
	) a;
    
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
