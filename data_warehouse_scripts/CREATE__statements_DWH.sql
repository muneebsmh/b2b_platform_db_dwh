SET FOREIGN_KEY_CHECKS=0;
SET @@cte_max_recursion_depth=100000;

DROP DATABASE IF EXISTS `dwh_b2b_platform`;

CREATE DATABASE `dwh_b2b_platform`; 

USE `dwh_b2b_platform`;

DROP TABLE IF EXISTS `dim_date`;
CREATE TABLE `dim_date` as 
WITH RECURSIVE `dim_date` AS (
	SELECT 
		'2000-01-01' AS `date` 
	UNION ALL 
	SELECT 
		DATE_ADD(`date`, INTERVAL 1 DAY) AS `date`
	FROM
		`dim_date`
	WHERE
		`date` < CURRENT_DATE
    )
SELECT 
    `date` AS date,
    DATE_FORMAT(`date`, '%Y') AS year,
    DATE_FORMAT(`date`, '%M') AS month,
    DATE_FORMAT(`date`, '%m') AS month_of_year,
    DATE_FORMAT(`date`, '%d') AS day_of_month,
    DATE_FORMAT(`date`, '%W') AS day,
    DAYOFWEEK(`date`) AS day_of_week,
    IF(DATE_FORMAT(`date`, '%W') IN ('Saturday' , 'Sunday'),
        'Weekend',
        'Weekday') AS weekend,
    DATE_FORMAT(`date`, '%j') AS day_of_year,
    DATE_FORMAT(`date`, '%V') AS week_of_year,
    QUARTER(`date`) AS quarter
FROM
    `dim_date`;

DROP TABLE IF EXISTS `stg_dim_companies`;

CREATE TABLE `stg_dim_companies` (
    `cuit_number` VARCHAR(255),
    `name` VARCHAR(255)
);

DROP TABLE IF EXISTS `dim_companies`;

CREATE TABLE `dim_companies` (
    `row_key` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `cuit_number` INT UNSIGNED NOT NULL,
    `name` VARCHAR(255),
    `start_date` DATE DEFAULT '1900-01-01',
    `end_date` DATE DEFAULT '9999-12-31',
    `is_active` TINYINT DEFAULT 0,
    `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
    `created_by` VARCHAR(250) DEFAULT NULL,
    `updated_at` DATETIME DEFAULT NULL,
    `updated_by` VARCHAR(250) DEFAULT NULL,
    PRIMARY KEY (`row_key`)
)  AUTO_INCREMENT=1;

DROP TABLE IF EXISTS `stg_dim_company_products`;

CREATE TABLE `stg_dim_company_products` (
    `company_cuit` VARCHAR(255),
    `product_id` VARCHAR(255),
    `product_name` VARCHAR(255),
    `product_category` VARCHAR(255),
    `uom` VARCHAR(255),
    `stock_quantity` VARCHAR(255),
    `price` VARCHAR(255)
);

DROP TABLE IF EXISTS `dim_company_products`;

CREATE TABLE `dim_company_products` (
    `row_key` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `company_cuit` INT UNSIGNED NOT NULL,
    `product_id` INT UNSIGNED,
    `product_name` VARCHAR(255) NOT NULL,
    `product_category` VARCHAR(255) NOT NULL,
    `uom` VARCHAR(100),
    `stock_quantity` INT,
    `price` INT,
    `start_date` DATE DEFAULT '1900-01-01',
    `end_date` DATE DEFAULT '9999-12-31',
    `is_active` TINYINT DEFAULT 0,
    `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
    `created_by` VARCHAR(250) DEFAULT NULL,
    `updated_at` DATETIME DEFAULT NULL,
    `updated_by` VARCHAR(250) DEFAULT NULL,
    PRIMARY KEY (`row_key`)
)  AUTO_INCREMENT=1;

DROP TABLE IF EXISTS `stg_dim_customers`;

CREATE TABLE `stg_dim_customers` (
    `document_id` VARCHAR(255),
    `full_name` VARCHAR(255),
    `date_of_birth` VARCHAR(255),
    `phone` VARCHAR(255),
    `email` VARCHAR(255),
    `address` VARCHAR(255),
    `country` VARCHAR(255),
    `region` VARCHAR(255),
    `city` VARCHAR(255)
);

DROP TABLE IF EXISTS `dim_customers`;

CREATE TABLE `dim_customers` (
    `row_key` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `document_id` INT UNSIGNED NOT NULL,
    `full_name` VARCHAR(255) NOT NULL,
    `date_of_birth` DATE NOT NULL,
    `phone` VARCHAR(100) DEFAULT NULL,
    `email` VARCHAR(255) DEFAULT NULL,
    `address` VARCHAR(255) DEFAULT NULL,
    `country` VARCHAR(100) DEFAULT NULL,
    `region` VARCHAR(50) DEFAULT NULL,
    `city` VARCHAR(255) DEFAULT NULL,
    `start_date` DATE DEFAULT '1900-01-01',
    `end_date` DATE DEFAULT '9999-12-31',
    `is_active` TINYINT DEFAULT 0,
    `created_at` DATETIME DEFAULT CURRENT_TIMESTAMP,
    `created_by` VARCHAR(250) DEFAULT NULL,
    `updated_at` DATETIME DEFAULT NULL,
    `updated_by` VARCHAR(250) DEFAULT NULL,
    PRIMARY KEY (`row_key`)
)  AUTO_INCREMENT=1;

DROP TABLE IF EXISTS `stg_fact_b2b_sales`;

CREATE TABLE `stg_fact_b2b_sales` (
    `order_id` VARCHAR(255),
    `order_item_id` VARCHAR(255),
    `company_id` VARCHAR(255),
    `datetime_of_order` VARCHAR(255),
    `total_order_value` VARCHAR(255),
    `supplier_id` VARCHAR(255),
    `product_id` VARCHAR(255),
    `price` VARCHAR(255),
    `quantity` VARCHAR(255),
    `total_item_value` VARCHAR(255)
);

DROP TABLE IF EXISTS `fact_b2b_sales`;

CREATE TABLE `fact_b2b_sales` (
    `row_key` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `order_id` INT UNSIGNED NOT NULL,
    `order_item_id` INT UNSIGNED NOT NULL,
    `company_id` INT UNSIGNED NOT NULL,
    `datetime_of_order` DATETIME DEFAULT CURRENT_TIMESTAMP,
    `total_order_value` BIGINT UNSIGNED DEFAULT NULL,
    `supplier_id` INT UNSIGNED NOT NULL,
    `product_id` INT UNSIGNED NOT NULL,
    `price` INT,
    `quantity` INT,
    `total_item_value` BIGINT DEFAULT NULL,
    PRIMARY KEY (`row_key`),
    CONSTRAINT `FK_FactB2bSales_CompanyId` FOREIGN KEY (`company_id`)
        REFERENCES `dim_companies` (`row_key`),
    CONSTRAINT `FK_FactB2bSales_ProductId` FOREIGN KEY (`product_id`)
        REFERENCES `dim_company_products` (`row_key`)
)  AUTO_INCREMENT=1;


DROP TABLE IF EXISTS `stg_fact_b2c_sales`;

CREATE TABLE `stg_fact_b2c_sales` (
    `order_id` VARCHAR(255),
    `order_item_id` VARCHAR(255),
    `customer_id` VARCHAR(255),
    `datetime_of_order` VARCHAR(255),
    `total_order_value` VARCHAR(255),
    `company_id` VARCHAR(255),
    `product_id` VARCHAR(255),
    `price` VARCHAR(255),
    `quantity` VARCHAR(255),
    `total_item_value` VARCHAR(255)
);

DROP TABLE IF EXISTS `fact_b2c_sales`;

CREATE TABLE `fact_b2c_sales` (
    `row_key` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `order_id` INT UNSIGNED NOT NULL,
    `order_item_id` INT UNSIGNED NOT NULL,
    `customer_id` INT UNSIGNED NOT NULL,
    `datetime_of_order` DATETIME DEFAULT CURRENT_TIMESTAMP,
    `total_order_value` BIGINT UNSIGNED DEFAULT NULL,
    `company_id` INT UNSIGNED NOT NULL,
    `product_id` INT UNSIGNED NOT NULL,
    `price` INT,
    `quantity` INT,
    `total_item_value` BIGINT DEFAULT NULL,
    PRIMARY KEY (`row_key`),
    CONSTRAINT `FK_FactB2cSales_CustomerId` FOREIGN KEY (`customer_id`)
        REFERENCES `dim_customers` (`row_key`),
    CONSTRAINT `FK_FactB2cSales_ProductId` FOREIGN KEY (`product_id`)
        REFERENCES `dim_company_products` (`row_key`)
)  AUTO_INCREMENT=1;

DROP TABLE IF EXISTS `stg_dwh_weblogs`;

CREATE TABLE `stg_dwh_weblogs` (
  `logs` text
);

DROP TABLE IF EXISTS `dwh_weblogs`;

CREATE TABLE `dwh_weblogs` (
	`row_key` INT UNSIGNED NOT NULL AUTO_INCREMENT,
	`client_ip` VARCHAR(255),
	`user_name` VARCHAR(100),
	`time` datetime,
	`request` VARCHAR(255),
	`url` VARCHAR(255),
	`response` BIGINT,
	`request_size` BIGINT,
	`user_agent` VARCHAR(255),
	`device` VARCHAR(100),
	`logs` TEXT,
    PRIMARY KEY (`row_key`)
)  AUTO_INCREMENT=1;

DROP TABLE IF EXISTS `etl_process_log`;

CREATE TABLE `etl_process_log`(
	`job_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `job_name` VARCHAR(255),
    `log_date` DATE,
    `etl_start_time` DATETIME,
    `etl_end_time` DATETIME,
    `status` VARCHAR(255),
    PRIMARY KEY (`job_id`)
)  AUTO_INCREMENT=1,
ENGINE=MyISAM;

DROP TABLE IF EXISTS `job_details_log`;

CREATE TABLE `job_details_log`(
	`step_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
	`step_name` VARCHAR(255),
    `job_id` INT UNSIGNED NOT NULL,
    `job_name` VARCHAR(255),
    `log_date` DATE,
    `step_start_time` DATETIME,
    `step_end_time` DATETIME,
    `status` VARCHAR(255),
    `rows_affected` INT,
    PRIMARY KEY (`step_id`),
    CONSTRAINT `FK_JobDetailsLog_JobId` FOREIGN KEY (`job_id`)
        REFERENCES `etl_process_log` (`job_id`)
)  AUTO_INCREMENT=1,
ENGINE=MyISAM;

DROP TABLE IF EXISTS `etl_exceptions`;

CREATE TABLE `etl_exceptions`(
	`exception_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `error_code` INT NOT NULL,
	`error_message` VARCHAR(255),
    `job_id` INT UNSIGNED NOT NULL,
    `job_name` VARCHAR(255),
    `step_id` INT UNSIGNED NOT NULL,
    `step_name` VARCHAR(255),
    `log_date` DATE,
    PRIMARY KEY (`exception_id`),
    CONSTRAINT `FK_EtlExceptions_JobId` FOREIGN KEY (`job_id`)
        REFERENCES `etl_process_log` (`job_id`),
    CONSTRAINT `FK_EtlExceptions_StepId` FOREIGN KEY (`step_id`)
        REFERENCES `job_details_log` (`step_id`)
)  AUTO_INCREMENT=1,
ENGINE=MyISAM;

SET FOREIGN_KEY_CHECKS=1;