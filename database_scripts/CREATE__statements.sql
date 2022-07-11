SET FOREIGN_KEY_CHECKS=0;

DROP DATABASE IF EXISTS `b2b_platform`;

CREATE DATABASE `b2b_platform`;

USE `b2b_platform`;

DROP TABLE IF EXISTS `categories`;

CREATE TABLE `categories` (
    `category_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `category_name` VARCHAR(255) NOT NULL,
    PRIMARY KEY (`category_id`)
)  AUTO_INCREMENT=1;

DROP TABLE IF EXISTS `products`;

CREATE TABLE `products` (
    `product_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `product_name` VARCHAR(255) NOT NULL,
    `product_category` INT UNSIGNED NOT NULL,
    PRIMARY KEY (`product_id`),
    CONSTRAINT `FK_Products_ProductCategory` FOREIGN KEY (`product_category`)
        REFERENCES `categories` (`category_id`)
)  AUTO_INCREMENT=1;

DROP TABLE IF EXISTS `companies`;

CREATE TABLE `companies` (
    `cuit_number` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `name` VARCHAR(255),
    PRIMARY KEY (`cuit_number`)
)  AUTO_INCREMENT=1;

DROP TABLE IF EXISTS `customers`;

CREATE TABLE `customers` (
    `document_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `full_name` VARCHAR(255) NOT NULL,
    `date_of_birth` DATE NOT NULL,
    `phone` VARCHAR(100) UNIQUE DEFAULT NULL,
    `email` VARCHAR(255) UNIQUE DEFAULT NULL,
    `address` VARCHAR(255) DEFAULT NULL,
    `country` VARCHAR(100) DEFAULT NULL,
    `region` VARCHAR(50) DEFAULT NULL,
    `city` VARCHAR(255),
    PRIMARY KEY (`document_id`)
)  AUTO_INCREMENT=1;

DROP TABLE IF EXISTS `company_products`;

CREATE TABLE `company_products` (
    `company_cuit` INT UNSIGNED AUTO_INCREMENT,
    `product_id` INT UNSIGNED NOT NULL,
    `uom` VARCHAR(100),
    `stock_quantity` INT,
    `price` INT,
    PRIMARY KEY (`company_cuit` , `product_id`),
    CONSTRAINT `FK_CompanyProducts_CompanyCuit` FOREIGN KEY (`company_cuit`)
        REFERENCES `companies` (`cuit_number`),
    CONSTRAINT `FK_CompanyProducts_ProductId` FOREIGN KEY (`product_id`)
        REFERENCES `products` (`product_id`)
)  AUTO_INCREMENT=1;

DROP TABLE IF EXISTS `b2b_orders`;

CREATE TABLE `b2b_orders` (
    `order_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `company_id` INT UNSIGNED NOT NULL,
    `datetime_of_order` DATETIME DEFAULT CURRENT_TIMESTAMP,
    `total_order_value` BIGINT UNSIGNED DEFAULT NULL,
    PRIMARY KEY (`order_id`),
    CONSTRAINT `FK_B2bOrders_CompanyId` FOREIGN KEY (`company_id`)
        REFERENCES `companies` (`cuit_number`)
)  AUTO_INCREMENT=1;

DROP TABLE IF EXISTS `b2b_order_items`;

CREATE TABLE `b2b_order_items` (
    `order_item_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `order_id` INT UNSIGNED NOT NULL,
    `supplier_id` INT UNSIGNED NOT NULL,
    `product_id` INT UNSIGNED NOT NULL,
    `price` INT,
    `quantity` INT,
    `total_item_value` BIGINT DEFAULT NULL,
    PRIMARY KEY (`order_item_id`),
    CONSTRAINT `FK_B2bOrderItems_OrderId` FOREIGN KEY (`order_id`)
        REFERENCES `b2b_orders` (`order_id`),
    CONSTRAINT `FK_B2bOrderItems_SupplierId_ProductId` FOREIGN KEY (`supplier_id` , `product_id`)
        REFERENCES `company_products` (`company_cuit` , `product_id`)
)  AUTO_INCREMENT=1;

DROP TABLE IF EXISTS `b2c_orders`;

CREATE TABLE `b2c_orders` (
    `order_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `customer_id` INT UNSIGNED NOT NULL,
    `datetime_of_order` DATETIME DEFAULT CURRENT_TIMESTAMP,
    `total_order_value` BIGINT UNSIGNED DEFAULT NULL,
    PRIMARY KEY (`order_id`),
    CONSTRAINT `FK_B2cOrders_CustomerId` FOREIGN KEY (`customer_id`)
        REFERENCES `customers` (`document_id`)
)  AUTO_INCREMENT=1;

DROP TABLE IF EXISTS `b2c_order_items`;

CREATE TABLE `b2c_order_items` (
    `order_item_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `order_id` INT UNSIGNED NOT NULL,
    `company_id` INT UNSIGNED NOT NULL,
    `product_id` INT UNSIGNED NOT NULL,
    `price` INT,
    `quantity` INT,
    `total_item_value` BIGINT DEFAULT NULL,
    PRIMARY KEY (`order_item_id`),
    CONSTRAINT `FK_B2cOrderItems_OrderId` FOREIGN KEY (`order_id`)
        REFERENCES `b2c_orders` (`order_id`),
    CONSTRAINT `FK_B2cOrderItems_SupplierId_ProductId` FOREIGN KEY (`company_id` , `product_id`)
        REFERENCES `company_products` (`company_cuit` , `product_id`)
)  AUTO_INCREMENT=1;


delimiter ~
CREATE TRIGGER `calculate_total_b2b_item_amount` BEFORE INSERT ON `b2b_order_items`
FOR EACH ROW BEGIN
    set new.`total_item_value` = new.`price` * new.`quantity`;
END ~

CREATE TRIGGER `calculate_total_b2c_item_amount` BEFORE INSERT ON `b2c_order_items`
FOR EACH ROW BEGIN
    set new.`total_item_value` = new.`price` * new.`quantity`;
END ~

delimiter ;

SET FOREIGN_KEY_CHECKS=1;