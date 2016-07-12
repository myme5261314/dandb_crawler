create database  if not exists `duns2company`;
use duns2company;
DELIMITER //
drop procedure if exists `missing_tables` //
CREATE PROCEDURE missing_tables(n INT)
BEGIN
    WHILE n > 0
      DO
        SET @sql = CONCAT('create table if not exists duns_1M_', n, ' (`duns_id` INT UNIQUE NOT NULL,
  `company_name` VARCHAR(100) NOT NULL,
  `address` VARCHAR(200) NULL,
  `postal_code` VARCHAR(10) NULL,
  PRIMARY KEY (`duns_id`))');
        PREPARE cmd FROM @sql;
        EXECUTE cmd;
        DEALLOCATE PREPARE cmd;
        SET n = n - 1;
    END WHILE;
END; //
DELIMITER ;
CALL missing_tables(1000);