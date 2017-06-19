drop table business_data.soil_moisture_nrsc_data;

CREATE TABLE `business_data`.`soil_moisture_nrsc_data` (
  `soil_moisture_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `grid_id` bigint(20) NOT NULL,
  `nrsc_file_upload_id` bigint(20) NOT NULL,
  `model_date` int(11) NOT NULL,
  `evapotranspiration` double DEFAULT NULL,
  `runoff` double DEFAULT NULL,
  `soil_moisture_L1` double DEFAULT NULL,
  `soil_moisture_L2` double DEFAULT NULL,
  `soil_moisture_L3` double DEFAULT NULL,
  `is_forecasted_data` tinyint(4) NOT NULL DEFAULT '0',
  `insert_ts` bigint(20) NOT NULL,
  `update_ts` bigint(20) DEFAULT NULL,
  `deleted` tinyint(4) NOT NULL,
  `user_session_id` bigint(20) NOT NULL,
  PRIMARY KEY (`soil_moisture_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2392242 DEFAULT CHARSET=latin1;


ALTER TABLE `business_data`.`nrsc_file_upload`
ADD COLUMN `model_date` INT(11) NOT NULL AFTER `filename`,
ADD COLUMN `is_forecasted_data` TINYINT NOT NULL AFTER `model_date`,
ADD COLUMN `last_modified_ts` BIGINT(11) NOT NULL AFTER `is_forecasted_data`;