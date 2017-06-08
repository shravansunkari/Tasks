CREATE TABLE `platform_data`.`sm_village_grid_map` (
  `sm_village_grid_map_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `sm_village_full_name` varchar(200) NOT NULL,
  `grid_id` int(11) NOT NULL,
  `area` double DEFAULT NULL,
  `village_external_id` varchar(60) NOT NULL,
  `latitude` double NOT NULL,
  `longitude` double NOT NULL,
  `grid_source` varchar(45) NOT NULL,
  `insert_ts` bigint(20) NOT NULL,
  `update_ts` bigint(20) DEFAULT NULL,
  `deleted` tinyint(4) DEFAULT '0',
  PRIMARY KEY (`sm_village_grid_map_id`)
) ENGINE=InnoDB AUTO_INCREMENT=28261 DEFAULT CHARSET=latin1 COMMENT='contain map between isro grid id and soil moisture village name ';


ALTER TABLE `platform_data`.`sm_village_isro_grid_map` 
CHANGE COLUMN `sm_village_isro_grid_map_id` `sm_village_grid_map_id` BIGINT(20) NOT NULL AUTO_INCREMENT ;

ALTER TABLE `platform_data`.`sm_village_isro_grid_map` 
CHANGE COLUMN `isro_grid_id` `grid_id` INT(11) NOT NULL ;

ALTER TABLE `platform_data`.`sm_village_isro_grid_map` 
ADD COLUMN `grid_source` VARCHAR(45) NOT NULL AFTER `longitude`;

ALTER TABLE `platform_data`.`sm_village_isro_grid_map` 
RENAME TO  `platform_data`.`sm_village_grid_map` ;