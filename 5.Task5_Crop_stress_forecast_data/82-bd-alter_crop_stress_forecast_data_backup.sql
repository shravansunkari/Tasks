/**
 * SMCS-188, Yield Model Database design
 * Table to store yield related meta data
 */

ALTER TABLE `business_data`.`crop_stress_forecast_data_backup` 
ADD `curr_actual_etc` DOUBLE DEFAULT -99999.999 AFTER `last_n_days_computed_asm`,
ADD `curr_potential_etc` DOUBLE DEFAULT -99999.999 AFTER `curr_actual_etc`,
ADD `cumm_actual_etc` DOUBLE DEFAULT -99999.999 AFTER `curr_potential_etc`,
ADD `cumm_potential_etc` DOUBLE DEFAULT -99999.999 AFTER `cumm_actual_etc`,
ADD `estimated_yield` DOUBLE DEFAULT -99999.999 AFTER `cumm_potential_etc`,
ADD `soil_water_critical_level` DOUBLE DEFAULT -99999.999 AFTER `estimated_yield`;

