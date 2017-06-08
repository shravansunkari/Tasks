-- phpMyAdmin SQL Dump
-- version 4.5.4.1deb2ubuntu2
-- http://www.phpmyadmin.net
--
-- Host: localhost
-- Generation Time: May 05, 2017 at 12:43 PM
-- Server version: 5.7.18-0ubuntu0.16.04.1
-- PHP Version: 7.0.15-0ubuntu0.16.04.4

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `business_data`
--

-- --------------------------------------------------------

--
-- Table structure for table `crop_stress_forecast_data_backup`
--

CREATE TABLE `crop_stress_forecast_data_backup` (
  `crop_stress_forecast_data_backup_id` bigint(20) NOT NULL,
  `crop_stress_forecast_data_id` bigint(20) NOT NULL,
  `crop_sown_data_id` bigint(20) NOT NULL,
  `village_water_available_data_id` bigint(20) NOT NULL,
  `model_date` int(11) NOT NULL,
  `crop_stage_id` smallint(5) NOT NULL,
  `cumm_water_supplied` double DEFAULT NULL,
  `optimal_water_req` double DEFAULT NULL,
  `minimal_water_req` double DEFAULT NULL,
  `owr_stress_factor_1` double DEFAULT NULL,
  `owr_stress_factor_2` double DEFAULT NULL,
  `owr_stress_factor_3` double DEFAULT NULL,
  `owr_stress_factor_4` double DEFAULT NULL,
  `mwr_stress_factor_1` double DEFAULT NULL,
  `mwr_stress_factor_2` double DEFAULT NULL,
  `mwr_stress_factor_3` double DEFAULT NULL,
  `mwr_stress_factor_4` double DEFAULT NULL,
  `owr_is_crop_under_stress` tinyint(4) NOT NULL DEFAULT '0',
  `mwr_is_crop_under_stress` tinyint(4) NOT NULL DEFAULT '0',
  `insert_ts` bigint(20) NOT NULL,
  `update_ts` bigint(20) DEFAULT NULL,
  `deleted` tinyint(4) NOT NULL DEFAULT '0',
  `user_session_id` bigint(20) NOT NULL,
  `pest_names` varchar(255) DEFAULT NULL,
  `owr_next_7_days` double DEFAULT NULL,
  `mwr_next_7_days` double DEFAULT NULL,
  `water_available_next_7_days` double DEFAULT NULL,
  `last_n_days_computed_asm` varchar(255) DEFAULT NULL,
  `curr_actual_etc` double DEFAULT '-99999.999',
  `curr_potential_etc` double DEFAULT '-99999.999',
  `cumm_actual_etc` double DEFAULT '-99999.999',
  `cumm_potential_etc` double DEFAULT '-99999.999',
  `estimated_yield` double DEFAULT '-99999.999',
  `soil_water_critical_level` double DEFAULT '-99999.999',
  `crop_sown_data_source` int(11) DEFAULT '0' COMMENT 'this field value 0 indicate the Farm_Data_id has placed in crop_sown_data field.1 Indicate crop_sown_data table id places in crop_sown_data_id'
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='It is a backup table';

--
-- Indexes for dumped tables
--

--
-- Indexes for table `crop_stress_forecast_data_backup`
--
ALTER TABLE `crop_stress_forecast_data_backup`
  ADD PRIMARY KEY (`crop_stress_forecast_data_backup_id`);

--
-- AUTO_INCREMENT for dumped tables
--

--
-- AUTO_INCREMENT for table `crop_stress_forecast_data_backup`
--
ALTER TABLE `crop_stress_forecast_data_backup`
  MODIFY `crop_stress_forecast_data_backup_id` bigint(20) NOT NULL AUTO_INCREMENT;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
