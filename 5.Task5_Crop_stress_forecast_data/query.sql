INSERT INTO crop_stress_forecast_data_backup(crop_stress_forecast_data_id, crop_sown_data_id, village_water_available_data_id, model_date,crop_stage_id, cumm_water_supplied, optimal_water_req, minimal_water_req, owr_stress_factor_1,owr_stress_factor_2,owr_stress_factor_3, owr_stress_factor_4, mwr_stress_factor_1,mwr_stress_factor_2, mwr_stress_factor_3, mwr_stress_factor_4, owr_is_crop_under_stress,mwr_is_crop_under_stress, insert_ts, update_ts, deleted, user_session_id,pest_names, owr_next_7_days, mwr_next_7_days, water_available_next_7_days,last_n_days_computed_asm, curr_actual_etc, curr_potential_etc, cumm_actual_etc,cumm_potential_etc, estimated_yield, soil_water_critical_level, crop_sown_data_source)
SELECT crop_stress_forecast_data_id, crop_sown_data_id, village_water_available_data_id, model_date,crop_stage_id, cumm_water_supplied, optimal_water_req, minimal_water_req, owr_stress_factor_1,owr_stress_factor_2, owr_stress_factor_3, owr_stress_factor_4, mwr_stress_factor_1,mwr_stress_factor_2, mwr_stress_factor_3, mwr_stress_factor_4, owr_is_crop_under_stress,mwr_is_crop_under_stress, csfd.insert_ts, csfd.update_ts, csfd.deleted, csfd.user_session_id,pest_names, owr_next_7_days, mwr_next_7_days, water_available_next_7_days,last_n_days_computed_asm, curr_actual_etc, curr_potential_etc, cumm_actual_etc,cumm_potential_etc, estimated_yield, soil_water_critical_level, crop_sown_data_source 
FROM crop_stress_forecast_data csfd JOIN farm_data fd
ON csfd.crop_sown_data_id = fd.farm_data_id
WHERE (csfd.model_date<20170131) OR (csfd.model_date < 20170320-2 AND expected_harvest_date >= 20170320-2) OR (csfd.model_date < fd.expected_harvest_date AND fd.expected_harvest_date < 20170320-2);



INSERT INTO crop_stress_forecast_data_backup 
			(crop_stress_forecast_data_id, crop_sown_data_id, 
			village_water_available_data_id, model_date, crop_stage_id, cumm_water_supplied, 
			optimal_water_req, minimal_water_req, owr_stress_factor_1, owr_stress_factor_2, 
			owr_stress_factor_3, owr_stress_factor_4, mwr_stress_factor_1, 
			mwr_stress_factor_2, mwr_stress_factor_3, mwr_stress_factor_4, 
			owr_is_crop_under_stress, mwr_is_crop_under_stress, insert_ts, update_ts, 
			deleted, user_session_id, pest_names, owr_next_7_days, mwr_next_7_days, 
			water_available_next_7_days, last_n_days_computed_asm, curr_actual_etc, 
			curr_potential_etc, cumm_actual_etc, cumm_potential_etc, estimated_yield, 
			soil_water_critical_level, crop_sown_data_source)

			SELECT crop_stress_forecast_data_id, crop_sown_data_id, 
			village_water_available_data_id, model_date, crop_stage_id, cumm_water_supplied, 
			optimal_water_req, minimal_water_req, owr_stress_factor_1, owr_stress_factor_2, 
			owr_stress_factor_3, owr_stress_factor_4, mwr_stress_factor_1, 
			mwr_stress_factor_2, mwr_stress_factor_3, mwr_stress_factor_4, 
			owr_is_crop_under_stress, mwr_is_crop_under_stress, csfd.insert_ts, 
			csfd.update_ts, csfd.deleted, csfd.user_session_id, pest_names, 
			owr_next_7_days, mwr_next_7_days, water_available_next_7_days, 
			last_n_days_computed_asm, curr_actual_etc, curr_potential_etc, cumm_actual_etc, 
			cumm_potential_etc, estimated_yield, soil_water_critical_level, 
			crop_sown_data_source 
			FROM crop_stress_forecast_data csfd JOIN farm_data fd ON csfd.crop_sown_data_id = fd.farm_data_id 
			WHERE 
				(csfd.model_date <= 20170131 ) OR 
				(csfd.model_date < 20170320-2 AND expected_harvest_date >= 20170320-2) OR 
				(csfd.model_date < fd.expected_harvest_date AND fd.expected_harvest_date < 20170320-2 );


DELETE FROM crop_stress_forecast_data 
WHERE crop_stress_forecast_data_id IN
	(SELECT crop_stress_forecast_data_id 
	FROM (SELECT * FROM crop_stress_forecast_data) AS csfd JOIN farm_data fd ON 
		csfd.crop_sown_data_id = fd.farm_data_id 
		WHERE 
				(csfd.model_date <= 20170131 ) OR 
				(csfd.model_date < 20170320-2 AND expected_harvest_date >= 20170320-2) OR 
				(csfd.model_date < fd.expected_harvest_date AND fd.expected_harvest_date < 20170320-2 ));




create view temp_table as select fd.farm_data_id, fd.village_code, fd.mandal_code, fd.district_code, fd.farmer_name, fd.father_name, fd.aadhar_no, 
fd.mobile_no, fd.khata_no, fd.occupant_extent, fd.total_extent, fd.survey_no, fd.category_of_farmer, fd.crop_name, 
fd.area_sown, fd.sowing_date, fd.source_of_irrigation, fd.latitude, fd.longitude, fd.expected_harvest_date, 
fd.expected_harvest_year, fd.period_name, fd.monsoon_year, fd.crop_year, fd.insert_ts, fd.update_ts, fd.deleted, 
fd.user_session_id, cf.crop_stress_forecast_data_id, cf.crop_sown_data_id, 
cf.model_date, cf.crop_stage_id, cf.cumm_water_supplied, cf.optimal_water_req, cf.minimal_water_req, 
cf.owr_stress_factor_1, cf.owr_stress_factor_2, cf.owr_stress_factor_3, cf.owr_stress_factor_4, cf.mwr_stress_factor_1, 
cf.mwr_stress_factor_2, cf.mwr_stress_factor_3, cf.mwr_stress_factor_4, cf.owr_is_crop_under_stress, 
cf.mwr_is_crop_under_stress, cf.pest_names, cf.owr_next_7_days, 
cf.mwr_next_7_days, cf.water_available_next_7_days, cf.last_n_days_computed_asm, cf.curr_actual_etc, 
cf.curr_potential_etc, cf.cumm_actual_etc, cf.cumm_potential_etc, cf.estimated_yield, cf.soil_water_critical_level, 
cf.crop_sown_data_source from crop_stress_forecast_data cf join farm_data fd on cf.crop_sown_data_id=fd.farm_data_id;

create view backup_table as select * from crop_stress_forecast_data_backup;

#whatever deleted from crop_stress_forecast_data have been inserted into crop_stress_forecast_data_backup but around 9 lakh records are left
in crop_stress_forecast_data table;

#not all records been deleted from crop_stress_forecast_data
#along with the last known record all other records are present
select * from crop_stress_forecast_data where crop_sown_data_id=623681 order by model_date;

select * from crop_stress_forecast_data join farm_data 
on farm_data.farm_data_id=crop_stress_forecast_data.crop_stress_forecast_data_id 
where crop_sown_data_id=623681 order by model_date;
