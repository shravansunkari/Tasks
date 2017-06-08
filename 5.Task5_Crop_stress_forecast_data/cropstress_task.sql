INSERT into crop_stress_forecast_data_backup (

SELECT
crop_stress_forecast_data_id, crop_sown_data_id, village_water_available_data_id, model_date, crop_stage_id, cumm_water_supplied, optimal_water_req, minimal_water_req, owr_stress_factor_1, owr_stress_factor_2, owr_stress_factor_3, owr_stress_factor_4, mwr_stress_factor_1, mwr_stress_factor_2, mwr_stress_factor_3, mwr_stress_factor_4, owr_is_crop_under_stress, mwr_is_crop_under_stress, csfd.insert_ts, csfd.update_ts, csfd.deleted, csfd.user_session_id, pest_names, owr_next_7_days, mwr_next_7_days, water_available_next_7_days, last_n_days_computed_asm, curr_actual_etc, curr_potential_etc, cumm_actual_etc, cumm_potential_etc, estimated_yield, soil_water_critical_level, crop_sown_data_source

FROM crop_stress_forecast_data csfd JOIN farm_data fd
ON csfd.crop_sown_data_id = fd.farm_data_id
WHERE (
	(csfd.model_date<20170131) OR
	(csfd.model_date < 20170410-2 and expected_harvest_date >= 20170410-2) OR
	(csfd.model_date < fd.expected_harvest_date AND fd.expected_harvest_date < 20170410-2)
	) AND 
	NOT
	(
		csfd.model_date >= fd.expected_harvest_date AND fd.expected_harvest_date < 20170410-2
	)

);




DELETE FROM crop_stress_forecast_data 
WHERE crop_stress_forecast_data_id 
	IN
	(
		SELECT crop_stress_forecast_data_id
		FROM (SELECT * FROM crop_stress_forecast_data) AS csfd JOIN farm_data fd ON csfd.crop_sown_data_id = fd.farm_data_id
		WHERE 
				(csfd.model_date<20170131) OR
				(csfd.model_date < 20170410-2 and expected_harvest_date >= 20170410-2) OR
				(csfd.model_date < fd.expected_harvest_date AND fd.expected_harvest_date < 20170410-2)
	);



Create index on crop_sown_data_id in crop_stress_forecast_data table



delete test
   from test
  inner join (
     select max(id) as lastId, email
       from test
      group by email
     having count(*) > 1) duplic on duplic.email = test.email
  where test.id < duplic.lastId;




 delete from test
 where id not in (
    select max(id)
      from test
     group by email)




DELETE FROM crop_stress_forecast_data 
WHERE 
	(crop_stress_forecast_data_id not in (select max(crop_stress_forecast_data_id) 
		from crop_stress_forecast_data 
		group by crop_sown_data_id 
		where csfd.model_date >= fd.expected_harvest_date
	) 

	AND
	
	crop_stress_forecast_data_id IN
	(SELECT crop_stress_forecast_data_id
	FROM (SELECT * FROM crop_stress_forecast_data) AS csfd JOIN farm_data fd ON
	csfd.crop_sown_data_id = fd.farm_data_id
	WHERE (csfd.model_date<20170131) OR
	(csfd.model_date < 20170410-2 and expected_harvest_date >= 20170410-2) OR
	(csfd.model_date < fd.expected_harvest_date AND fd.expected_harvest_date < 20170410-2)
);




INSERT into crop_stress_forecast_data_backup(SELECT
crop_stress_forecast_data_id, crop_sown_data_id, village_water_available_data_id, model_date,
crop_stage_id, cumm_water_supplied, optimal_water_req, minimal_water_req, owr_stress_factor_1,
owr_stress_factor_2, owr_stress_factor_3, owr_stress_factor_4, mwr_stress_factor_1,
mwr_stress_factor_2, mwr_stress_factor_3, mwr_stress_factor_4, owr_is_crop_under_stress,
mwr_is_crop_under_stress, csfd.insert_ts, csfd.update_ts, csfd.deleted, csfd.user_session_id,
pest_names, owr_next_7_days, mwr_next_7_days, water_available_next_7_days,
last_n_days_computed_asm, curr_actual_etc, curr_potential_etc, cumm_actual_etc,
cumm_potential_etc, estimated_yield, soil_water_critical_level, crop_sown_data_source
FROM crop_stress_forecast_data csfd JOIN farm_data fd
ON csfd.crop_sown_data_id = fd.farm_data_id
WHERE (csfd.model_date<20170131) OR
(csfd.model_date < 20170410-2 and expected_harvest_date >= 20170410-2) OR
(csfd.model_date < fd.expected_harvest_date AND fd.expected_harvest_date < 20170410-2));