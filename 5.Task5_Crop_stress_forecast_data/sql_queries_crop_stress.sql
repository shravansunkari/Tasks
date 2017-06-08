cropstressforecastdatadsp
in that method
write dsp method to get the last known farm records under the villge




select *,max(cf.model_date) from crop_stress_forecast_data cf join farm_data fd on cf.crop_sown_data_id = fd.farm_data_id where fd.village_code=27950 group by fd.crop_name;



final query


select * from crop_stress_forecast_data cf join farm_data fd on cf.crop_sown_data_id = fd.farm_data_id where fd.village_code=27950 and fd.crop_name="Paddy" and cf.model_date<=20170215 order by cf.model_date desc limit 1;


select * from crop_stress_forecast_data cf join farm_data fd on cf.crop_sown_data_id = fd.farm_data_id where fd.village_code=27950 and fd.crop_name="Paddy" and cf.model_date<=20170215 group by fd.survey_no order by cf.model_date desc limit 1;

select * from crop_stress_forecast_data cf join farm_data fd on cf.crop_sown_data_id = fd.farm_data_id where fd.farm_data_id in (select farm_data_id from crop_stress_forecast_data cf join farm_data fd on cf.crop_sown_data_id = fd.farm_data_id where fd.village_code=27950 and fd.crop_name="Paddy" and cf.model_date<=20170215 group by fd.survey_no);


select fd.farm_data_id from farm_data fd where fd.farm_data_id in (select farm_data_id from crop_stress_forecast_data cf join farm_data fd on cf.crop_sown_data_id = fd.farm_data_id where fd.village_code=27950 and fd.crop_name="Paddy" and cf.model_date<=20170215);


select * from crop_stress_forecast_data cf join farm_data fd on cf.crop_sown_data_id = fd.farm_data_id 
where fd.village_code=27950 and fd.crop_name="Paddy" and cf.model_date<=20170215 and 
fd.farm_data_id in (select fd.farm_data_id order by model_date desc limit 1);


select * from crop_stress_forecast_data cf join farm_data fd on cf.crop_sown_data_id = fd.farm_data_id 
where fd.farm_data_id in (select distinct farm_data_id from crop_stress_forecast_data cf1 join farm_data fd1 
on cf1.crop_sown_data_id = fd1.farm_data_id 
where fd1.village_code=27950 and fd1.crop_name="Paddy" and cf1.model_date<=20170215 group by fd1.survey_no);



select village_code,crop_name,survey_no from farm_data where village_code=27950 and crop_name="Paddy" group by survey_no;


#max model_date for a specific farm
select fd.village_code,fd.crop_name,fd.survey_no,max(cf.model_date) from farm_data fd join crop_stress_forecast_data cf on cf.crop_sown_data_id = fd.farm_data_id where fd.village_code=27950 and fd.crop_name="Paddy" group by fd.survey_no;


#final query to get max model_date record for each farm under village
select fd.village_code,fd.crop_name,fd.survey_no,max(cf.model_date) from farm_data fd join crop_stress_forecast_data cf 
on cf.crop_sown_data_id = fd.farm_data_id where fd.village_code=30217 and fd.crop_name="Redgram" group by fd.survey_no;



select fd.village_code,fd.crop_name,fd.survey_no,cf.model_date from farm_data fd join crop_stress_forecast_data cf on cf.crop_sown_data_id = fd.farm_data_id where fd.village_code=30217 and fd.crop_name="Redgram" group by fd.survey_no;

select * from farm_data fd join crop_stress_forecast_data cf on cf.crop_sown_data_id = fd.farm_data_id where fd.farm_data_id 
in (select fd1.farm_data_id,max(cf1.model_date) from farm_data fd1 join crop_stress_forecast_data cf1 
	on cf1.crop_sown_data_id = fd1.farm_data_id 
	where fd1.village_code=30217 and fd1.crop_name="Redgram" group by fd1.survey_no);


#all records for each farm
select fd.village_code,fd.crop_name,fd.survey_no,cf.model_date from farm_data fd join crop_stress_forecast_data cf 
on cf.crop_sown_data_id = fd.farm_data_id where fd.farm_data_id  in 
(select fd1.farm_data_id from farm_data fd1 join crop_stress_forecast_data cf1  
	on cf1.crop_sown_data_id = fd1.farm_data_id  where fd1.village_code=30217 and fd1.crop_name="Redgram" group by fd1.survey_no);


select fd.village_code,fd.crop_name,fd.survey_no,cf.model_date from farm_data fd join crop_stress_forecast_data cf 
on cf.crop_sown_data_id = fd.farm_data_id where fd.farm_data_id  in 
(select fd1.farm_data_id from farm_data fd1 join crop_stress_forecast_data cf1  
	on cf1.crop_sown_data_id = fd1.farm_data_id  where fd1.village_code=30217 and fd1.crop_name="Redgram" and cf1.model_date 
	in (select max(cf2.model_date) from farm_data fd2 join crop_stress_forecast_data cf2  
	on cf2.crop_sown_data_id = fd2.farm_data_id  where fd2.village_code=30217 and fd2.crop_name="Redgram" group by fd2.survey_no));


#all records for each farm using view
select fd.village_code,fd.crop_name,fd.survey_no,cf.model_date from farm_data fd join crop_stress_forecast_data cf 
on cf.crop_sown_data_id = fd.farm_data_id where fd.farm_data_id  in 
(select farm_details.farm_data_id create view farm_details as select fd1.farm_data_id from farm_data fd1 join crop_stress_forecast_data cf1  
	on cf1.crop_sown_data_id = fd1.farm_data_id  where fd1.village_code=30217 and fd1.crop_name="Redgram" group by fd1.survey_no);




select fd.village_code,fd.crop_name,fd.survey_no,cf.model_date from farm_data fd join crop_stress_forecast_data cf 
on cf.crop_sown_data_id = fd.farm_data_id where fd.farm_data_id  in 
(select fd1.farm_data_id from farm_data fd1 join crop_stress_forecast_data cf1  
	on cf1.crop_sown_data_id = fd1.farm_data_id  where fd1.village_code=30217 and fd1.crop_name="Redgram" group by fd1.survey_no);


select fd.village_code,fd.crop_name,fd.survey_no,cf.model_date from farm_data fd join crop_stress_forecast_data cf 
on cf.crop_sown_data_id = fd.farm_data_id where fd.farm_data_id  in 
(select fd2.farm_data_id from farm_data fd2 join crop_stress_forecast_data cf2 on cf.crop_sown_data_id = fd.farm_data_id substring_index(group_concat(fd1.farm_data_id order by cf1.model_date desc),',',1) fd1.farm_data_id 
	from farm_data fd1 join crop_stress_forecast_data cf1 on cf1.crop_sown_data_id = fd1.farm_data_id 
	where fd1.village_code=30217 and fd1.crop_name="Redgram" group by fd1.survey_no);




select fd.village_code,fd.crop_name,fd.survey_no,cf.model_date from farm_data fd join crop_stress_forecast_data cf 
on cf.crop_sown_data_id = fd.farm_data_id where fd.farm_data_id  in 
(select fd1.farm_data_id from farm_data fd1 join crop_stress_forecast_data cf1  
	on cf1.crop_sown_data_id = fd1.farm_data_id and cf1.model_date<=20170318 where fd1.village_code=30217 and 
	fd1.crop_name="Redgram" group by fd1.survey_no);


#all records for each farm
select fd.village_code,fd.crop_name,fd.survey_no,cf.model_date from farm_data fd join crop_stress_forecast_data cf 
on cf.crop_sown_data_id = fd.farm_data_id where cf.model_date<=20170318 and fd.farm_data_id  
in (select fd1.farm_data_id from farm_data fd1 join crop_stress_forecast_data cf1 
on cf1.crop_sown_data_id = fd1.farm_data_id where fd1.village_code=30217 and fd1.crop_name="Redgram" group by fd1.survey_no);


select fd.village_code,fd.crop_name,fd.survey_no,cf.model_date from farm_data fd join crop_stress_forecast_data cf 
on cf.crop_sown_data_id = fd.farm_data_id where cf.model_date<=20170318 and fd.farm_data_id  
in (select fd1.farm_data_id from farm_data fd1 join crop_stress_forecast_data cf1 
	on cf1.crop_sown_data_id = fd1.farm_data_id where fd1.village_code=30217 and fd1.crop_name="Redgram" group by fd1.survey_no);

#working but returing first record for each farm
select fd.village_code,fd.crop_name,fd.survey_no,cf.model_date from farm_data fd join crop_stress_forecast_data cf 
on cf.crop_sown_data_id = fd.farm_data_id where cf.model_date<=20170318 and fd.farm_data_id in 
(select fd1.farm_data_id from farm_data fd1 join crop_stress_forecast_data cf1 on cf1.crop_sown_data_id = fd1.farm_data_id 
where fd1.village_code=30217 and fd1.crop_name="Redgram" group by fd1.survey_no order by cf1.model_date desc) group by fd.survey_no;


#zub
select max(model_date), crop_sown_data_id, village_code from crop_stress_forecast_data cs join farm_data fd 
on cs.crop_sown_data_id = fd.farm_data_id where fd.village_code = 30217 group by crop_sown_data_id order by crop_sown_data_id;

#final query working
select fd.village_code,fd.crop_name,fd.survey_no,max(cf.model_date) 
from farm_data fd join crop_stress_forecast_data cf  on cf.crop_sown_data_id = fd.farm_data_id 
where cf.model_date<=20170318 and fd.farm_data_id   in 
(select fd1.farm_data_id from farm_data fd1 join crop_stress_forecast_data cf1 
on cf1.crop_sown_data_id = fd1.farm_data_id where fd1.village_code=30217 and fd1.crop_name="Redgram" 
group by fd1.survey_no) group by fd.farm_data_id; 



sql.append("select *");
			sql.append(" from crop_stress_forecast_data cf join farm_data cs join village_water_available_data vw ");
			sql.append(" on cf.crop_sown_data_id = cs.farm_data_id and cf.village_water_available_data_id = vw.village_water_available_data_id where cs.village_code = vw.village_code ");
			sql.append(" where fd.village_code=? and fd.crop_name=? and cf.model_date<=? order by cf.model_date desc limit 1");	
			

select fd.village_code,fd.crop_name,fd.sowing_date,fd.survey_no,cf.model_date,max(cf.model_date) 
from crop_stress_forecast_data cf join farm_data cs join village_water_available_data vw   
on cf.crop_sown_data_id = cs.farm_data_id and cf.village_water_available_data_id = vw.village_water_available_data_id where cf.model_date<=20170318 and fd.farm_data_id in
(select fd1.farm_data_id from farm_data fd1 join crop_stress_forecast_data cf1  on cf1.crop_sown_data_id = fd1.farm_data_id where fd1.village_code=30217 and fd1.crop_name="Redgram" group by fd1.survey_no) group by fd.farm_data_id;


#final query
select fd.village_code,fd.crop_name,fd.sowing_date,fd.survey_no,cf.model_date,max(cf.model_date)  
from crop_stress_forecast_data cf join farm_data fd join village_water_available_data vw 
on cf.crop_sown_data_id = fd.farm_data_id and cf.village_water_available_data_id = vw.village_water_available_data_id 
where cf.model_date<=20170318 and fd.farm_data_id in 
(select fd1.farm_data_id from farm_data fd1 join crop_stress_forecast_data cf1  
	on cf1.crop_sown_data_id = fd1.farm_data_id where fd1.village_code=30217 and fd1.crop_name="Redgram" 
	group by fd1.survey_no) group by fd.farm_data_id;


 select cf.crop_sown_data_id as farm_id,max(cf.model_date) as final_date from crop_stress_forecast_data cf join farm_data fd join village_water_available_data vw  on cf.crop_sown_data_id = fd.farm_data_id and cf.village_water_available_data_id = vw.village_water_available_data_id where cf.model_date<=20170318 and fd.village_code=30217 and fd.crop_name="Redgram" group by cf.crop_sown_data_id;

##########################FINAL Query #################################################
select a.* 
from (select fd.farm_data_id, fd.village_code, fd.mandal_code, fd.district_code, fd.farmer_name, fd.father_name, fd.aadhar_no, 
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
cf.crop_sown_data_source, vw.village_water_available_data_id, vw.soil_moisture, 
vw.soil_moisture_date, vw.asm_in_mm_last_N_day, vw.curr_rainfall, vw.rf_forecast_1, vw.rf_forecast_2, vw.rf_forecast_3, 
vw.rf_forecast_4, vw.rf_forecast_5, vw.rf_forecast_6, vw.rf_forecast_7, vw.rf_last_N_days, vw.potential_evapotranspiration, 
vw.eto_last_N_days, vw.eto_forecast_N_days 
from crop_stress_forecast_data cf join farm_data fd join village_water_available_data vw  
on cf.crop_sown_data_id = fd.farm_data_id and cf.village_water_available_data_id = vw.village_water_available_data_id and fd.village_code = 27667
join 
(select cf1.crop_sown_data_id as farm_id, max(cf1.model_date) as final_date 
	from crop_stress_forecast_data cf1 
	join farm_data fd1 on cf1.crop_sown_data_id = fd1.farm_data_id 
	where cf1.model_date<=20170320 and fd1.village_code=27667 and fd1.crop_name="PADDY" group by cf1.crop_sown_data_id) farm_data
	on farm_data.farm_id=fd.farm_data_id and farm_data.final_date=cf.model_date) a;



fd.farm_data_id, fd.village_code, fd.mandal_code, fd.district_code, fd.farmer_name, fd.father_name, fd.aadhar_no, 
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
cf.crop_sown_data_source, vw.village_water_available_data_id, vw.soil_moisture, 
vw.soil_moisture_date, vw.asm_in_mm_last_N_day, vw.curr_rainfall, vw.rf_forecast_1, vw.rf_forecast_2, vw.rf_forecast_3, 
vw.rf_forecast_4, vw.rf_forecast_5, vw.rf_forecast_6, vw.rf_forecast_7, vw.rf_last_N_days, vw.potential_evapotranspiration, 
vw.eto_last_N_days, vw.eto_forecast_N_days
| village_code |
+--------------+
|        30421 |
|        30390 |
|        30389 |
|        30386 |
|        30381 |
|        30375 |
|        30371 |
|        30370 |
|        30369 |
|        30368 |
|        30367 |
|        30366 |
|        30365 |
|        30364 |
|        30363 |
|        30360 |
|        30359 |
|        30351 |
|        30350 |
|        30347 |
|        30346 |
|        30345 |
|        30344 |
|        30342 |
|        30341 |
|        30322 |
|        30321 |
|        30318 |
|        30316 |
|        30315 |
|        30313 |
|        30312 |
|        30311 |
|        30310 |
|        30309 |
|        30308 |
|        30307 |
|        30300 |
|        30299 |
|        30298 |
|        30297 |
|        30296 |
|        30295 |
|        30294 |
|        30293 |
|        30292 |
|        30291 |
|        30290 |
|        30288 |
|        30286 |
|        30279 |
|        30273 |
|        30248 |
|        30246 |
|        30245 |
|        30244 |
|        30243 |
|        30242 |
|        30241 |
|        30240 |
|        30239 |
|        30238 |
|        30237 |
|        30236 |
|        30235 |
|        30234 |
|        30232 |
|        30231 |
|        30230 |
|        30229 |
|        30228 |
|        30227 |
|        30226 |
|        30225 |
|        30223 |
|        30222 |
|        30221 |
|        30220 |
|        30219 |
|        30217 |
|        30216 |
|        30214 |
|        30213 |
|        30212 |
|        30210 |
|        30208 |
|        30207 |
|        30206 |
|        30205 |
|        30204 |
|        30203 |
|        29928 |
|        29908 |
|        29891 |
|        29890 |
|        29876 |
|        29875 |
|        29871 |
|        29849 |
|        29827