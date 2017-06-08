create index index_name on table_name;

create index index_name on table_name(column_name);

create unique index index_name on table_name(column_name);

#composite index
create index index_name on table_name(column1, column2);

drop index index_name;

select * from table with(index(index_name))






#Done
create index farm_index on farm_data(village_code);
create index village_index on village_water_available_data(village_code); 
create index crop_index on crop_stress_forecast_data(crop_sown_data_id);	

#to test
select count(*) from farm_data cs join crop_stress_forecast_data cf join village_water_available_data vw 
on cf.crop_sown_data_id=cs.farm_data_id and cs.village_code=vw.village_code and cs.deleted=0 and cs.expected_harvest_year >= 2014;

select count(*) from farm_data cs join crop_stress_forecast_data cf join village_water_available_data vw 
on cf.crop_sown_data_id=cs.farm_data_id and cs.village_code=vw.village_code and cs.deleted=0 and 
cs.expected_harvest_year >= 2017 and cs.expected_harvest_date >= 20170320 and cf.model_date = 20170320 and vw.model_date = 20170320;

select count(*) from farm_data cs join crop_stress_forecast_data cf join village_water_available_data vw 
on cf.crop_sown_data_id=cs.farm_data_id and cs.village_code=vw.village_code and cs.deleted=0 and 
cs.expected_harvest_year >= 2017 and cs.expected_harvest_date >= 20170320 and cf.model_date = 20170315 and vw.model_date = 20170315;



#default indexes
village_code 5
crop_name 4
source_of_irrigation 3
expected_harvest_year 2
deleted 1
time: 4 min 11.47

village_code 1
crop_name 5
source_of_irrigation 4
expected_harvest_year 2
deleted 3
time: 4 min 16.71

#created index using village_code in farm_data, village_water_available_data
village_water_available_data
#indexes
village_code 1
time: 4.63 sec


#########################log
mysql> select count(*) from farm_data cs join crop_stress_forecast_data cf join village_water_available_data vw on cf.crop_sown_data_id=cs.farm_data_id and cs.village_code=vw.village_code and cs.deleted=0 and cs.expected_harvest_year >= 2014;
^C^C -- query aborted
ERROR 1317 (70100): Query execution was interrupted
mysql> select count(*) from farm_data cs join crop_stress_forecast_data cf join village_water_available_data vw on cf.crop_sown_data_id=cs.farm_data_id and cs.village_code=vw.village_code and cs.deleted=0 and cs.expected_harvest_year >= 2014;
+----------+
| count(*) |
+----------+
|  3814089 |
+----------+
1 row in set (0.84 sec)