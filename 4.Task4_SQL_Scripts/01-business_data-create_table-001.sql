use business_data;
CREATE TABLE water_supply_data(
  water_supply_data_id BIGINT(20) NOT NULL AUTO_INCREMENT,
  location_type_md_id int(11),
  location_name varchar(100),
  location_code varchar(45),
  event_gen_ts bigint(20),
  event_gen_day int(11),
  water_quantity double,
  water_quantity_mu int(2),
  supply_block_count double,
  supply_block_capacity double,
  supply_block_mu int(2),
  impacted_population int(11),
  source varchar(10),
  insert_ts bigint(20),
  user_session_id bigint(20),
  primary key(water_supply_data_id)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
