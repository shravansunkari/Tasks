select * from location_type_md; 

select display_name  
from location loc, location_type_map loctype 
where loc.location_id = loctype.location_id and location_type_md_id=1  
order by display_name;