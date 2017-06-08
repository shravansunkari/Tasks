use platfarm_data;

select * from crop_stress_forecast_data limit 10 \G; 
take farm_data_id from the result list

select village_code from farm_data where farm_data_id = 145314;
get village_code using farm_data_id

select * from crop_location where village_code = 27694 \G;
get district, mandal, village names by this query
            

#modified files
CleanUpDSPImpl
CropStressForecastDataDSP
CropStressDataService
CropStressApplicationController 2130
routes 328 line
CropStressApplicationController 2210,2224 removed nullpointer exception
CropStressDataModel.java
soilmoisture-stress.html

select * from farm_data where village_code = 28469 limit 10\G;
select * from crop_location where village_code = 27667 \G;

http://localhost:9000/api/crop/stress/district/WEST%20GODAVARI/mandal/PALACOLE/village/VARIDHANAM/crop/GROUNDNUT
http://localhost:9000/api/crop/stress/district/WEST%20GODAVARI/mandal/NIDAMARRU/village/CHANAMILLI/crop/PADDY

State : ANDHRA PRADESH District : PRAKASAM Mandal : CHANDRASEKHARA PURAM Village : AMBAVARAM 

Age of crops in days
Soil Moisture
Pest
ASM today
Rainfall next 7 days
Total water available
water required next 7 days
water deficit next 7 days

count
Pest
wrsIndex
EstimatedYield

public void setWRSIndex(double wRSIndex);
public void setEstimatedYield(double estimatedYield);
public void setPestNames(List<String> pestNames);
public void setPestAlertCount(int pestAlertCount);