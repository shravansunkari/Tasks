package com.vassarlabs.iwm.rainfall.forecast.service;

import static com.vassarlabs.iwm.utils.IWMConstants.IS_DEBUG_ENABLED;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.vassarlabs.common.dsp.err.DSPException;
import com.vassarlabs.common.dsp.utils.DSPConstants;
import com.vassarlabs.common.utils.DateUtils;
import com.vassarlabs.common.utils.err.ObjectNotFoundException;
import com.vassarlabs.iwm.rainfall.forecast.dsp.api.IRainfallForecastDSP;
import com.vassarlabs.iwm.rainfall.forecast.pojo.api.IRainfallForecast;
import com.vassarlabs.iwm.rainfall.forecast.pojo.impl.RainfallForecast;
import com.vassarlabs.iwm.rainfall.forecast.service.api.IRainfallForecastService;
import com.vassarlabs.iwm.rainfall.pojo.api.IRFStats;
import com.vassarlabs.iwm.rainfall.utils.BasinAssociation;
import com.vassarlabs.iwm.rainfall.utils.MandalArea;
import com.vassarlabs.iwm.rainfall.utils.RFStats;
import com.vassarlabs.location.service.api.ILocationHierarchyService;
import com.vassarlabs.location.utils.LocationConstants;

@Component
public class RainfallForecastService
	implements IRainfallForecastService {

	@Autowired
	protected IRainfallForecastDSP rainfallForecastDSP;
	
	@Autowired
	protected ILocationHierarchyService locService;
	
	@Override
	public List<IRainfallForecast> getAllRainfallForecast(long forecastDay)
		throws DSPException {
		return rainfallForecastDSP.getAllRainfallForecast(forecastDay);
	}

	@Override
	public IRainfallForecast getRainfallForecast(String locationUUID,
			long forecastDay) throws DSPException {
		return rainfallForecastDSP.getRainfallForecast(locationUUID, forecastDay);
	}

	@Override
	public IRainfallForecast insertOrUpdateRainfallForecast(
			IRainfallForecast rainfallForecast)
		throws DSPException, ObjectNotFoundException {
		return rainfallForecastDSP.insertOrUpdateRainfallForecast(rainfallForecast);
	}

	@Override
	public long getLastKnownForecastDate() throws DSPException {
		return rainfallForecastDSP.getLastKnownForecastDate();
	}
	
	
	/**
	 * Logic used here is take all the mandals group by basin wise and compute their avg value,max value,tmc value
	 */
	@Override
	public Map<String, Map<String, List<IRFStats>>> getBasinLevelRFForecastData(
			long foreCastDay) throws DSPException {
		
		long currentForecastDay = foreCastDay;
		long nextForecastDay = Long.valueOf(DateUtils.getNextDayInFormat("yyyyMMdd", String.valueOf(foreCastDay)));
		
		Map<String, Map<String, List<IRFStats>>> basinReportMap = new HashMap<String, Map<String,List<IRFStats>>>();
		//TODO: Handle duplicate mandal names
		Map<String, List<String>> basinMandalMap = BasinAssociation.BASIN_MANDAL_ASSOCIATION;
		
		List<String> locNameList = new ArrayList<String>();
		
		for (String basinName : basinMandalMap.keySet()) {
			locNameList.addAll(basinMandalMap.get(basinName));
		}
		
		List<String> basinMandalList = new ArrayList<String>();
		basinMandalList = locService.getLocationUUIDsFromNames(locNameList);
		List<IRainfallForecast> rfForecastList = rainfallForecastDSP.getAllRainfallForecast(currentForecastDay);
		
		for (IRainfallForecast rfForecastData : rfForecastList) {		
			
			String locUUID = rfForecastData.getLocationUUID();
			if(!basinMandalList.contains(locUUID)){
				continue;
			}
			
			String mandalName = null;
			try {
				mandalName = locService.getLocationNameFromUUID(locUUID);
			} catch (ObjectNotFoundException e) {
				System.out.println("Object Not Found exception while gettinf location name for" + locUUID);
				e.printStackTrace();
			}
			if(null == mandalName){
				System.out.println("mandal name not found for mandal uuid" + locUUID);
				continue;
			}
			String basinName = BasinAssociation.MANDAL_BASIN_ASSOCIATION.get(mandalName);
			if(null == basinName){
				System.out.println("basin name not found for mandal" + mandalName);
				continue;
			}
			String districtName = BasinAssociation.MANDAL_DISTRICT_ASSOCIATION.get(mandalName);
			if(null == districtName){
				System.out.println("district name not found for mandal" + mandalName);
				continue;
			}
			
			if(null != basinReportMap.get(basinName)){
				Map<String, List<IRFStats>> distForecast = basinReportMap.get(basinName);
				
				if(null != distForecast.get(districtName)){
					List<IRFStats> rfStatsList = distForecast.get(districtName);
					
					for (IRFStats rfStats : rfStatsList) {
						if(rfStats.getForeCastDay() == currentForecastDay){	
							updateRFStats(rfForecastData.getDay1(), rfStats, mandalName);
						}else if(rfStats.getForeCastDay() == nextForecastDay){
							updateRFStats(rfForecastData.getDay2(), rfStats, mandalName);
						}
					}
				}else{
					Map<String, List<IRFStats>> newDistForeCast = new HashMap<String, List<IRFStats>>();
					IRFStats todayStats =	createRFStats(currentForecastDay, rfForecastData.getDay1(), mandalName);
					IRFStats tommStats =	createRFStats(nextForecastDay, rfForecastData.getDay2(), mandalName);
					
					List<IRFStats> rfSTatsList = new ArrayList<IRFStats>();
					rfSTatsList.add(todayStats);
					rfSTatsList.add(tommStats);
					newDistForeCast.put(districtName, rfSTatsList);
					
					basinReportMap.get(basinName).putAll(newDistForeCast);
				}		
			}else{
				IRFStats todayStats =	createRFStats(currentForecastDay, rfForecastData.getDay1(), mandalName);
				IRFStats tommStats =	createRFStats(nextForecastDay, rfForecastData.getDay2(), mandalName);
				
				List<IRFStats> rfSTatsList = new ArrayList<IRFStats>();
				rfSTatsList.add(todayStats);
				rfSTatsList.add(tommStats);
				Map<String, List<IRFStats>> distForecast = new HashMap<String, List<IRFStats>>();
				distForecast.put(districtName, rfSTatsList);
				basinReportMap.put(basinName, distForecast);
			}
			
		}
		

		return basinReportMap;
	}

	private IRFStats createRFStats(long foreCastDay,
			double foreCastedData, String mandalName) {
		
		IRFStats rfStats = new RFStats();
		
		rfStats.setAvgValue(foreCastedData);
		rfStats.setMaxValue(foreCastedData);
		
		double tmcValue = getTMCFromMM(foreCastedData, mandalName);
		rfStats.setTmcValue(tmcValue);
		
		rfStats.setSumValue(foreCastedData);
		rfStats.setStationsCount(1);
		rfStats.setForeCastDay(foreCastDay);
		
		return rfStats;
	}

	private void updateRFStats(double foreCastedValue, IRFStats rfStats, String mandalName) {
		double cMaxValue = rfStats.getMaxValue();
		double cTMCValue = rfStats.getTmcValue();
		double cSumValue = rfStats.getSumValue();
		
		int newCount = rfStats.getStationsCount() + 1;
		rfStats.setStationsCount(newCount);
		
		if(cMaxValue <= foreCastedValue){
			rfStats.setMaxValue(foreCastedValue);
		}
		
		double tmcValue = getTMCFromMM(foreCastedValue, mandalName);
		
		double newTMCValue = cTMCValue + tmcValue;
		rfStats.setTmcValue(newTMCValue);
		
		double newSumValue = cSumValue + foreCastedValue;
		rfStats.setSumValue(newSumValue);
		
		double newAvgValue = (newSumValue / newCount);
		rfStats.setAvgValue(newAvgValue);
	}

	private double getTMCFromMM(double mmValue,
			String mandalName) {
		if(null != MandalArea.BASINS_MANDAL_AREA.get(mandalName)){
			return Double.valueOf(mmValue) * 0.000001 * MandalArea.BASINS_MANDAL_AREA.get(mandalName) * 35.314666721 * 11886.211;	
		}else{
			
			System.out.println("Mandal area not found for: " + mandalName);
			return Double.valueOf(mmValue) * 0.000001 * 1 * 35.314666721 * 11886.211;
		}
	}

	@Override
	public Map<String, Map<Long, Double>> getMandalWQData(long foreCastDay)
			throws DSPException {
		Map<String, Map<Long, Double>> mandalData = new HashMap<String, Map<Long, Double>>();

		List<IRainfallForecast> rfForecastList = rainfallForecastDSP
				.getAllRainfallForecast(foreCastDay);

		// TODO: Handle duplicate mandal names
		Map<String, List<String>> basinMandalMap = BasinAssociation.BASIN_MANDAL_ASSOCIATION;

		List<String> locNameList = new ArrayList<String>();

		for (String basinName : basinMandalMap.keySet()) {
			locNameList.addAll(basinMandalMap.get(basinName));
		}

		List<String> basinMandalList = new ArrayList<String>();
		basinMandalList = locService.getLocationUUIDsFromNames(locNameList);
		for (IRainfallForecast iRainfallForecast : rfForecastList) {
			String locUUID = iRainfallForecast.getLocationUUID();

			if (!basinMandalList.contains(locUUID)) {
				continue;
			}
			Map<Long, Double> wqData = getWQFromForecast(iRainfallForecast, foreCastDay);

			mandalData.put(locUUID, wqData);
		}

		return mandalData;
	}

	private Map<Long, Double> getWQFromForecast(
			IRainfallForecast iRainfallForecast, long forecastDay) {
		Map<Long, Double> wqData = new HashMap<Long, Double>();
		
		long nextForecastDay = Long.valueOf(DateUtils.getNextDayInFormat("yyyyMMdd", String.valueOf(forecastDay)));
		wqData.put(forecastDay, iRainfallForecast.getDay1());
		wqData.put(nextForecastDay, iRainfallForecast.getDay2());
		return wqData;
	}

	@Override
	public Map<String, List<Double>> getMandalLevelForecastData(String basinName, long forecastDay, int noOfForecastValues) {
		
		Map<String, List<Double>> finalDataMap = new HashMap<String, List<Double>>();
		List<String> locNameList = BasinAssociation.BASIN_MANDAL_ASSOCIATION.get(basinName);
		Map<String, Map<String, Double>> mandalNameToForecastDataMap = new HashMap<>();
		List<String> locUUIDList = null;
		try {
			locUUIDList = locService.getLocationUUIDsFromNames(locNameList);
		} catch (DSPException e) {
			System.out.println("RainfallForecastService :: Unable to get LocUUIDs from names. returning..");
			e.printStackTrace();
			return finalDataMap;
		}
		List<IRainfallForecast> mandalUUIDToForecastData = null;
		try {
			mandalUUIDToForecastData = rainfallForecastDSP.getAllLastKnownRainfallForecast(forecastDay);
		} catch (DSPException e) {
			System.out.println("RainfallForecastService :: Unable to get rainfall forecast data. returning..");
			e.printStackTrace();
			return finalDataMap;
		}
		String dayPrefix = "RFF";
		for(IRainfallForecast rainfallForecast : mandalUUIDToForecastData){
			if(!locUUIDList.contains(rainfallForecast.getLocationUUID())) continue;
			String locName = null;
			try {
				locName = locService.getLocationNameFromUUID(rainfallForecast.getLocationUUID());
			} catch (ObjectNotFoundException | DSPException e) {
				System.out.println("RainfallForecastService :: Unable to get name from locUUID. Skipping.." + rainfallForecast);
				e.printStackTrace();
				continue;
			}
			List<Double> valueList = new ArrayList<>();
			valueList.add(rainfallForecast.getDay1());
			valueList.add(rainfallForecast.getDay2());
			valueList.add(rainfallForecast.getDay3());
			valueList.add(rainfallForecast.getDay4());
			valueList.add(rainfallForecast.getDay5());
			valueList.add(rainfallForecast.getDay6());
			valueList.add(rainfallForecast.getDay7());
			Map<String, Double> dayToDataMap = new LinkedHashMap<String, Double>();
			mandalNameToForecastDataMap.put(locName, dayToDataMap);
			for(int index = 0; index < noOfForecastValues && index< valueList.size(); index++){
				dayToDataMap.put(dayPrefix+(index+1), valueList.get(index));
			}			
		}

		extractDataInFMFormat(mandalNameToForecastDataMap, finalDataMap, noOfForecastValues);

		return finalDataMap;
	}
	
	private void extractDataInFMFormat(Map<String, Map<String, Double>> inputMap,
			Map<String, List<Double>> outputMap, int noOfForecastValues) {
		String dayPrefix = "RFF";
		for (int index = 1; index <= noOfForecastValues; index++) {
			dayPrefix = dayPrefix + String.valueOf(index);
			for (String mandalName : BasinAssociation.EXPECTED_MANDAL_ORDER_FM) {
				if (null != inputMap.get(mandalName)) {
					Double value;
					if(null != inputMap.get(mandalName).get(dayPrefix)){
						value = getTMCFromMM(inputMap.get(mandalName).get(dayPrefix), mandalName);
					}else{
						value = -1.0;
					}
					
					if(null != outputMap.get(dayPrefix)){
						List<Double> dataList = outputMap.get(dayPrefix);
						dataList.add(value);
						
					}else{
						List<Double> dataList = new ArrayList<Double>();
						dataList.add(value);
						outputMap.put(dayPrefix, dataList);
					}
				} else {
					if(null != outputMap.get(dayPrefix)){
						List<Double> dataList = outputMap.get(dayPrefix);
						dataList.add(-1.0);
					}else{
						List<Double> dataList = new ArrayList<Double>();
						dataList.add(-1.0);
						outputMap.put(dayPrefix, dataList);
					}
				}

			}
			 dayPrefix = "RFF";
		}
	}
		
	@Override
	public Map<String, IRainfallForecast> getForecastDataForStress(long forecastDate) throws DSPException {
		
		Map<String, IRainfallForecast> resultMap = new HashMap<>();
		Map<String, List<String>> districtMandMap  = locService.getAllLocForParentChildTypes(LocationConstants.DISTRICT, LocationConstants.MANDAL);			
		Map<String, List<String>> mandVillMap  = locService.getAllLocForParentChildTypes(LocationConstants.MANDAL, LocationConstants.RAINFALL);

		resultMap = rainfallForecastDSP.getLastKnownRainfallForecast(forecastDate);
		if (IS_DEBUG_ENABLED) {
			System.out.println("resultMap :: "+resultMap.size());
		}
		for (String districtUUID : districtMandMap.keySet()) {	
			List<String> mandalList = districtMandMap.get(districtUUID);
						
			int districtMandalCountToday = 0;
			int districtMandalCountTmrw = 0;
			int districtMandalCountDay3 = 0;
			int districtMandalCountDay4 = 0;
			int districtMandalCountDay5 = 0;
			int districtMandalCountDay6 = 0;
			int districtMandalCountDay7 = 0;
			
			IRainfallForecast distForecastData = new RainfallForecast();
			distForecastData.setForecastDay(forecastDate);
			distForecastData.setLocationUUID(districtUUID);
			distForecastData.setDay1(0);
			distForecastData.setDay2(0);
			distForecastData.setDay3(0);
			distForecastData.setDay4(0);
			distForecastData.setDay5(0);
			distForecastData.setDay6(0);
			distForecastData.setDay7(0);

			for ( String mandalUUID : mandalList ) {
				IRainfallForecast mandalData =  resultMap.get(mandalUUID);
				
				if ( mandalData == null ) {
					if (IS_DEBUG_ENABLED) {
						System.out.println("ZBXXX : No rainfall forecast data found for mandal = "+mandalUUID);
					}
					continue;
				}

				if ( mandalData.getDay1() >= 0) {
					distForecastData.setDay1(distForecastData.getDay1() + mandalData.getDay1());  
					districtMandalCountToday++;
				}
				if ( mandalData.getDay2() >= 0) {
					distForecastData.setDay2(distForecastData.getDay2() + mandalData.getDay2());
					districtMandalCountTmrw++;
				}
				if ( mandalData.getDay3() >= 0) {
					distForecastData.setDay3(distForecastData.getDay3() + mandalData.getDay3());
					districtMandalCountDay3++;
				}
				if ( mandalData.getDay4() >= 0) {
					distForecastData.setDay4(distForecastData.getDay4() + mandalData.getDay4());
					districtMandalCountDay4++;
				}
				if ( mandalData.getDay5() >= 0) {
					distForecastData.setDay5(distForecastData.getDay5() + mandalData.getDay5());
					districtMandalCountDay5++;
				}
				if ( mandalData.getDay6() >= 0) {
					distForecastData.setDay6(distForecastData.getDay6() + mandalData.getDay6());
					districtMandalCountDay6++;
				}
				if ( mandalData.getDay7() >= 0) {
					distForecastData.setDay7(distForecastData.getDay7() + mandalData.getDay7());
					districtMandalCountDay7++;
				}

				
				// Use the Mandal's data for all the villages under it
				List<String> villageList = mandVillMap.get(mandalUUID);
				if(villageList != null) {
					for ( String village : villageList ) {
						
						IRainfallForecast villageData = new RainfallForecast();
						villageData.setLocationUUID(village);
						villageData.setDay1(mandalData.getDay1());
						villageData.setDay2(mandalData.getDay2());
						villageData.setDay3(mandalData.getDay3());
						villageData.setDay4(mandalData.getDay4());
						villageData.setDay5(mandalData.getDay5());
						villageData.setDay6(mandalData.getDay6());
						villageData.setDay7(mandalData.getDay7());
	
						villageData.setEventGenTS(mandalData.getEventGenTS());
						villageData.setForecastDay(mandalData.getForecastDay());
						
						resultMap.put(village, villageData);
					}				
				}
			}
			
			if ( districtMandalCountToday != 0) {
				distForecastData.setDay1(distForecastData.getDay1()/districtMandalCountToday);
			}else{
				distForecastData.setDay1(DSPConstants.NO_DATA);
			}
			
			if ( districtMandalCountTmrw != 0) {
				distForecastData.setDay2(distForecastData.getDay2()/districtMandalCountTmrw);
			}else{
				distForecastData.setDay2(DSPConstants.NO_DATA);
			}
			
			if ( districtMandalCountDay3 != 0) {
				distForecastData.setDay3(distForecastData.getDay3()/districtMandalCountDay3);
			}else{
				distForecastData.setDay3(DSPConstants.NO_DATA);
			}
			
			if ( districtMandalCountDay4 != 0) {
				distForecastData.setDay4(distForecastData.getDay4()/districtMandalCountDay4);
			}else{
				distForecastData.setDay4(DSPConstants.NO_DATA);
			}

			if ( districtMandalCountDay5 != 0) {
				distForecastData.setDay5(distForecastData.getDay5()/districtMandalCountDay5);
			}else{
				distForecastData.setDay5(DSPConstants.NO_DATA);
			}

			if ( districtMandalCountDay6 != 0) {
				distForecastData.setDay6(distForecastData.getDay6()/districtMandalCountDay6);
			}else{
				distForecastData.setDay6(DSPConstants.NO_DATA);
			}

			if ( districtMandalCountDay7 != 0) {
				distForecastData.setDay7(distForecastData.getDay7()/districtMandalCountDay7);
			}else{
				distForecastData.setDay7(DSPConstants.NO_DATA);
			}

				resultMap.put(districtUUID, distForecastData);
		}
		return resultMap;
	}

	@Override
	public Map<String, IRainfallForecast> getForecastDataForStress() throws DSPException, ObjectNotFoundException {

		long modelDate = getLastKnownForecastDate();
		if (modelDate == 0) {
			if (IS_DEBUG_ENABLED) {
				System.out.println("No Rainfall Forecast Data Found - No data in DB");
			}
			throw new ObjectNotFoundException("No Rainfall Forecast Data Found - No data in DB");
		}
		return getForecastDataForStress(modelDate);
	}

	@Override
	public List<IRainfallForecast> getAllLastKnownRainfallForecast(
			long forecastDay) throws DSPException {
		return rainfallForecastDSP.getAllLastKnownRainfallForecast(forecastDay);
	}

	@Override
	public Map<String, IRainfallForecast> getLastKnownRainfallForecast(long forecastDay) throws DSPException {
		return rainfallForecastDSP.getLastKnownRainfallForecast(forecastDay);
	}

	@Override
	public Map<String, IRainfallForecast> getLastKnownRainfallForecast(
			List<String> locationUUIDs, long forecastDay) throws DSPException {
		return rainfallForecastDSP.getLastKnownRainfallForecast(locationUUIDs, forecastDay);
	}
	
	@Override
	public int getLastForecastDateForLocation(String locationName) throws DSPException {
		String locationUUID = locService.getLocUUIDForLocName(locationName);
		return rainfallForecastDSP.getLastForecastDateForLocation(locationUUID);
	}
	
	/**
	 * Jira SMCS-202
	 * create rainfall forecast service
	 */
	@Override
	public Map<String, IRainfallForecast> getRFData(String location_uuid, long dateTs) throws DSPException{
		
		//Check if actual rainfall is available (water_quantity_rf), if yes then give it the higher priority
		if(rainfallForecastDSP.isWaterQuantityExist(location_uuid, dateTs)){
			//Returns data from water_quantity_rf table
			return rainfallForecastDSP.getRFData(location_uuid, dateTs);
		}
		else{
			//should I return data from rainfall_forecast if data does not exist in water_quantity_rf
			System.out.println("No water_quantity data exist for the given date "+ dateTs +"\nreturning null");
			return null;
		}
		
		
	}
	
}
