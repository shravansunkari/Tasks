package com.vassarlabs.iwm.soilmoisture.stress.service;

import static com.vassarlabs.iwm.utils.IWMConstants.IS_DEBUG_ENABLED;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.vassarlabs.common.dsp.err.DSPException;
import com.vassarlabs.common.dsp.utils.DSPConstants;
import com.vassarlabs.common.utils.DateUtils;
import com.vassarlabs.common.utils.StringUtils;
import com.vassarlabs.common.utils.err.ObjectNotFoundException;
import com.vassarlabs.iwm.commandcenter.pojo.api.IWeatherForecast;
import com.vassarlabs.iwm.commandcenter.service.api.IWeatherForecastService;
import com.vassarlabs.iwm.pojo.api.IExtendedRainfallForecastData;
import com.vassarlabs.iwm.pojo.api.ISoilMoistureRFObject;
import com.vassarlabs.iwm.pojo.api.IWaterQuantityEventData;
import com.vassarlabs.iwm.pojo.impl.ExtendedRainfallForecastData;
import com.vassarlabs.iwm.rainfall.forecast.pojo.api.IRainfallForecast;
import com.vassarlabs.iwm.rainfall.forecast.service.api.IRainfallForecastService;
import com.vassarlabs.iwm.service.api.IIWMService;
import com.vassarlabs.iwm.soilmoisture.stress.dsp.api.ISoilMoistureStressDataDSP;
import com.vassarlabs.iwm.soilmoisture.stress.pojo.api.ISMLocMapData;
import com.vassarlabs.iwm.soilmoisture.stress.pojo.api.ISMRainfallStressData;
import com.vassarlabs.iwm.soilmoisture.stress.pojo.api.ISoilMoistureNRSCData;
import com.vassarlabs.iwm.soilmoisture.stress.pojo.api.ISoilMoistureNRSCGridData;
import com.vassarlabs.iwm.soilmoisture.stress.pojo.api.ISoilMoistureStressData;
import com.vassarlabs.iwm.soilmoisture.stress.pojo.impl.SMRainfallStressData;
import com.vassarlabs.iwm.soilmoisture.stress.pojo.impl.SoilMoistureStressData;
import com.vassarlabs.iwm.soilmoisture.stress.service.api.ISMLocMapDataService;
import com.vassarlabs.iwm.soilmoisture.stress.service.api.ISoilMoistureNRSCDataService;
import com.vassarlabs.iwm.soilmoisture.stress.service.api.ISoilMoistureStressService;
import com.vassarlabs.iwm.utils.DataSourceConstants;
import com.vassarlabs.iwm.utils.IWMConstants;
import com.vassarlabs.iwm.utils.SourceTypeConstants;
import com.vassarlabs.location.service.api.ILocationHierarchyService;
import com.vassarlabs.location.utils.LocationConstants;
import com.vividsolutions.jts.io.ParseException;

@Component
public class SoilMoistureStressService
	implements ISoilMoistureStressService {
	
	@Autowired
	protected ISoilMoistureStressDataDSP soilMoistureStressDataDSP;
	
	@Autowired
	protected ISMLocMapDataService smLocMapDataService;

	@Autowired
	protected ISoilMoistureNRSCDataService nrscDataService;
	
	@Autowired
	protected ILocationHierarchyService locationHierarchyService;

	@Autowired
	protected IIWMService iwmService;
	
	@Autowired
	protected IRainfallForecastService rfForecastService;
	
	@Autowired
	protected ISoilMoistureNRSCDataService smNRSCData;

	@Autowired
	protected IWeatherForecastService weatherForecastService;

	@Override
	public Map<String, ISoilMoistureStressData> getAllSMStressData(double rootZoneDepth)
		throws DSPException, ObjectNotFoundException {
		
		int modelDate = nrscDataService.getLastKnownModelDateForNRSCData(DateUtils.getModelDateFromTs(System.currentTimeMillis()));
		if (modelDate == 0) {
			if (IS_DEBUG_ENABLED) {
				System.out.println("ZBXXX : SMStressService - getAllSMStressData, "
					+ "No Soil Moisture Stress Data found - No data in DB");
			}
			throw new ObjectNotFoundException("No Soil Moisture Stress Data found - No data in DB");
		}
		
		if (rootZoneDepth < 0) {
			if (IS_DEBUG_ENABLED) {
				System.out.println("ZBXXX : SMStressService - getAllSMStressData, Unable to compute soil moisture "
					+ "stress for a negative value of root depth, "+rootZoneDepth);
			}
			/* TODO Throw Proper Exception 
			 * */
			throw new ObjectNotFoundException("Unable to compute soil moisture "
					+ "stress for a negative value of root depth");
		}

		/*
		 * System.out.println("ZBXXX : SMStressService - getAllSMStressData, Model Date = "+modelDate
		 *		+ ", Root-Zone Depth = "+rootZoneDepth); 
		 * */
		return getAllSMStressData(modelDate, rootZoneDepth);
	}
	
	@Override
	public Map<String, ISoilMoistureStressData> getAllLastKnownSMStressData(int modelDate, double rootZoneDepth) 
			throws DSPException, ObjectNotFoundException {
		
		modelDate = nrscDataService.getLastKnownModelDateForNRSCData(modelDate);
	
		if (modelDate == 0) {
			if (IS_DEBUG_ENABLED) {
				System.out.println("ZBXXX : SMStresService - getAllLastKnownSMStressData. "
					+ "No Soil Moisture Stress Data found - No data in DB");
			}
			throw new ObjectNotFoundException("No Soil Moisture Stress Data found - No data in DB");
		}
		if (rootZoneDepth < 0) {
			if (IS_DEBUG_ENABLED) {
				System.out.println("ZBXXX : SMStressService - getAllLastKnownSMStressData, Unable to compute soil moisture "
					+ "stress for a negative value of root depth, "+rootZoneDepth);
			}
			/* TODO Throw Proper Exception 
			 * */
			throw new ObjectNotFoundException("Unable to compute soil moisture "
					+ "stress for a negative value of root depth");
		}

		/* 
		 * System.out.println("ZBXXX : SMStressService - getAllLastKnownSMStressData, Model Date = "+modelDate
		 *		+ ", Root-Zone Depth = "+rootZoneDepth); 
		 * */
		return getAllSMStressData(modelDate, rootZoneDepth);
	}
	
	@Override
	public Map<Long, ISoilMoistureStressData> getAllGridSMStressData(int modelDate, double rootZoneDepth)
		throws DSPException, ObjectNotFoundException {
		
		if (rootZoneDepth < 0) {
			if (IS_DEBUG_ENABLED) {
				System.out.println("ZBXXX : SMStressService - getAllGridSMStressData, Unable to compute soil moisture "
					+ "stress for a negative value of root depth, "+rootZoneDepth);
			}
			/* TODO Throw Proper Exception 
			 * */
			throw new ObjectNotFoundException("Unable to compute soil moisture "
					+ "stress for a negative value of root depth");
		}
		
		Map<String, ISoilMoistureStressData> villageSMStressMap = soilMoistureStressDataDSP.getSMStressDataAllVillages(modelDate, rootZoneDepth);

		Map<String, ISMLocMapData> gridVillageMap = new HashMap<String, ISMLocMapData>() ;
		Map<Long, ISoilMoistureStressData> gridSMStressMap = new HashMap<Long, ISoilMoistureStressData>();

		for (Entry<String, ISoilMoistureStressData> villageEntry : villageSMStressMap.entrySet()) {
			String villageName = villageEntry.getKey();
			if (!gridVillageMap.containsKey(villageName)) {
				/*
				 * System.out.println("ZBXXX : SMStressService - getAllGridSMStressData, "
				 * 		+ "No Grid-Village map record found for village : " + villageName+", rootDepth : " + rootZoneDepth); 
				 * */		
				continue;
			}
			gridSMStressMap.put(gridVillageMap.get(villageName).getGridId(), villageEntry.getValue());
		}
		return gridSMStressMap;
	}
	
	@Override
	public Map<Long, Double> getAllGridRFStressData(int modelDate)
		throws DSPException, ObjectNotFoundException {
		
		Map<String, ISMRainfallStressData> villageRFStressMap = getAllRFStressData(modelDate);
		
		List<ISoilMoistureNRSCGridData> smNRSCGridDataList = smNRSCData.getAllNRSCGridData();
		
		Map<Long, Double> gridRFStressMap = new HashMap<Long, Double>();

		for (ISoilMoistureNRSCGridData smNRSCGridData:smNRSCGridDataList) {
			List<ISMLocMapData> villList = smLocMapDataService.getSMLocMapDataForGridID(smNRSCGridData.getGridId());
			
			if (villList.size()!=0) {
				int nonRainyDays = 0;
				int count = 0;
				
				for (ISMLocMapData vill:villList) {
					String VillName = vill.getVillageFullName().toUpperCase();
					if (!villageRFStressMap.containsKey(VillName)) {
						/* 
						 * System.out.println("ZBXXX : SMStressService - getAllGridRFStressData, "
						 * 		+ "No Grid Village map record found for village : " + villageName); 
						 * */	
					}
					else {
						int villageRF = villageRFStressMap.get(VillName).getNoOfNonRainyDays();
						if (villageRF != -1){
							nonRainyDays += villageRF;
							count +=1;
						}
					}				
				}
				if (count!=0) {
					double avgNrd = (double) (nonRainyDays/count);
					gridRFStressMap.put(smNRSCGridData.getGridId(),avgNrd);
				}
			}
		/*  else{
		 *		System.out.println("No Villages for grid"+smNRSCGridData.getGridId());
		 *	} 
		 * */
		}
		return  gridRFStressMap;
	}
	
	@Override
	public Map<String, ISoilMoistureStressData> getAllSMStressData(int modelDate, double rootZoneDepth) 
			throws DSPException, ObjectNotFoundException {
		
		/*
		 *  1. Get the list of all SM-stress 'district names to Mandal names' map
		 *  2. Get all village level computed data and also add it to the result map
		 *  3. Get MandalFN to villageFullName Mapping
		 *  4. For each district
		 *  		Iterate over each Mandal under the district [and compute district level average] 
		 *  			Iterate over each village under the Mandal [and compute Mandal level average]
		 *  
		 *  Note : Perform average for { availableSMPercentage, availableSMPer150cm, rootZoneAvailableSMPer, rootZoneAvailableSMInMM }
		 *  	   For the remaining attributes, just sum up the values
		 *  
		 */
		
		if (rootZoneDepth < 0) {
			if (IS_DEBUG_ENABLED) {
				System.out.println("ZBXXX : SMStressService - getAllSMStressData(int modelDate, double rootZoneDepth), "
					+ "Unable to compute soil moisture stress for a negative value of root depth, "+rootZoneDepth);
			}
			/* TODO Throw Proper Exception 
			 * */
			throw new ObjectNotFoundException("Unable to compute soil moisture "
					+ "stress for a negative value of root depth");
		}
		
		Map<String, ISoilMoistureStressData> resultMap = new HashMap<>();
		Map<String, List<String>> allDistToMandMap = smLocMapDataService.getAllMandalList();
		
		Map<String, ISoilMoistureStressData> villageRFStressMap = soilMoistureStressDataDSP.getSMStressDataAllVillages(modelDate, rootZoneDepth);
		resultMap.putAll(villageRFStressMap);

		/* 
		 * Get MandalFN to villageFN mapping 
		 * */
		Map<String, List<String>> allMandToVillMap = smLocMapDataService.getVillagesForMandalsMap(null);

		
		for (String districtName : allDistToMandMap.keySet() ) {
			
			ISoilMoistureStressData distStressData = new SoilMoistureStressData();
			distStressData.setLocationName(districtName);
			distStressData.setFullLocationName(districtName);
			distStressData.setModelDate(modelDate);
			
			/* Get the list of MandalNames under the district */
			List<String> mandalList = allDistToMandMap.get(districtName);
			
			/* 
			 *	To store the total number of Mandal locations for which data is available, 
			 *	This total will be used to compute the average by dividing with the summed up values 
			 * 
			 * */
			Integer[] distMandCount = new Integer[] {0, 0, 0, 0, 0, 0};
			
			for (String mandalName : mandalList) {
				
				ISoilMoistureStressData mandStressData = new SoilMoistureStressData();
				mandStressData.setFullLocationName(districtName + "##" + mandalName);
				mandStressData.setLocationName(mandalName);
				mandStressData.setModelDate(modelDate);
				
				/* Get the list of VillFullNames for the MandalName */
				List<String> villageFullNameList = allMandToVillMap.get(mandStressData.getFullLocationName());
				
				/* 
				 *	To store the total number of village locations for which data is available, 
				 *	This total will be used to compute the average by dividing with the summed up values 
				 * 
				 * */
				Integer[] mandVillCount = new Integer[]{0, 0, 0, 0, 0, 0};
				
				/* Iterating over all the villages to sum up the values */
				for (String villageFullName : villageFullNameList) {
					
					ISoilMoistureStressData villStressData = villageRFStressMap.get(villageFullName);
					if (villStressData == null) {
						continue;
					}
					/* Sum up the village values to get the mandal level sum */
					addSoilMoistureStressData(villStressData, mandStressData, mandVillCount);
				}
				/* Compute average by dividing mandal level sum with villages count */
				computeAverageSMStressData(mandStressData, mandVillCount);
				
				/* Sum up the mandal values to get the district level sum */
				addSoilMoistureStressData(mandStressData, distStressData, distMandCount);
			
				/* Put the mandal level data into the result map*/
				resultMap.put(mandStressData.getFullLocationName(), mandStressData);
			}
			/* Compute average by dividing district level sum with mandals count */
			computeAverageSMStressData(distStressData, distMandCount);
			
			/* Add district data to the final map*/
			resultMap.put(distStressData.getFullLocationName(), distStressData);
		}
		return resultMap;
	}
		
	@Override
	public Map<String, ISMRainfallStressData> getAllRFStressData(int modelDate) throws DSPException, ObjectNotFoundException {
		/*
		 *  1. Get the list of all SM-stress 'district names to Mandal names' map
		 *  2. Get all village level computed data and also add it to the result map
		 *  3. Get MandalFN to villageFullName Mapping
		 *  4. For each district
		 *  		Iterate over each Mandal under the district [and compute district level average] 
		 *  			Iterate over each village under the Mandal [and compute Mandal level average]
		 */
		
		Map<String, ISMRainfallStressData> resultMap = new HashMap<>();
		Map<String, List<String>> allDistToMandMap = smLocMapDataService.getAllMandalList();
		
		Map<String, ISMRainfallStressData> villageRFStressMap = getRFStressDataForVillages(modelDate);
		resultMap.putAll(villageRFStressMap);


		/* 
		 * Construct MandalFN to villageFN mapping 
		 * */
		Map<String, List<String>> allMandToVillMap = smLocMapDataService.getVillagesForMandalsMap(null);

		
		ISMRainfallStressData distStressData;
		ISMRainfallStressData mandStressData;
		ISMRainfallStressData villStressData;
		
		for (String districtName : allDistToMandMap.keySet() ) {
			
			distStressData = new SMRainfallStressData();
			distStressData.setLocationName(districtName);
			distStressData.setFullLocationName(districtName);
			List<Integer> mandalNoOfNonRainyDaysList = new ArrayList<Integer>();
			distStressData.setChildNoOfNonRainyDaysList(mandalNoOfNonRainyDaysList);
			
			/* Get the list of MandalNames under the district */
			List<String> mandalList = allDistToMandMap.get(districtName);
			
			/* 
			 *	To store the total number of Mandal locations for which data is available, 
			 *	This total will be used to compute the average by dividing with the summed up values 
			 * 
			 * */
			Integer[] distMandCount = new Integer[] {0, 0, 0, 0, 0, 0, 0, 0, 0};
			
			for (String mandalName : mandalList) {
				
				mandStressData = new SMRainfallStressData();
				mandStressData.setFullLocationName(districtName + "##" + mandalName);
				mandStressData.setLocationName(mandalName);
				List<Integer> villNoOfNonRainyDaysList = new ArrayList<Integer>();
				mandStressData.setChildNoOfNonRainyDaysList(villNoOfNonRainyDaysList);
				
				/* Get the list of VillFullNames for the MandalName */
				List<String> villageFullNameList = allMandToVillMap.get(mandStressData.getFullLocationName());
				
				/* 
				 *	To store the total number of village locations for which data is available, 
				 *	This total will be used to compute the average by dividing with the summed up values 
				 * 
				 * */
				Integer[] mandVillCount = new Integer[]{0, 0, 0, 0, 0, 0, 0, 0, 0};
				
				/* Iterating over all the villages to sum up the values */
				for (String villageFullName : villageFullNameList) {
					
					villStressData = villageRFStressMap.get(villageFullName);
					if (villStressData == null) {
						/* Log Statement */
						continue;
					}
					/* Sum up the village values to get the mandal level sum */	
					addRainfallStressData(villStressData, mandStressData, mandVillCount);
				}
				/* Compute average by dividing mandal level sum with village count */
				computeAverageRainfallStressData(mandStressData, mandVillCount);
				
				/* Sum up the mandal values to get the district level sum */
				addRainfallStressData(mandStressData, distStressData, distMandCount);
				
				/* Put the mandal level data into the result map*/
				resultMap.put(mandStressData.getFullLocationName(), mandStressData);
			}
			/* Compute average by dividing district level sum with mandal count */
			computeAverageRainfallStressData(distStressData, distMandCount);
			
			/* Add district data to the final map*/
			resultMap.put(distStressData.getFullLocationName(), distStressData);
		}
		return resultMap;
	}
	
	protected Map<String, ISMRainfallStressData> getRFStressDataForVillages(int modelDate) 
				throws DSPException, ObjectNotFoundException {
		
		/* The final map that will be returned */
		Map<String, ISMRainfallStressData> resultMap = new HashMap<>();
		
		/* DB Call : Getting ISMLocMapData for all the villages */
		Map<String, List<ISMLocMapData>> soilMoistLocDataMap = smLocMapDataService.getAllSMLocMapData();

		/* Preparing all the required time stamps */
		
		/* 
		 * If modelDate = today's date
		 * 		then Apply rainfall Logic based on time
		 * else
		 * 		modelDate timeStamp must be greater than 8 : 30 i.e rainfall day start time 
		 */
		long systemCurrMillis = System.currentTimeMillis();
		long todayStartTs = DateUtils.getStartOfDay(systemCurrMillis);
		long todayEndTs = DateUtils.getEndOfDay(systemCurrMillis);
		long modelDateMillis = DateUtils.getModelDateInMillis(modelDate);		
		
		/*
		 * Checking if modelDate is today or not
		 */
		long curTime;
		if (todayStartTs <= modelDateMillis && modelDateMillis <= todayEndTs) {
			 long ts0830 = DateUtils.getIntervalOfDay(systemCurrMillis, 8, 30, 01);
			 long ts0929 = DateUtils.getIntervalOfDay(systemCurrMillis ,9, 29, 59);
				/*
				 *  If the current TS is between 8:30:01 AM to 9:29:59 AM change it to 8:30 AM
				 *  http://inkriti.net/jira/browse/VAS-247 
				 */
				if (systemCurrMillis >= ts0830 && systemCurrMillis <= ts0929) {
					curTime = ts0830;
				} else {
					curTime = systemCurrMillis;	
				}
		} else {
			/*
			 *	Consider the day time to be more than 8:30 AM
			 *
			 *	NOTE : (3600 seconds/hour * 8 1/2 Hours) *1000 = 30600000 milliseconds
			 *
			 */
			curTime = modelDateMillis + 30600001;  
		}
		
		/*
		 * RF day starts at 8:30 AM
		 */
		long currDayStartTs = DateUtils.getLastIntervalOfGivenTime(curTime, 8, 30, 0);
		long currDayEndTs = currDayStartTs + DateUtils.TWENTY_FOUR_HOURS_IN_SECONDS;
		long yesterdayStartTs = DateUtils.getIntervalOfDay(DateUtils.getYesterdayStartTs(currDayStartTs), 8, 30, 0);
		long yesterdayEndTs = yesterdayStartTs + DateUtils.TWENTY_FOUR_HOURS_IN_SECONDS;
		long last7DaysTime = DateUtils.getIntervalOfDay(DateUtils.getLastNDaysTs(curTime, 7), 8, 30, 0);
		long last10DaysTime = DateUtils.getIntervalOfDay(DateUtils.getLastNDaysTs(curTime, 10), 8, 30, 0);
		long last3DaysTime = DateUtils.getIntervalOfDay(currDayStartTs-(3*DateUtils.TWENTY_FOUR_HOURS_IN_SECONDS), 8, 30, 0);
		
		/* Prepare a list of all rainfall location UUIDs */
		Map<String, List<String>> stateToRainfallLocMap = locationHierarchyService.getAllLocForParentChildTypes(LocationConstants.STATE, LocationConstants.RAINFALL);
		List<String> rainfallLocations = stateToRainfallLocMap.get("6f86292b-dd9a-4987-bb8f-c3940263b349"); /* Andhra Pradesh LocationUUID */
		
		Set<String> setOfLocations = new HashSet<>();
		for ( String locationUUID : rainfallLocations) {
			setOfLocations.add(locationUUID);
		}
		
		/* DB Calls : Get all rainfall related data required */
		Map<String, IWaterQuantityEventData> last3DaysData = iwmService.getRFWQDataForStress(last3DaysTime, yesterdayEndTs, setOfLocations);
		Map<String, IWaterQuantityEventData> last7DaysData = iwmService.getRFWQDataForStress(last7DaysTime, yesterdayEndTs, setOfLocations);
		Map<String, IWaterQuantityEventData> last10DaysData = iwmService.getRFWQDataForStress(last10DaysTime, yesterdayEndTs, setOfLocations);
		Map<String, IWaterQuantityEventData> yesterdayData = iwmService.getRFWQDataForStress(yesterdayStartTs , yesterdayEndTs, setOfLocations);
		Map<String, IWaterQuantityEventData> todayData = iwmService.getRFWQDataForStress(currDayStartTs, currDayEndTs, setOfLocations);
		Map<String, IExtendedRainfallForecastData> rainfallForecastData = getMultiSourcedRainfallForecast(modelDate, rainfallLocations);

		Map<String, ISoilMoistureRFObject> nonRainyDaysData = iwmService.getVillageContinousNonRainyDays(currDayEndTs, IWMConstants.MIN_RAINFALL_LEVEL);

		/*
		 * Step 1 : Iterate over each village
		 * Step 2 : iterate over the list of rainfall station UUIDs under that village 
		 * 			and compute average over all the stations under the village to get the village level data
		 * 
		 */
		for (String villageFullName : soilMoistLocDataMap.keySet()) {
			
			List<ISMLocMapData> smLocDataList = soilMoistLocDataMap.get(villageFullName);
			ISMRainfallStressData villRFStress = new SMRainfallStressData();
			villRFStress.setFullLocationName(villageFullName);
			if (smLocDataList != null && smLocDataList.get(0) != null) {
				villRFStress.setLocationName(smLocDataList.get(0).getVillageName());
			}

			/* 
			 *	To store the total count of locations for which data is available, 
			 *	This total will be used to compute the average by dividing with the summed up values 
			 * 
			 * */
			Integer[] villToStationCount = new Integer[]{0, 0, 0, 0, 0, 0, 0, 0, 0};
			
			for (ISMLocMapData smLocData: smLocDataList) {
				String rainfallUUID = smLocData.getRainfallLocUUID();

				if (last7DaysData.containsKey(rainfallUUID) && last7DaysData.get(rainfallUUID).getLevelValue1() >= 0) {
					villRFStress.setLast7DaysCumulative(villRFStress.getLast7DaysCumulative() + last7DaysData.get(rainfallUUID).getLevelValue1());
					villToStationCount[IWMConstants.SM_RF_LAST7DAYS_INDEX]++;
				}
				
				if (last3DaysData.containsKey(rainfallUUID) && last3DaysData.get(rainfallUUID).getLevelValue1() >= 0) {
					villRFStress.setLast3DaysCumulative(villRFStress.getLast3DaysCumulative() + last3DaysData.get(rainfallUUID).getLevelValue1());
					villToStationCount[IWMConstants.SM_RF_LAST3DAYS_INDEX]++;
				}
				
				if (todayData.containsKey(rainfallUUID) && todayData.get(rainfallUUID).getLevelValue1() >= 0) {
					villRFStress.setTodayActual(villRFStress.getTodayActual() + todayData.get(rainfallUUID).getLevelValue1());
					villToStationCount[IWMConstants.SM_RF_TODAY_INDEX]++;
				}
				
				if (yesterdayData.containsKey(rainfallUUID) && yesterdayData.get(rainfallUUID).getLevelValue1() >= 0) {
					villRFStress.setYesterdayActual(villRFStress.getYesterdayActual() + yesterdayData.get(rainfallUUID).getLevelValue1());
					villToStationCount[IWMConstants.SM_RF_YESTERDAY_INDEX]++;
				}
				
				if (rainfallForecastData.containsKey(rainfallUUID)) {
					
					IExtendedRainfallForecastData forecastData = rainfallForecastData.get(rainfallUUID);
					
					double next7DaysForecastCum = 0.0;
					boolean isForecastDataAvailable = false;

					/* For today's forecast, we use current rainfall, 
					 * from tomorrow, we use forecasted data
					 */
					if (todayData.containsKey(rainfallUUID) && todayData.get(rainfallUUID).getLevelValue1() >= 0) {
						next7DaysForecastCum += todayData.get(rainfallUUID).getLevelValue1();
						villRFStress.setNext24HrForecast(villRFStress.getNext24HrForecast() + todayData.get(rainfallUUID).getLevelValue1());	
						villToStationCount[IWMConstants.SM_RF_24HRS_FORECAST_INDEX]++;
						isForecastDataAvailable = true;
					}
					if (forecastData.getDay2() >= 0) {
						next7DaysForecastCum += forecastData.getDay2();
						villRFStress.setNext48HrForecast(villRFStress.getNext48HrForecast() + forecastData.getDay2());
						villToStationCount[IWMConstants.SM_RF_48HRS_FORECAST_INDEX]++;
						isForecastDataAvailable = true;
					}
					if (forecastData.getDay3() >= 0) {
						next7DaysForecastCum += forecastData.getDay3();
						isForecastDataAvailable = true;
					}
					if (forecastData.getDay4() >= 0) {
						next7DaysForecastCum += forecastData.getDay4();
						isForecastDataAvailable = true;
					}
					if (forecastData.getDay5() >= 0) {
						next7DaysForecastCum += forecastData.getDay5();
						isForecastDataAvailable = true;
					}
					if (forecastData.getDay6() >= 0) {
						next7DaysForecastCum += forecastData.getDay6();
						isForecastDataAvailable = true;
					}
					if (forecastData.getDay7() >= 0) {
						next7DaysForecastCum += forecastData.getDay7();
						isForecastDataAvailable = true;
					}
					
					if (isForecastDataAvailable) {
						villRFStress.setNext7DaysForecastCumulative(villRFStress.getNext7DaysForecastCumulative() + next7DaysForecastCum);
						villToStationCount[IWMConstants.SM_RF_7DAYS_FORECAST_INDEX]++;
					}
				}
				
				if (nonRainyDaysData.containsKey(rainfallUUID) && nonRainyDaysData.get(rainfallUUID).getNoOfNonRainDays() >= 0) {
					villRFStress.setNoOfNonRainyDays(villRFStress.getNoOfNonRainyDays() + nonRainyDaysData.get(rainfallUUID).getNoOfNonRainDays());
					villToStationCount[IWMConstants.SM_RF_NON_RAINY_DAYS_INDEX]++;
				}
				
				if (last10DaysData.containsKey(rainfallUUID) && last10DaysData.get(rainfallUUID).getLevelValue1() >= 0) {
					villRFStress.setLast10DaysCumulative(villRFStress.getLast10DaysCumulative() + last10DaysData.get(rainfallUUID).getLevelValue1());
					villToStationCount[IWMConstants.SM_RF_LAST10DAYS_INDEX]++;
				}
				
			}
			/* Compute average by dividing village level sum with rf-stations count */
			computeAverageRainfallStressData(villRFStress, villToStationCount);
			
			/* Add to the final map*/
			resultMap.put(villageFullName, villRFStress);
		}
		return resultMap;
	}
	
	@Override
	public Map<String, Map<Integer, ISoilMoistureStressData>> getSMStressDataForRange(
			int startDate, int endDate, List<String> locationFullNameList, double rootZoneDepth)
			throws DSPException, ObjectNotFoundException {

		if (rootZoneDepth < 0) {
			if (IS_DEBUG_ENABLED) {
				System.out.println("ZBXXX : SMStressService - getSMStressDataForRange, Unable to compute soil moisture "
					+ "stress for a negative value of root depth, "+rootZoneDepth);
			}	
			/* TODO Throw Proper Exception 
			 * */
			throw new ObjectNotFoundException("Unable to compute soil moisture "
					+ "stress for a negative value of root depth");
		}
		
		Map<String, Map<Integer, ISoilMoistureStressData>> resultMap = new HashMap<String, Map<Integer, ISoilMoistureStressData>>();

		/* Distinguish mandal names and village names, 
		 * store each of them in a separate List 
		 * */
		List<String> mandalNamesList = new ArrayList<>();
		List<String> villageNamesList = new ArrayList<>();
		for (String fullName : locationFullNameList) {
			if (StringUtils.isVillageConcatenatedName(fullName)) {
				villageNamesList.add(fullName);				
			} 
			else {
				mandalNamesList.add(fullName);				
			}
		}
		
		/* 
		 * Get villages for the given Mandals 
		 * */ 
		Map<String, List<String>> mandalVillMap = smLocMapDataService.getVillagesForMandalsMap(mandalNamesList);

		/*
		 *  Make a single list of all villageNames 
		 * 	i. By adding all the given villages to it
		 * 	ii. By adding all the villages under the given Mandals
		 * 
		 * */
		List<String> allVillageNamesList = new ArrayList<>();
		allVillageNamesList.addAll(villageNamesList);
		for (String mandalName : mandalVillMap.keySet()) {
			List<String> villageList = mandalVillMap.get(mandalName);
			for (String villageName : villageList) {
				if (!allVillageNamesList.contains(villageName)) {
					allVillageNamesList.add(villageName);										
				}
			}
		}
		
		/*
		 * Get village level stress data 
		 */
		resultMap = getSMVillageStressData(startDate, endDate, allVillageNamesList, rootZoneDepth);

		/* Computes MandalLevel Data, if any
		 * 
		 * Note : Perform average for { availableSMPercentage, availableSMPer150cm, rootZoneAvailableSMPer, rootZoneAvailableSMInMM }
		 *  	   For the remaining attributes, just sum up the values
		 * */		
		for (String mandalName : mandalVillMap.keySet()) {
			
			List<String> villageList = mandalVillMap.get(mandalName);
			Map<Integer, ISoilMoistureStressData> dateToDataMap = new HashMap<>();
			Map<Integer, Integer[]> villDataCountForDate = new HashMap<>();
			
			for (String villageName : villageList) {
				Map<Integer, ISoilMoistureStressData> villDateToDataMap = resultMap.get(villageName);
				
				if (villDateToDataMap == null) {
					if (IS_DEBUG_ENABLED) {
						System.out.println("ZBXXX : SMStressService - getSMStressDataForRange, No SM Stress data available "
							+ "for village = " +villageName + ", mandal = "+mandalName+", between "+startDate +" and "+endDate);
					}
					continue;
				}
				
				for (Integer modelDate :  villDateToDataMap.keySet()) {
					ISoilMoistureStressData villStressData = villDateToDataMap.get(modelDate);
					ISoilMoistureStressData mandStressData = dateToDataMap.get(modelDate);
					Integer[] mandVillCount = villDataCountForDate.get(modelDate);
					
					if (mandStressData == null) {
						mandStressData = new SoilMoistureStressData();
						mandStressData.setFullLocationName(mandalName);
						mandStressData.setModelDate(modelDate);
						dateToDataMap.put(modelDate, mandStressData);
						/*
						 * To store the total count of grids for which data is available,
						 * This total will be used to compute the average by dividing with
						 * the summed up values
						 * 
						 */
						mandVillCount = new Integer[] {0, 0, 0, 0, 0, 0};
						villDataCountForDate.put(modelDate, mandVillCount);
					}
					
					/* Summing up the data of all the villages under the Mandal */
					addSoilMoistureStressData(villStressData, mandStressData, mandVillCount);
				}
			}
			
			if (dateToDataMap.size()==0) {
				if (IS_DEBUG_ENABLED) {
					System.out.println("ZBXXX : SMStressService - getSMStressDataForRange, No SM Stress data available "
						+ "for mandal = " +mandalName + ", between "+startDate +" and "+endDate);
				}
				continue;
			}
			
			for (Integer modelDate : dateToDataMap.keySet()) {
				ISoilMoistureStressData mandStressData = dateToDataMap.get(modelDate);
				Integer[] mandVillCount = villDataCountForDate.get(modelDate);

				/* Compute average by dividing mandal level sum with villages count */
				computeAverageSMStressData(mandStressData, mandVillCount);
			}
			resultMap.put(mandalName, dateToDataMap);
		}
		
		/* 
		 * Remove the entries for the 'extra-villages' which are 
		 * under the given Mandals (if any)
		 * */
		for (String mandalName : mandalVillMap.keySet()) {
			List<String> villageList = mandalVillMap.get(mandalName);
			for (String villageName : villageList) {
				if (!villageNamesList.contains(villageName)) {
					resultMap.remove(villageName);
				}
			}
		}
		return resultMap;
	}

	@Override
	public Map<String, Map<Integer, Double>> getSMStressRFDataForDate(
			int startDate, int endDate, List<String> locationFullNameList)
			throws DSPException, ObjectNotFoundException {
		
		Map<String, Map<Integer, Double>> resultMap = new HashMap<>();
		
		/* Convert model dates to time-stamps in Milliseconds */
		long startTs = DateUtils.getStartOfDay(DateUtils.getModelDateInMillis(startDate));
		long endTs = DateUtils.getEndOfDay(DateUtils.getModelDateInMillis(endDate));

		List<String> allRFUUIDList = new ArrayList<>(); 
		List<String> mandalNamesList = new ArrayList<>();
		List<String> villageNamesList = new ArrayList<>();
		List<String> allVillageNamesList = new ArrayList<>();

		/* 
		 * Distinguish mandal names and village names, 
		 * store each of them in a separate List 
		 * */
		for (String locFullName : locationFullNameList) {
			if (StringUtils.isVillageConcatenatedName(locFullName)) {
				villageNamesList.add(locFullName);				
			} 
			else {
				mandalNamesList.add(locFullName);				
			}
		}
		
		/* Getting a map of mandalFullN to villageFNList 
		 * */
		Map<String, List<String>> mandalVillMap = smLocMapDataService.getVillagesForMandalsMap(mandalNamesList);

		/*
		 *  Make a single list of all villageNames 
		 * 	i. By adding all the given villages to it
		 * 	ii. By adding all the villages under the given Mandals
		 * 
		 * */
		allVillageNamesList.addAll(villageNamesList);
		for (String mandalName : mandalVillMap.keySet()) {
			List<String> villageList = mandalVillMap.get(mandalName);
			for (String villageName : villageList) {
				if (!allVillageNamesList.contains(villageName)) {
					allVillageNamesList.add(villageName);										
				}
			}
		}
		/* Get grid data for all the required villages
		 * */
		Map<String, List<ISMLocMapData>> villageSMLocDataMap = smLocMapDataService.getSMLocMapData(allVillageNamesList);

		/* 
		 * Make a single list of all the rainfall locationUUIDs, 
		 * associated with the given village locations 
		 * */
		for (String villageName : villageSMLocDataMap.keySet()) {
			List<ISMLocMapData> gridsUnderVillage = villageSMLocDataMap.get(villageName);
			for (ISMLocMapData gridData : gridsUnderVillage) {
				if(!allRFUUIDList.contains(gridData.getRainfallLocUUID())){
					allRFUUIDList.add(gridData.getRainfallLocUUID());					
				}
			}
		}
		/* Get rainfall data between the given dates for the given locationUUIDs */
		Map<String, List<IWaterQuantityEventData>> rfData = iwmService.getWaterQuantityEventsInTimeRange(allRFUUIDList, startTs, endTs, SourceTypeConstants.RAINFALL);

		/*
		 * 
		 * 1. For each of the given village
		 * 2.			Create  a map, M1, to store 'date' to 'dataRecordsCount'
		 * 3.			Create a map, M2, to store 'date' to 'rainfallLevelData'
		 * 4. 			For every grid under the village, find out it's rfStation
		 * 5.				For every data record associated with that rfStation
		 * 6.						Find out it's date and compute sum with the existing data in M2 for that date and store it back in M2
		 * 7.						Increment the dataRecordsCount for that date in M1		 
		 * 8. 			For each (date, Data) pair in M2
		 * 9.					compute average by dividing the Data with the 'dataRecordsCount' avilable from M1 for that date.
		 * 
		 * */
		
		for (String villageName : villageSMLocDataMap.keySet()) {
		
			/* 
			 * i. Getting list of all gridIDs for the given village
			 * ii. Creating a map to store date to Data
			 * iii. Creating a map to store date to dataCount
			 * 
			 * */
			List<ISMLocMapData> gridsUnderVillage = villageSMLocDataMap.get(villageName);
			Map<Integer, Double> dateToLevelMap = new HashMap<>();
			Map<Integer, Integer> dateToGridCount = new HashMap<>();
			
			for (ISMLocMapData gridData : gridsUnderVillage) {
				/* Find out the rainfall station for the given grid */
				String rfLocationUUID = gridData.getRainfallLocUUID();
				
				/* Get rainfall data for the above rainfall station*/
				List<IWaterQuantityEventData> rfLocDataList = rfData.get(rfLocationUUID);
				if (rfLocDataList == null) {
					if (IS_DEBUG_ENABLED) {
						System.out.println("ZBXXX : SMStressService - getSMStressRFDataForDate, No rainfall data available "
						+ "for grid = " +gridData.getGridId() + " with rfLocUUID = "+rfLocationUUID);
					}
					continue;
				}
				
				for (IWaterQuantityEventData wqEventData :  rfLocDataList) {
					int modelDate = DateUtils.getModelDateFromTs(wqEventData.getEventGenTs());
					if (!dateToLevelMap.containsKey(modelDate)) {
						dateToLevelMap.put(modelDate, 0.0);
						dateToGridCount.put(modelDate, 0);
					}
					if (wqEventData.getLevelValue1() >= 0) {
						dateToLevelMap.put(modelDate, dateToLevelMap.get(modelDate) + wqEventData.getLevelValue1());
						dateToGridCount.put(modelDate, dateToGridCount.get(modelDate) + 1);
					}
				}
			}
			
			if (dateToLevelMap.size()==0) {
				if (IS_DEBUG_ENABLED) {
					System.out.println("ZBXXX : SMStressService - getSMStressRFDataForDate, No rainfall data available "
						+ "for village = " +villageName + ", between "+startDate +" and "+endDate);
				}
				continue;
			}
			
			for (Integer modelDate : dateToLevelMap.keySet()) {
				if (dateToGridCount.get(modelDate) != 0) {
					dateToLevelMap.put(modelDate, dateToLevelMap.get(modelDate)/dateToGridCount.get(modelDate));
				}
				else {
					dateToLevelMap.put(modelDate, DSPConstants.NO_DATA);
				}
			}
			resultMap.put(villageName, dateToLevelMap);
		}
		
		/*
		 *  Computes MandalLevel Data, if any		
		 */
		for (String mandalName : mandalVillMap.keySet()) {
			/* 
			 * i. Getting list of all villages for the given Mandal
			 * ii. Creating a map to store date to Data
			 * iii. Creating a map to store date to dataCount
			 * 
			 * */
			List<String> villageList = mandalVillMap.get(mandalName);
			Map<Integer, Double> dateToLevelMap = new HashMap<>();
			Map<Integer, Integer> villDataCountForDate = new HashMap<>();
			
			/* 
			 * Iterate over villages and compute average for each day 
			 * */
			for (String villageName : villageList) {
				
				Map<Integer, Double> villDateToLevelMap = resultMap.get(villageName);
				if (villDateToLevelMap == null) {
					if (IS_DEBUG_ENABLED) {
						System.out.println("ZBXXX : SMStressService - getSMStressRFDataForDate, No rainfall data available "
							+ "for village = " +villageName + ", mandal = "+mandalName+", between "+startDate +" and "+endDate);
					}
					continue;
				}
				
				for (Integer modelDate :  villDateToLevelMap.keySet()) {
					if (!dateToLevelMap.containsKey(modelDate)) {
						dateToLevelMap.put(modelDate, 0.0);
						villDataCountForDate.put(modelDate, 0);
					}
					if (villDateToLevelMap.get(modelDate) >= 0) {
						dateToLevelMap.put(modelDate, dateToLevelMap.get(modelDate)+villDateToLevelMap.get(modelDate));
						villDataCountForDate.put(modelDate, villDataCountForDate.get(modelDate)+1);
					}
				}
			}
			
			if (dateToLevelMap.size()==0) {
				if (IS_DEBUG_ENABLED) {
					System.out.println("ZBXXX : SMStressService - getSMStressRFDataForDate, No rainfall data available "
						+ "for mandal = " +mandalName + ", between "+startDate +" and "+endDate);
				}
				continue;
			}
			
			for (Integer modelDate : dateToLevelMap.keySet()) {
				if (villDataCountForDate.get(modelDate) >= 0) {
					dateToLevelMap.put(modelDate, dateToLevelMap.get(modelDate)/villDataCountForDate.get(modelDate));					
				}
				else {
					dateToLevelMap.put(modelDate, DSPConstants.NO_DATA);
				}
			}
			
			resultMap.put(mandalName, dateToLevelMap);
		}
		
		/* 
		 * Remove the entries for the 'extra-villages' which are 
		 * under the given Mandals (if any)
		 * */
		for (String mandalName : mandalVillMap.keySet()) {
			List<String> villageList = mandalVillMap.get(mandalName);
			for (String villageName : villageList) {
				if (!villageNamesList.contains(villageName)) {
					resultMap.remove(villageName);
				}
			}
		}
		
		return resultMap;		
	}
	
	@Override
	public Map<String, Map<Integer, ISoilMoistureStressData>> getSMVillageStressData(int startDate, int endDate, 
			List<String> villageList, double rootZoneDepth) throws DSPException, ObjectNotFoundException {
		Map<String, Map<Integer, ISoilMoistureStressData>> resultMap = new HashMap<>();

		if (rootZoneDepth < 0) {
			if (IS_DEBUG_ENABLED) {
				System.out.println("ZBXXX : SMStressService - getSMVillageStressData, Unable to compute soil moisture "
					+ "stress for a negative value of root depth, "+rootZoneDepth);
			}	
			/* TODO Throw Proper Exception 
			 * */
			throw new ObjectNotFoundException("Unable to compute soil moisture "
					+ "stress for a negative value of root depth");
		}
		
		/* 
		 * Make a list of all Monsoons that falls 
		 * within the given startDate and endDate   
		 * 
		 * */
		List<Integer> spannedMonsoons = new ArrayList<>();
		for (int date = startDate; date<=endDate ; date++) {
			int monsoonStartDate = DateUtils.getStartOfMonsoon(date);
			if (!spannedMonsoons.contains(monsoonStartDate)) {
				spannedMonsoons.add(monsoonStartDate);
			}
		}
		/* 
		 * Get ISMLocMapData for all the given villages. 
		 * And then make a list of all required gridIDs
		 * 
		 *  */
		Map<String, List<ISMLocMapData>> smLocDataMap = smLocMapDataService.getSMLocMapData(villageList);
		List<Long> gridIDList = new ArrayList<>(); 
		for (String village : smLocDataMap.keySet()) {
			List<ISMLocMapData> gridsUnderVillage =  smLocDataMap.get(village);
			for (ISMLocMapData locData : gridsUnderVillage) {
				if (!gridIDList.contains(locData.getGridId())) {
					gridIDList.add(locData.getGridId());
				}
			}
		}
		
		/* 
		 * DBCall - Get the GridData for all the required gridIDs 
		 * And then make a Map from GridID to GridData 
		 * */
		List<ISoilMoistureNRSCGridData> gridDataList = smNRSCData.getNRSCGridData(gridIDList);
		Map<Long, ISoilMoistureNRSCGridData> gridDataMap = new HashMap<>();
		for (ISoilMoistureNRSCGridData gridData : gridDataList ) {
			gridDataMap.put(gridData.getGridId(), gridData);
		}
		
		/* 
		 * DBCall - Get June1st soil moisture data for all spanned Monsoons 
		 * */
		Map<Integer, Map<Long, ISoilMoistureNRSCData>> allJune1stData = new HashMap<>();
		for (Integer monsoonStart : spannedMonsoons) {
			Map<Long, ISoilMoistureNRSCData> june1stNRSCData = nrscDataService.getNRSCData(monsoonStart, gridIDList);
			allJune1stData.put(monsoonStart, june1stNRSCData);
		}
		
		/* DBCall : Get all soil moisture data between the startDate and endDate */
		Map<Long, List<ISoilMoistureNRSCData>> nrscDataMap = nrscDataService.getNRSCData(startDate, endDate, gridIDList);

		/*
		 * 
		 * 1. For each of the given village
		 * 2.			Create  a map, M1, to store 'date' to 'dataRecordsCount'
		 * 3.			Create a map, M2, to store 'date' to 'actualData'
		 * 4. 			For every grid under the village
		 * 5.				For every data record associated with that grid
		 * 6.						Find out it's date and compute sum with the existing data in M2 for that date and store it back in M2
		 * 7.						Increment the dataRecordsCount for that date in M1		 
		 * 8. 			For each (date, Data) pair in M2
		 * 9.					compute average by dividing the Data with the 'dataRecordsCount' avilable from M1 for that date.
		 * 
		 * */
		for (String villageFullName : villageList) {


			/* 
			 * i. Getting list of all gridIDs for the given village
			 * ii. Creating a map to store date to Data
			 * iii. Creating a map to store date to dataCount
			 * 
			 * */
			List<ISMLocMapData> smLocDataList = smLocDataMap.get(villageFullName);
			Map<Integer, ISoilMoistureStressData> dateToDataMap = new HashMap<>();
			Map<Integer, Integer[]> dateToGridCount = new HashMap<>();

			/* For each grid under the village */
			for (ISMLocMapData smLocData : smLocDataList) {
				
				/*
				 * Get the following data for the grid : 
				 * 		i.	NRSC grid data 
				 * 		ii. All NRSC soil moisture data between the given dates
				 */
				
				ISoilMoistureNRSCGridData gridData = gridDataMap.get(smLocData.getGridId());
				List<ISoilMoistureNRSCData> nrscCurDataList = nrscDataMap.get(smLocData.getGridId());
				
				if (nrscCurDataList == null || gridData == null) {
					if (IS_DEBUG_ENABLED) {
						System.out.println("ZBXXX : SMStressDSP : inside getSMStressDataAllVillages(), "
							+ "Skipping record, NRSCData or GridData is null for gridID = "+smLocData.getGridId() 
							+ "[ NRSCDataList = "+nrscCurDataList+", gridData = "+ gridData+"]");
					}
					continue;
				}
				
				/* 
				 * Iterate over each of the fetched NRSC soil moisture record for the grid 
				 * */
				for (ISoilMoistureNRSCData nrscData : nrscCurDataList) {
					int modelDate = nrscData.getModelDate();
					ISoilMoistureStressData villageStressData = dateToDataMap.get(modelDate);
					Integer[] villToGridCount = dateToGridCount.get(modelDate);
					
					if (villageStressData == null) {
						villageStressData = new SoilMoistureStressData();
						villageStressData.setFullLocationName(villageFullName);
						villageStressData.setLocationName(smLocData.getVillageName());
						villageStressData.setModelDate(modelDate);
						
						dateToDataMap.put(modelDate, villageStressData);
						/*
						 * To store the total count of grids for which data is available,
						 * This total will be used to compute the average by dividing with
						 * the summed up values
						 * 
						 */
						villToGridCount = new Integer[] { 0, 0, 0, 0, 0, 0 };
						dateToGridCount.put(modelDate, villToGridCount);
					}
					Integer monsoonStart = DateUtils.getStartOfMonsoon(modelDate);
					ISoilMoistureNRSCData june1stData = allJune1stData.get(monsoonStart).get(gridData.getGridId()); 
					
					if (june1stData == null ) {
						if (IS_DEBUG_ENABLED) {
							System.out.println("ZBXXX : SMStressDSP : inside getSMStressDataAllVillages(), "
								+ "Skipping record, june1stNRSCData for grid = "+ smLocData.getGridId());
						}
						continue;
					}
					
					/* Computing various soil moisture data */
					ISoilMoistureStressData gridStressData = new SoilMoistureStressData();
					gridStressData.setModelDate(modelDate);
					gridStressData.setSoilMoisturePercent(soilMoistureStressDataDSP.computeAvailableSMPercentage(nrscData, gridData, IWMConstants.SM_DEFAULT_TOTAL_DEPTH));
					gridStressData.setRootZoneAvailableSoilMoistureInMM(soilMoistureStressDataDSP.computeAvailableSMInMM(nrscData, gridData, rootZoneDepth));
					gridStressData.setAverageSMRootZone(soilMoistureStressDataDSP.computeAvailableSMPercentage(nrscData, gridData, rootZoneDepth));
					Double june1stSMContent = soilMoistureStressDataDSP.computeMoistureContent(june1stData, gridData, smLocData, rootZoneDepth);
					Double currSMContent = soilMoistureStressDataDSP.computeMoistureContent(nrscData, gridData, smLocData, rootZoneDepth);
					gridStressData.setAverageSM15m(gridStressData.getSoilMoisturePercent());
					gridStressData.setAvailableSoilMoistureInTMC(currSMContent);

					/* Summing up the data of all the grids under the village */
					addSoilMoistureStressDataForVillage(gridStressData, villageStressData, villToGridCount, currSMContent, june1stSMContent);
				}
			}
			
			if (dateToDataMap.size() == 0) {
				if (IS_DEBUG_ENABLED) {
					System.out.println("ZBXXX : SMStressDSP : inside getSMStressDataAllVillages(), "
						+ "No SoilMoistureStressData is available for village = "+ villageFullName);
				}
				continue;
			}
			
			for (Integer modelDate : dateToDataMap.keySet()) {
				ISoilMoistureStressData stressData = dateToDataMap.get(modelDate);
				Integer[] count = dateToGridCount.get(modelDate);
				
				/* Compute average by dividing village level sum with grid count */
				computeAverageSMStressDataForVillage(stressData, count);
			}
			resultMap.put(villageFullName, dateToDataMap);			
		}
		return resultMap;
	}
	
	@Override
	public Map<String, IExtendedRainfallForecastData> getMultiSourcedRainfallForecast(int modelDate, List<String> rfLocUUID) {		
		
		/* 
		 * 1. Gets all weatherForecastData at rainfall staion level from ISRO.
		 * 2. Gets {@link IRainfallForecast} data from APSDPS
		 * 3. Prepare {@link ICSRFForecastData} for crop stress computation
		 *     Use rf forecast value from {@link IWeatherForecast} if available for a rfLocUUID and rfForecastDate
		 *     else use {@link IRainfallForecast}	
		 *     else return NO_DATA	
		 */ 

		Map<String, IExtendedRainfallForecastData> rainfallForecastDataMap = new HashMap<String, IExtendedRainfallForecastData>();
		List<Integer> modelDatesList = DateUtils.addNDaysFromModelDate(modelDate, IWMConstants.SM_RF_DEFAULT_ISRO_FORECAST_DAYS);
		
		/*
		 * 1. Get rainfall forecast data from ISRO/WeatherForecast service
		 * 2. Get rainfall forecast data from APSDPS
		 */
		Map<String, Map<Integer, IWeatherForecast>> rfStationWeatherForecastData = null;
		Map<String, IRainfallForecast> apsdpsRainfallForecastData = null;
		
		try {
			
			apsdpsRainfallForecastData = rfForecastService.getForecastDataForStress(modelDate);
			rfStationWeatherForecastData = weatherForecastService.getNDaysRFStationLevelForecast(
					DateUtils.getModelDateInMillis(modelDate), IWMConstants.SM_RF_DEFAULT_ISRO_FORECAST_DAYS);
			
		} catch (DSPException | ObjectNotFoundException | ParseException e) {
			e.printStackTrace();
			System.out.println("SMStressService : getMultiSourcedRainfallForecast - Exception "
					+ "encountered while fetching rainfall forecast data from APSDPS or ISRO, "  + e.getMessage());
		}
		
		/* If data is not present from both the sources, then 
		 * return empty map */
		if (rfStationWeatherForecastData == null && apsdpsRainfallForecastData == null) {
			if (IS_DEBUG_ENABLED) {
				System.out.println("SMStressService : getMultiSourcedRainfallForecast - "
						+ " no forecast is available for date = " + modelDate);
			}
			return rainfallForecastDataMap;
		}
		
		/*
		 * Else if there is data from any one of the sources, make the 
		 * other as empty instead of null to avoid null pointer exceptions 
		 * during computation in the following for-loop.
		 */
		if (rfStationWeatherForecastData == null) {
			rfStationWeatherForecastData = new HashMap<>();
		}
		if (apsdpsRainfallForecastData == null) {
			apsdpsRainfallForecastData = new HashMap<>();
		}
		
		for(String locUUID : rfLocUUID) {		 
				Map<Integer, IWeatherForecast> weatherForecastMap = rfStationWeatherForecastData.get(locUUID);
				IRainfallForecast rfForecast = apsdpsRainfallForecastData.get(locUUID);		
				boolean isRFForecastAvailable = true;	
				boolean isWForecastAvailable = true;
				
				if(weatherForecastMap == null || weatherForecastMap.isEmpty()) {
					isWForecastAvailable = false;
				}
				
				if(rfForecast == null) {		
					isRFForecastAvailable = false;	
					/* 
					 * If both the sources doesn't has the forecast data, then continue 
					 * */
					if(!isWForecastAvailable) {		
						if (IS_DEBUG_ENABLED) {
							System.out.println("SMStressService : getMultiSourcedRainfallForecast - "
									+ "Forecast not available for - " + locUUID);
						}
						continue;		
					}		
				}
				
				IExtendedRainfallForecastData commonForecastData = new ExtendedRainfallForecastData();		
				commonForecastData.setLocationUUID(locUUID);		
				commonForecastData.setForecastDay(modelDate);
				
				/*
				 * 1. APSDPS has rainfall forecast starting from today
				 * 2. ISRO Weather Forecast has data starting from tomorrow
				 * 3. Adjust accordingly
				 * 
				 * Use Current rainfall explicitly for Day-1 for today's forecast, 
				 * if APSDPS is not needed. 
				 */
				
				if(isRFForecastAvailable && rfForecast.getDay1() >= 0) {
					commonForecastData.setDay1(rfForecast.getDay1());		
					commonForecastData.setDataSourceDay1(DataSourceConstants.APSDPS); 		
				} else {		
					commonForecastData.setDay1(DSPConstants.NO_DATA);		
					commonForecastData.setDataSourceDay1(DataSourceConstants.NO_SOURCE); 		
				}

				/*
				 * Day 2nd to Day 7th
				 */
				if(isWForecastAvailable && modelDatesList.size() > 0 && weatherForecastMap.containsKey(modelDatesList.get(0))
						&& weatherForecastMap.get(modelDatesList.get(0)).getRf() >= 0) {		
					commonForecastData.setDay2(weatherForecastMap.get(modelDatesList.get(0)).getRf());		
					commonForecastData.setDataSourceDay2(DataSourceConstants.ISRO); 		
				} else {	
					if(isRFForecastAvailable && rfForecast.getDay2() >= 0) {		
						commonForecastData.setDay2(rfForecast.getDay2());		
						commonForecastData.setDataSourceDay2(DataSourceConstants.APSDPS); 		
					} else {		
						commonForecastData.setDay2(DSPConstants.NO_DATA);		
						commonForecastData.setDataSourceDay2(DataSourceConstants.NO_SOURCE); 		
					}		
				}
						
				if(isWForecastAvailable && modelDatesList.size() > 1 && weatherForecastMap.containsKey(modelDatesList.get(1))
						&& weatherForecastMap.get(modelDatesList.get(1)).getRf() >= 0) {		
					commonForecastData.setDay3(weatherForecastMap.get(modelDatesList.get(1)).getRf());		
					commonForecastData.setDataSourceDay3(DataSourceConstants.ISRO); 		
				} else {		
					if(isRFForecastAvailable && rfForecast.getDay3() >= 0) {		
						commonForecastData.setDay3(rfForecast.getDay3());		
						commonForecastData.setDataSourceDay3(DataSourceConstants.APSDPS); 		
					} else {		
						commonForecastData.setDay3(DSPConstants.NO_DATA);		
						commonForecastData.setDataSourceDay3(DataSourceConstants.NO_SOURCE); 		
					}		
				}		
				
				if(isWForecastAvailable && modelDatesList.size() > 2 && weatherForecastMap.containsKey(modelDatesList.get(2))
						&& weatherForecastMap.get(modelDatesList.get(2)).getRf() >= 0) {		
					commonForecastData.setDay4(weatherForecastMap.get(modelDatesList.get(2)).getRf());		
					commonForecastData.setDataSourceDay4(DataSourceConstants.ISRO); 		
				} else {		
					if(isRFForecastAvailable && rfForecast.getDay4() >= 0) {		
						commonForecastData.setDay4(rfForecast.getDay4());		
						commonForecastData.setDataSourceDay4(DataSourceConstants.APSDPS); 		
					} else {		
						commonForecastData.setDay4(DSPConstants.NO_DATA);		
						commonForecastData.setDataSourceDay4(DataSourceConstants.NO_SOURCE); 		
					}		
				}		
				
				
				if(isWForecastAvailable && modelDatesList.size() > 3 && weatherForecastMap.containsKey(modelDatesList.get(3))
						&& weatherForecastMap.get(modelDatesList.get(3)).getRf() >= 0) {	
					commonForecastData.setDay5(weatherForecastMap.get(modelDatesList.get(3)).getRf());		
					commonForecastData.setDataSourceDay5(DataSourceConstants.ISRO); 		
				} else {		
					if(isRFForecastAvailable && rfForecast.getDay5() >= 0) {		
						commonForecastData.setDay5(rfForecast.getDay5());		
						commonForecastData.setDataSourceDay5(DataSourceConstants.APSDPS); 		
					} else {		
						commonForecastData.setDay5(DSPConstants.NO_DATA);		
						commonForecastData.setDataSourceDay5(DataSourceConstants.NO_SOURCE); 		
					}		
				}		

				if(isWForecastAvailable && modelDatesList.size() > 4 && weatherForecastMap.containsKey(modelDatesList.get(4))
						&& weatherForecastMap.get(modelDatesList.get(4)).getRf() >= 0) {
					commonForecastData.setDay6(weatherForecastMap.get(modelDatesList.get(4)).getRf());		
					commonForecastData.setDataSourceDay6(DataSourceConstants.ISRO); 		
				} else {		
					if(isRFForecastAvailable && rfForecast.getDay6() >= 0) {		
						commonForecastData.setDay6(rfForecast.getDay6());		
						commonForecastData.setDataSourceDay6(DataSourceConstants.APSDPS); 		
					} else {		
						commonForecastData.setDay6(DSPConstants.NO_DATA);		
						commonForecastData.setDataSourceDay6(DataSourceConstants.NO_SOURCE); 		
					}		
				}		

				if(isWForecastAvailable && modelDatesList.size() > 5 && weatherForecastMap.containsKey(modelDatesList.get(5))
						&& weatherForecastMap.get(modelDatesList.get(5)).getRf() >= 0) {
					commonForecastData.setDay7(weatherForecastMap.get(modelDatesList.get(5)).getRf());		
					commonForecastData.setDataSourceDay7(DataSourceConstants.ISRO); 		
				} else {		
					if(isRFForecastAvailable && rfForecast.getDay7() >= 0) {		
						commonForecastData.setDay7(rfForecast.getDay7());		
						commonForecastData.setDataSourceDay7(DataSourceConstants.APSDPS); 		
					} else {		
						commonForecastData.setDay7(DSPConstants.NO_DATA);		
						commonForecastData.setDataSourceDay7(DataSourceConstants.NO_SOURCE); 		
					}		
				}		

				rainfallForecastDataMap.put(locUUID, commonForecastData);		
			} 		
		
		return rainfallForecastDataMap;		
	}
	
	/**
	 * This method adds the fields of child {@link ISoilMoistureStressData} 
	 * to the corresponding parent {@link ISoilMoistureStressData} fields.
	 * It also increments the corresponding count of each field in the input Integer array.
	 * 
	 * NOTE : This method is used only if the parent-level is either Mandal or District
	 * 
	 * @param childStressData {@link ISoilMoistureStressData}
	 * @param parentStressData {@link ISoilMoistureStressData}
	 * @param parentToChildCount - An Integer array whose indexes holds 
	 * the 'child-data count' for the attributes of {@link ISoilMoistureStressData}
	 */
	protected void addSoilMoistureStressData(ISoilMoistureStressData childStressData, 
			ISoilMoistureStressData parentStressData, Integer[] parentToChildCount) {
		
		double rootZoneAvailableSMInMM = childStressData.getRootZoneAvailableSoilMoistureInMM();
		double availableSMInTMC = childStressData.getAvailableSoilMoistureInTMC();
		double rootzoneAvailableSMPer = childStressData.getAverageSMRootZone();
		double availableSMPercent = childStressData.getSoilMoisturePercent();
		double availableSM150cm = childStressData.getAverageSM15m();
		double residualSM = childStressData.getResidualSM();
		
		/* 
		 * Store the list of Soil Moisture percent of the Villages (used in UI for filtering) 
		 * at both Mandal and District levels
		 * */
		if (childStressData.getChildSMPercentList() != null && !childStressData.getChildSMPercentList().isEmpty()) {
			/* This happens at District or higher levels */
			parentStressData.getChildSMPercentList().addAll(childStressData.getChildSMPercentList());
		}
		else {
			/* This happens at Mandal level */
			parentStressData.getChildSMPercentList().add(rootzoneAvailableSMPer);
		}
		
		if (availableSMPercent >= 0) {
			parentStressData.setSoilMoisturePercent(parentStressData.getSoilMoisturePercent() + availableSMPercent);
			parentToChildCount[IWMConstants.SM_AVLBL_SM_PERCENT_INDEX]++;
		}
		
		if (availableSM150cm >= 0) {
			parentStressData.setAverageSM15m(parentStressData.getAverageSM15m() + availableSM150cm);
			parentToChildCount[IWMConstants.SM_AVLBL_SM_PERCENT_150CM_INDEX]++;
		}
		
		if (rootZoneAvailableSMInMM >= 0) {
			parentStressData.setRootZoneAvailableSoilMoistureInMM(parentStressData.getRootZoneAvailableSoilMoistureInMM() + rootZoneAvailableSMInMM);
			parentToChildCount[IWMConstants.SM_AVLBL_SM_ROOTZONE_IN_MM_INDEX]++;
		}
		
		if (rootzoneAvailableSMPer >= 0) {
			parentStressData.setAverageSMRootZone(parentStressData.getAverageSMRootZone() + rootzoneAvailableSMPer);
			parentToChildCount[IWMConstants.SM_AVLBL_SM_PERCENT_ROOTZONE_INDEX]++;
		}

		if (residualSM != DSPConstants.NO_DATA) {
			parentStressData.setResidualSM(parentStressData.getResidualSM() + residualSM);
			parentToChildCount[IWMConstants.SM_RESIDUAL_SM_INDEX]++;
		}
		
		if (availableSMInTMC >= 0) {
			parentStressData.setAvailableSoilMoistureInTMC(parentStressData.getAvailableSoilMoistureInTMC() + availableSMInTMC);
			parentToChildCount[IWMConstants.SM_AVLBL_SM_IN_TMC_INDEX]++;
		}
	}
	
	/**
	 * This method computes averages of the {@link ISoilMoistureStressData} fields
	 * by dividing them with the corresponding count of each field from the input 
	 * Integer array.<br><br>
	 * 
 	 * <b>NOTE : </b><br>
	 * At village level residualSM and availableSMinTMC are computed as average over 
	 * grid's residualSM and availableSMinTMC, respectively. But for the District and 
	 * Mandal Levels, they should be just summed up.
	 * 
	 * This method is used only if the parent-level is either Mandal or District
	 * 
	 * @param parentStressData {@link ISoilMoistureStressData}
	 * @param parentToChildCount - An Integer array whose indexes holds 
	 * the 'child-data count' for the attributes of {@link ISoilMoistureStressData}
	 */
	protected void computeAverageSMStressData(ISoilMoistureStressData parentStressData, Integer[] parentToChildCount) {
		/*
		 * If data is not available, then use IWMConstants.NO_DATA 
		 * to explicitly indicate 'No Data' condition
		 */
		
		if (parentToChildCount[IWMConstants.SM_AVLBL_SM_PERCENT_INDEX] > 0) {
			parentStressData.setSoilMoisturePercent(parentStressData.getSoilMoisturePercent() / parentToChildCount[IWMConstants.SM_AVLBL_SM_PERCENT_INDEX]);
		} else {
			parentStressData.setSoilMoisturePercent(DSPConstants.NO_DATA);
		}
		
		if (parentToChildCount[IWMConstants.SM_AVLBL_SM_PERCENT_150CM_INDEX] > 0) {
			parentStressData.setAverageSM15m(parentStressData.getAverageSM15m() / parentToChildCount[IWMConstants.SM_AVLBL_SM_PERCENT_150CM_INDEX]);
		} else {
			parentStressData.setAverageSM15m(DSPConstants.NO_DATA);
		}

		if (parentToChildCount[IWMConstants.SM_AVLBL_SM_PERCENT_ROOTZONE_INDEX] > 0) {
			parentStressData.setAverageSMRootZone((parentStressData.getAverageSMRootZone() / parentToChildCount[IWMConstants.SM_AVLBL_SM_PERCENT_ROOTZONE_INDEX]));
		} else {
			parentStressData.setAverageSMRootZone(DSPConstants.NO_DATA);
		}
		
		if (parentToChildCount[IWMConstants.SM_AVLBL_SM_ROOTZONE_IN_MM_INDEX] > 0) {
			parentStressData.setRootZoneAvailableSoilMoistureInMM(parentStressData.getRootZoneAvailableSoilMoistureInMM() / parentToChildCount[IWMConstants.SM_AVLBL_SM_ROOTZONE_IN_MM_INDEX]);
		} else {
			parentStressData.setRootZoneAvailableSoilMoistureInMM(DSPConstants.NO_DATA);
		}
		
		if (parentToChildCount[IWMConstants.SM_RESIDUAL_SM_INDEX] == 0) {
			parentStressData.setResidualSM(DSPConstants.NO_DATA);
		}
		
		if (parentToChildCount[IWMConstants.SM_AVLBL_SM_IN_TMC_INDEX] == 0) {
			parentStressData.setAvailableSoilMoistureInTMC(DSPConstants.NO_DATA);
		}
	}
	
	/**
	 * This method computes averages of the {@link ISoilMoistureStressData} fields
	 * by dividing them with the corresponding count of each field from the input 
	 * Integer array.<br><br>
	 * 
 	 * <b>NOTE : </b><br>
	 * At village level residualSM and availableSMinTMC are computed as average over 
	 * grid's residualSM and availableSMinTMC, respectively. But for the District and 
	 * Mandal Levels, they should be just summed up.
	 * 
	 * This method is used only if the parent-level is Village.
	 * 
	 * @param parentStressData {@link ISoilMoistureStressData}
	 * @param parentToChildCount - An Integer array whose indexes holds 
	 * the 'child-data count' for the attributes of {@link ISoilMoistureStressData}
	 */
	protected void computeAverageSMStressDataForVillage(ISoilMoistureStressData parentStressData, Integer[] parentToChildCount) {
		
		computeAverageSMStressData(parentStressData, parentToChildCount);
		
		if (parentToChildCount[IWMConstants.SM_RESIDUAL_SM_INDEX] > 0) {
			parentStressData.setResidualSM(parentStressData.getResidualSM() / parentToChildCount[IWMConstants.SM_RESIDUAL_SM_INDEX]);
		}
		
		if (parentToChildCount[IWMConstants.SM_AVLBL_SM_IN_TMC_INDEX] > 0) {
			parentStressData.setAvailableSoilMoistureInTMC(parentStressData.getAvailableSoilMoistureInTMC() / parentToChildCount[IWMConstants.SM_AVLBL_SM_IN_TMC_INDEX]);
		}
	}
	
	/**
	 * This method adds the fields of child {@link ISoilMoistureStressData} 
	 * to the corresponding parent {@link ISoilMoistureStressData} fields.
	 * It also increments the corresponding count of each field in the input Integer array.
	 * 
	 * NOTE : This method is used only if the parent-level is Village
	 * 
	 * @param childStressData {@link ISoilMoistureStressData}
	 * @param parentStressData {@link ISoilMoistureStressData}
	 * @param parentToChildCount - An Integer array whose indexes holds 
	 * the 'child-data count' for the attributes of {@link ISoilMoistureStressData}
 	 * @param currSMContent - A double value representing the current soil moisture content in TMC
	 * @param june1stSMContent - A double value representing the June1st soil moisture content in TMC
	 */
	protected void addSoilMoistureStressDataForVillage(ISoilMoistureStressData childStressData, 
			ISoilMoistureStressData parentStressData, Integer[] parentToChildCount, double currSMContent, double june1stSMContent) {
		
		childStressData.setResidualSM(DSPConstants.NO_DATA);
		addSoilMoistureStressData(childStressData, parentStressData, parentToChildCount);
		
		if (june1stSMContent >= 0 && currSMContent >= 0) {
			double residualSMRootZone = currSMContent - june1stSMContent;
			parentStressData.setResidualSM(parentStressData.getResidualSM() + residualSMRootZone);
			parentToChildCount[IWMConstants.SM_RESIDUAL_SM_INDEX]++;
		}
	}
	
	/**
	 * This method computes averages of the {@link ISMRainfallStressData} fields
	 * by dividing them with the corresponding count of each field from the input 
	 * Integer array.<br><br>
	 * 
	 * @param parentStressData {@link ISMRainfallStressData}
	 * @param parentToChildCount - An Integer array whose indexes holds 
	 * the 'child-data count' for the attributes of {@link ISMRainfallStressData}
	 */
	protected void computeAverageRainfallStressData(ISMRainfallStressData parentStressData, Integer[] parentToChildCount) {
		/* 
		 * If data is not available, then use IWMConstants.NO_DATA 
		 * to explicitly indicate 'No Data' condition 
		 * 
		 * */
		if (parentToChildCount[IWMConstants.SM_RF_LAST7DAYS_INDEX] > 0) {
			parentStressData.setLast7DaysCumulative(parentStressData.getLast7DaysCumulative() / parentToChildCount[IWMConstants.SM_RF_LAST7DAYS_INDEX]);
		}
		else {
			parentStressData.setLast7DaysCumulative(DSPConstants.NO_DATA);
		}
		
		if (parentToChildCount[IWMConstants.SM_RF_LAST3DAYS_INDEX] > 0) {
			parentStressData.setLast3DaysCumulative(parentStressData.getLast3DaysCumulative() / parentToChildCount[IWMConstants.SM_RF_LAST3DAYS_INDEX]);
		}
		else {
			parentStressData.setLast3DaysCumulative(DSPConstants.NO_DATA);
		}
		
		if (parentToChildCount[IWMConstants.SM_RF_TODAY_INDEX] > 0) {
			parentStressData.setTodayActual(parentStressData.getTodayActual() / parentToChildCount[IWMConstants.SM_RF_TODAY_INDEX]);
		}
		else {
			parentStressData.setTodayActual(DSPConstants.NO_DATA);
		}
		
		if (parentToChildCount[IWMConstants.SM_RF_YESTERDAY_INDEX] > 0) {
			parentStressData.setYesterdayActual(parentStressData.getYesterdayActual() / parentToChildCount[IWMConstants.SM_RF_YESTERDAY_INDEX]);
		}
		else {
			parentStressData.setYesterdayActual(DSPConstants.NO_DATA);
		}
		
		if (parentToChildCount[IWMConstants.SM_RF_24HRS_FORECAST_INDEX] > 0) {
			parentStressData.setNext24HrForecast(parentStressData.getNext24HrForecast() / parentToChildCount[IWMConstants.SM_RF_24HRS_FORECAST_INDEX]);
		}
		else {
			parentStressData.setNext24HrForecast(DSPConstants.NO_DATA);
		}
		
		if (parentToChildCount[IWMConstants.SM_RF_48HRS_FORECAST_INDEX] > 0) {
			parentStressData.setNext48HrForecast(parentStressData.getNext48HrForecast() / parentToChildCount[IWMConstants.SM_RF_48HRS_FORECAST_INDEX]);
		} 
		else {
			parentStressData.setNext48HrForecast(DSPConstants.NO_DATA);
		}
		
		if (parentToChildCount[IWMConstants.SM_RF_NON_RAINY_DAYS_INDEX] > 0) {
			parentStressData.setNoOfNonRainyDays(Math.round( ((float) parentStressData.getNoOfNonRainyDays()) / parentToChildCount[IWMConstants.SM_RF_NON_RAINY_DAYS_INDEX]));
		}
		else {
			parentStressData.setNoOfNonRainyDays(-1);
		}
		
		if (parentToChildCount[IWMConstants.SM_RF_LAST10DAYS_INDEX] > 0) {
			parentStressData.setLast10DaysCumulative(parentStressData.getLast10DaysCumulative() / parentToChildCount[IWMConstants.SM_RF_LAST10DAYS_INDEX]);
		}
		else {
			parentStressData.setLast10DaysCumulative(DSPConstants.NO_DATA);
		}
		
		if (parentToChildCount[IWMConstants.SM_RF_7DAYS_FORECAST_INDEX] > 0) {
			parentStressData.setNext7DaysForecastCumulative(parentStressData.getNext7DaysForecastCumulative() / parentToChildCount[IWMConstants.SM_RF_7DAYS_FORECAST_INDEX]);
		} 
		else {
			parentStressData.setNext7DaysForecastCumulative(DSPConstants.NO_DATA);
		}
		
		/*
		 * JIRA : SMCS-1, Get Rainfall since yesterday
		 * 
		 * Since we have computed final rainfall data value  
		 * for today and yesterday, we can simply add them up.
		 */
		boolean rfDataPresentSinceYesterday = false;
		
		if (parentStressData.getTodayActual() >= 0) {
			parentStressData.setRainfallSinceYesterday(parentStressData.getTodayActual());
			rfDataPresentSinceYesterday = true;
		}
		if (parentStressData.getYesterdayActual() >= 0) {
			parentStressData.setRainfallSinceYesterday(parentStressData.getRainfallSinceYesterday() + parentStressData.getYesterdayActual());
			rfDataPresentSinceYesterday = true;
		}

		if (!rfDataPresentSinceYesterday) {
			parentStressData.setRainfallSinceYesterday(DSPConstants.NO_DATA);
		}
	}

	/**
	 * 
 	 * This method adds the fields of child {@link ISMRainfallStressData} 
	 * to the corresponding parent {@link ISMRainfallStressData} fields.
	 * It also increments the corresponding count of each field in the input Integer array.
	 * 
	 * @param childStressData {@link ISMRainfallStressData}
	 * @param parentStressData {@link ISMRainfallStressData}
	 * @param parentToChildCount - An Integer array whose indexes holds 
	 * the 'child-data count' for the attributes of {@link ISMRainfallStressData}
	 */
	protected void addRainfallStressData(ISMRainfallStressData childStressData, ISMRainfallStressData parentStressData, 
			Integer[] parentToChildCount) {
		
		parentStressData.getChildNoOfNonRainyDaysList().add(childStressData.getNoOfNonRainyDays());

		if (childStressData.getLast7DaysCumulative() >= 0) {
			parentStressData.setLast7DaysCumulative(parentStressData.getLast7DaysCumulative() + childStressData.getLast7DaysCumulative());
			parentToChildCount[IWMConstants.SM_RF_LAST7DAYS_INDEX]++;
		}
		if (childStressData.getLast3DaysCumulative() >= 0) {
			parentStressData.setLast3DaysCumulative(parentStressData.getLast3DaysCumulative() + childStressData.getLast3DaysCumulative());
			parentToChildCount[IWMConstants.SM_RF_LAST3DAYS_INDEX]++;
		}
		if (childStressData.getTodayActual() >= 0) {
			parentStressData.setTodayActual(parentStressData.getTodayActual() + childStressData.getTodayActual());
			parentToChildCount[IWMConstants.SM_RF_TODAY_INDEX]++;
		}
		if (childStressData.getYesterdayActual() >= 0) {
			parentStressData.setYesterdayActual(parentStressData.getYesterdayActual() + childStressData.getYesterdayActual());
			parentToChildCount[IWMConstants.SM_RF_YESTERDAY_INDEX]++;
		}
		if (childStressData.getNext24HrForecast() >=  0) {
			parentStressData.setNext24HrForecast(parentStressData.getNext24HrForecast() + childStressData.getNext24HrForecast());	
			parentToChildCount[IWMConstants.SM_RF_24HRS_FORECAST_INDEX]++;
		}
		if (childStressData.getNext48HrForecast() >=  0) {
			parentStressData.setNext48HrForecast(parentStressData.getNext48HrForecast() + childStressData.getNext48HrForecast());	
			parentToChildCount[IWMConstants.SM_RF_48HRS_FORECAST_INDEX]++;
		}
		if (childStressData.getNoOfNonRainyDays() >=  0) {
			parentStressData.setNoOfNonRainyDays(parentStressData.getNoOfNonRainyDays() + childStressData.getNoOfNonRainyDays());	
			parentToChildCount[IWMConstants.SM_RF_NON_RAINY_DAYS_INDEX]++;
		}
		if (childStressData.getLast10DaysCumulative() >= 0) {
			parentStressData.setLast10DaysCumulative(parentStressData.getLast10DaysCumulative() + childStressData.getLast10DaysCumulative());
			parentToChildCount[IWMConstants.SM_RF_LAST10DAYS_INDEX]++;
		}
		if (childStressData.getNext7DaysForecastCumulative() >=  0) {
			parentStressData.setNext7DaysForecastCumulative(parentStressData.getNext7DaysForecastCumulative() + childStressData.getNext7DaysForecastCumulative());	
			parentToChildCount[IWMConstants.SM_RF_7DAYS_FORECAST_INDEX]++;
		}
	}
	
}