package com.vassarlabs.iwm.dssnew.wsc.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.vassarlabs.common.dsp.err.DSPException;
import com.vassarlabs.common.dsp.err.FetchingObjectException;
import com.vassarlabs.common.dsp.utils.DSPConstants;
import com.vassarlabs.common.ext.api.pojo.IExtensionValue;
import com.vassarlabs.common.utils.DateUtils;
import com.vassarlabs.common.utils.err.ObjectNotFoundException;
import com.vassarlabs.eventmapper.utils.EventConstants;
import com.vassarlabs.ext.utils.ExtensionConstants;
import com.vassarlabs.iwm.dsp.api.IIWMSDSP;
import com.vassarlabs.iwm.dss.wcs.pojo.impl.WSCData;
import com.vassarlabs.iwm.dss.wcs.pojo.impl.WSCRainfallData;
import com.vassarlabs.iwm.dss.wcs.pojo.impl.WSCReservoirData;
import com.vassarlabs.iwm.dssnew.wcs.pojo.api.ILocationStructureInfo;
import com.vassarlabs.iwm.dssnew.wcs.pojo.api.IWSCData;
import com.vassarlabs.iwm.dssnew.wcs.pojo.api.IWSCRainfallData;
import com.vassarlabs.iwm.dssnew.wcs.pojo.api.IWSCReservoirData;
import com.vassarlabs.iwm.dssnew.wsc.service.api.IWaterShedConservationService;
import com.vassarlabs.iwm.dssnew.wsc.service.api.IWaterStructMasterDataService;
import com.vassarlabs.iwm.pojo.api.IWaterQuantityEventData;
import com.vassarlabs.iwm.pojo.impl.WaterQuantityEventData;
import com.vassarlabs.iwm.reservoir.cache.api.IReservoirIWMCache;
import com.vassarlabs.iwm.service.api.IIWMService;
import com.vassarlabs.iwm.soilmoisture.stress.service.api.ISoilMoistureNRSCDataService;
import com.vassarlabs.iwm.utils.IWMConstants;
import com.vassarlabs.iwm.utils.SourceTypeConstants;
import com.vassarlabs.location.service.api.ILocationHierarchyService;
import com.vassarlabs.location.utils.LocationConstants;

/**
 * 
 * @author zubair
 */
@Component
public class WaterShedConservationService implements IWaterShedConservationService {

	@Autowired
	IIWMService iwmService;
	
	@Autowired
	protected IIWMSDSP iwmsDSP;

	@Autowired
	ISoilMoistureNRSCDataService nrscDataService;

	@Autowired
	ILocationHierarchyService locationHierarchyService;

	@Autowired
	IWaterStructMasterDataService waterStructureMasterService;

	@Autowired
	ILocationHierarchyService locService;

	@Autowired
	private IReservoirIWMCache resCache;

	@Override
	public Map<String, IWSCData> getAllWSCDataBasinView(int startDate, int endDate, int bucketSize,
			Map<Long, Map<Integer, Double>> allGridLevelRunoffDataMap) throws DSPException {

		Map<String, IWSCData> resultMap = new HashMap<>();

		/*
		 * 1. Get state to basin mappings 2. Get all basin to sub-basin mappings
		 * 3. Get all sub-basin to micro-basin mappings 4. Get WSCData for all
		 * locations at micro-basin level
		 * 
		 * 5. Iterate over each basin and compute the state level data 6.
		 * Iterate over each sub-basin under the basin to compute basin level
		 * data 7. Iterate over each micro-basin under the sub-basin to compute
		 * sub-basin level data
		 */

		Map<String, List<String>> stateToBasinUUIDMap = locationHierarchyService
				.getAllLocForParentChildTypes(LocationConstants.STATE, LocationConstants.BASIN);
		Map<String, List<String>> basinToSubBasinUUIDMap = locationHierarchyService
				.getAllLocForParentChildTypes(LocationConstants.BASIN, LocationConstants.SUBBASIN);
		Map<String, List<String>> subBasinToMicroBasinUUIDMap = locationHierarchyService
				.getAllLocForParentChildTypes(LocationConstants.SUBBASIN, LocationConstants.MICROBASIN);

		/*
		 * 4a. Prepare a Map from microBasinName to their fullnames 4b. Get
		 * micro-basin level WSC data (using the above map)
		 */
		Map<String, String> microBasinNameToFullNameMap = prepareMicroBasinNameToFullNameMapping(stateToBasinUUIDMap,
				basinToSubBasinUUIDMap, subBasinToMicroBasinUUIDMap);
		Map<String, IWSCData> microBasinDataMap = getAllWSCDataForMicroBasin(startDate, endDate, bucketSize,
				microBasinNameToFullNameMap, allGridLevelRunoffDataMap);
		// Map<String, IWSCData> microBasinDataMapForReservoir =
		// getAllReservoirDataForMicroBasin(refDate);

		/*
		 * Step 5 to 7
		 */
		for (String stateUUID : stateToBasinUUIDMap.keySet()) {
			String stateLocName = locationHierarchyService.getLocNameForLocUUID(stateUUID);

			IWSCData stateWSCData = new WSCData();
			stateWSCData.setLocationName(stateLocName);
			stateWSCData.setLocationFullName(stateLocName);

			/* Get the list of Basin Locations under the state */
			List<String> basinUUIDList = stateToBasinUUIDMap.get(stateUUID);

			if (basinUUIDList == null) {
				basinUUIDList = new ArrayList<>();
			}

			/*
			 * To store the total number of basin locations for which data is
			 * available. This total will be used to indicate whether the state
			 * level data is partial, complete, or NA
			 * 
			 */
			MultiBoolean stateToBasinDataIndicator = new MultiBoolean();

			for (String basinUUID : basinUUIDList) {
				String basinLocName = locationHierarchyService.getLocNameForLocUUID(basinUUID);
				String basinFullName = stateLocName + IWMConstants.DOUBLE_HASH_DELIMITER + basinLocName;

				IWSCData basinWSCData = new WSCData();
				basinWSCData.setLocationName(basinLocName);
				basinWSCData.setLocationFullName(basinFullName);

				/* Get the list of Sub-Basin Locations under the Basin */
				List<String> subBasinUUIDList = basinToSubBasinUUIDMap.get(basinUUID);

				if (subBasinUUIDList == null) {
					subBasinUUIDList = new ArrayList<>();
				}

				/*
				 * To store the total number of sub-basin locations for which
				 * data is available. This total will be used to indicate
				 * whether the basin level data is partial, complete, or NA
				 * 
				 */
				MultiBoolean basinToSubBasinDataIndicator = new MultiBoolean();

				for (String subBasinUUID : subBasinUUIDList) {
					String subBasinLocName = locationHierarchyService.getLocNameForLocUUID(subBasinUUID);
					String subBasinLocFullName = basinFullName + IWMConstants.DOUBLE_HASH_DELIMITER + subBasinLocName;

					IWSCData subBasinWSCData = new WSCData();
					subBasinWSCData.setLocationName(subBasinLocName);
					subBasinWSCData.setLocationFullName(subBasinLocFullName);

					/* Get the list of Sub-Basin Locations under the Basin */
					List<String> microBasinUUIDList = subBasinToMicroBasinUUIDMap.get(subBasinUUID);

					if (microBasinUUIDList == null) {
						microBasinUUIDList = new ArrayList<>();
					}

					/*
					 * To store the total number of micro-basin locations for
					 * which data is available. This total will be used to
					 * indicate whether the basin level data is partial,
					 * complete, or NA
					 * 
					 */
					MultiBoolean subBasinToMBDataIndicator = new MultiBoolean();

					for (String microBasinUUID : microBasinUUIDList) {

						String microBasinLocName = locationHierarchyService.getLocNameForLocUUID(microBasinUUID);
						String microBasinLocFullName = subBasinLocFullName + IWMConstants.DOUBLE_HASH_DELIMITER
								+ microBasinLocName;

						IWSCData microBasinWSCData = microBasinDataMap.get(microBasinLocFullName);

						if (microBasinWSCData == null) {
							microBasinWSCData = new WSCData();
							assignNoDataToAttributes(microBasinWSCData);
							microBasinWSCData.setLocationName(microBasinLocName);
						}
						microBasinWSCData.setLocationFullName(microBasinLocFullName);

						/*
						 * a. Sum up the micro-basin values to get the sub-basin
						 * level sum b. Put the micro-basin level into the
						 * result map
						 * 
						 */
						addWSCData(subBasinWSCData, microBasinWSCData, subBasinToMBDataIndicator);
						resultMap.put(microBasinLocFullName, microBasinWSCData);
					}

					/*
					 * a. Validate computed sum of sub-basin level data b. Sum
					 * up the sub-basin values to get the basin level sum c. Put
					 * the sub-basin level into the result map
					 * 
					 */
					validateWCSData(subBasinWSCData, subBasinToMBDataIndicator);
					addWSCData(basinWSCData, subBasinWSCData, basinToSubBasinDataIndicator);
					resultMap.put(subBasinLocFullName, subBasinWSCData);
				}

				/*
				 * a. Validate computed sum of basin-level data b. Sum up the
				 * basin values to get the state level sum c. Put the basin
				 * level into the result map
				 * 
				 */
				validateWCSData(basinWSCData, basinToSubBasinDataIndicator);
				addWSCData(stateWSCData, basinWSCData, stateToBasinDataIndicator);
				resultMap.put(basinFullName, basinWSCData);
			}

			/*
			 * a. Validate computed sum of state-level data b. Put the state
			 * level into the result map
			 * 
			 */
			validateWCSData(stateWSCData, stateToBasinDataIndicator);
			resultMap.put(stateLocName, stateWSCData);
		}

		return resultMap;
	}

	@Override
	public Map<String, IWSCData> getAllWSCDataAdminView(int startDate, int endDate, int bucketSize,
			Map<Long, Map<Integer, Double>> allGridLevelRunoffDataMap) throws DSPException {

		Map<String, IWSCData> resultMap = new HashMap<>();

		/*
		 * 1. Get state to district mappings 2. Get all district to mandal
		 * mappings 4. Get WSCData for all locations at mandal level
		 * 
		 * 5. Iterate over each district and compute the state level data 6.
		 * Iterate over each mandal under the district to compute district level
		 * data
		 */

		Map<String, List<String>> stateToDistrictUUID = locationHierarchyService
				.getAllLocForParentChildTypes(LocationConstants.STATE, LocationConstants.DISTRICT);
		Map<String, List<String>> districtToMandalUUID = locationHierarchyService
				.getAllLocForParentChildTypes(LocationConstants.DISTRICT, LocationConstants.MANDAL);

		/*
		 * a. Prepare a Map from MandalName to their fullnames b. Get Mandal
		 * level WSC data (using the above map)
		 */
		Map<String, String> mandalNameToFullNameMap = prepareMandalNameToFullNameMapping(stateToDistrictUUID,
				districtToMandalUUID);
		Map<String, IWSCData> microBasinDataMap = getAllWSCDataForMandals(startDate, endDate, bucketSize,
				mandalNameToFullNameMap, allGridLevelRunoffDataMap);

		/*
		 * Step 5 to 7
		 */
		for (String stateUUID : stateToDistrictUUID.keySet()) {
			String stateLocName = locationHierarchyService.getLocNameForLocUUID(stateUUID);

			IWSCData stateWSCData = new WSCData();
			stateWSCData.setLocationName(stateLocName);
			stateWSCData.setLocationFullName(stateLocName);

			/* Get the list of District Locations under the state */
			List<String> districtUUIDList = stateToDistrictUUID.get(stateUUID);

			if (districtUUIDList == null) {
				districtUUIDList = new ArrayList<>();
			}

			/*
			 * To store the total number of district locations for which data is
			 * available. This total will be used to indicate whether the state
			 * level data is partial, complete, or NA
			 * 
			 */
			MultiBoolean stateToDistrictDataIndicator = new MultiBoolean();

			for (String districtUUID : districtUUIDList) {
				String districtLocName = locationHierarchyService.getLocNameForLocUUID(districtUUID);
				String districtFullName = stateLocName + IWMConstants.DOUBLE_HASH_DELIMITER + districtLocName;

				IWSCData districtWSCData = new WSCData();
				districtWSCData.setLocationName(districtLocName);
				districtWSCData.setLocationFullName(districtFullName);

				/* Get the list of Mandal Locations under the District */
				List<String> mandalUUIDList = districtToMandalUUID.get(districtUUID);

				if (mandalUUIDList == null) {
					mandalUUIDList = new ArrayList<>();
				}

				/*
				 * To store the total number of mandal locations for which data
				 * is available. This total will be used to indicate whether the
				 * district level data is partial, complete, or NA
				 * 
				 */
				MultiBoolean districtToMandalDataIndicator = new MultiBoolean();

				for (String mandalUUID : mandalUUIDList) {
					String mandalLocName = locationHierarchyService.getLocNameForLocUUID(mandalUUID);
					String mandalLocFullName = districtFullName + IWMConstants.DOUBLE_HASH_DELIMITER + mandalLocName;

					IWSCData mandalWSCData = microBasinDataMap.get(mandalLocFullName);

					if (mandalWSCData == null) {
						mandalWSCData = new WSCData();
						assignNoDataToAttributes(mandalWSCData);
						mandalWSCData.setLocationName(mandalLocName);
					}
					mandalWSCData.setLocationFullName(mandalLocFullName);

					/*
					 * a. Sum up the mandal values to get the district level sum
					 * b. Put the mandal level into the result map
					 * 
					 */
					addWSCData(districtWSCData, mandalWSCData, districtToMandalDataIndicator);
					resultMap.put(mandalLocFullName, mandalWSCData);

				}

				/*
				 * a. Validate computed sum of district level data b. Sum up the
				 * district values to get the state level sum c. Put the
				 * district level into the result map
				 * 
				 */
				validateWCSData(districtWSCData, districtToMandalDataIndicator);
				addWSCData(stateWSCData, districtWSCData, stateToDistrictDataIndicator);
				resultMap.put(districtFullName, districtWSCData);
			}

			/*
			 * a. Validate computed sum of state-level data b. Put the state
			 * level into the result map
			 * 
			 */
			validateWCSData(stateWSCData, stateToDistrictDataIndicator);
			resultMap.put(stateLocName, stateWSCData);
		}

		return resultMap;
	}

	/**
	 * Returns WSCData for all the microbasins
	 * 
	 * Takes in the startDate and endDate ("ddMMYYYY") Takes in the bucket size
	 * to be considered during computation
	 * 
	 * @param startDate
	 * @param endDate
	 * @param bucketSize
	 * @param microBasinLocFullNameMap
	 * @return
	 * @throws DSPException
	 */
	protected Map<String, IWSCData> getAllWSCDataForMicroBasin(int startDate, int endDate, int bucketSize,
			Map<String, String> microBasinLocFullNameMap, Map<Long, Map<Integer, Double>> allGridLevelRunoffDataMap)
			throws DSPException {

		/*
		 * 1. Get all RunOff data at MicroBasin Level 2. Iterate over each of
		 * the micro-basin locations 3. Iterate over the given period in steps
		 * of 'bucket-size' 4. Compute weekly values for Excess-RunOff,
		 * Conserved-RunOff,...
		 * 
		 */

		int startDateCopy = startDate;
		Map<String, IWSCData> resultMap = new HashMap<>();
		Map<String, Map<Integer, Double>> microBasinTorunOffDataMap = nrscDataService
				.getMicroBasinLevelRunOffData(startDate, endDate, allGridLevelRunoffDataMap);

		for (String microBasinName : microBasinLocFullNameMap.keySet()) {

			String microBasinLocFullName = microBasinLocFullNameMap.get(microBasinName);

			/*
			 * 1. Creating an empty POJO and assign it to NO_DATA 2. Add it to
			 * the resultant map
			 */
			IWSCData microBasinWSCData = new WSCData();
			assignNoDataToAttributes(microBasinWSCData);
			microBasinWSCData.setLocationName(microBasinName);
			resultMap.put(microBasinLocFullName, microBasinWSCData);

			/*
			 * Get run-off data for current micro-basin
			 */
			Map<Integer, Double> microBasinDataMap = microBasinTorunOffDataMap.get(microBasinName);

			/*
			 * Get the water structure information to compute total capacity and
			 * total loss under the micro-basin
			 */
			Map<String, ILocationStructureInfo> structInfo = waterStructureMasterService
					.getStructuresDataForLocation(microBasinLocFullName);
			ILocationStructureInfo aggregatedStructInfo = waterStructureMasterService
					.getAggregatedStructDataForLocation(microBasinLocFullName);
			double wStructLoss = waterStructureMasterService.getAllWSLossForLocation(microBasinLocFullName, bucketSize);

			if (structInfo == null || aggregatedStructInfo == null || wStructLoss < 0
			/* || !structInfo.containsKey(MI_TANK) */) {
				continue;
			}

			double totalRunOff = 0;
			double totalExcessRunOff = 0;
			double totalConservedRunOff = 0;
			double wStructCapacity = aggregatedStructInfo.getCapacity();

			/*
			 * TODO : Validate the logic
			 * 
			 * double miTankCapacity = structInfo.get(MI_TANK).getCapacity();
			 * double miTanksFraction = miTankCapacity / (wStructCapacity +
			 * miTankCapacity);
			 */
			double miTanksFraction = IWMConstants.WSC_DEFAULT_MITANK_ALLOCATION_FRACTION;

			/*
			 * Dividing the entire period into slots equal to the bucketSize.
			 * Compute total run-off water conserved by all the water
			 * structures.
			 */
			while (startDate <= endDate) {

				double qtyEndOfWeek = 0.0;
				double qtyStartOfWeek = 0.0;
				double qtyWeeklyRunOff = 0.0;
				double qtyWeeklyExcessRunOff = 0.0;

				/*
				 * 1. Compute week's runoff value by aggregating day-wise NRSC
				 * runoff data 2. Removing fraction of MITanks runoff 3.
				 * Computing excess runoff of the week
				 */
				qtyWeeklyRunOff = getBucketsRunOffData(startDate, endDate, bucketSize, microBasinDataMap);
				qtyWeeklyRunOff -= (miTanksFraction * qtyWeeklyRunOff);
				qtyEndOfWeek = qtyStartOfWeek + qtyWeeklyRunOff - wStructLoss;

				if (qtyEndOfWeek < 0) {
					qtyEndOfWeek = 0.0;
				}

				if (qtyEndOfWeek > wStructCapacity) {
					qtyWeeklyExcessRunOff = qtyEndOfWeek - wStructCapacity;
					qtyEndOfWeek = wStructCapacity;
				}

				/*
				 * 1. Computing Σ(WeeklyRunOff) 2. Computing
				 * Σ(WeeklyExcessRunOff)
				 */
				totalRunOff += qtyWeeklyRunOff;
				totalExcessRunOff += qtyWeeklyExcessRunOff;

				qtyStartOfWeek = qtyEndOfWeek;
				startDate = DateUtils.addNDaysToModelDate(startDate, bucketSize);
			}

			/*
			 * RunOffConserved = Σ(WeeklyRunOff) - Σ(WeeklyExcessRunOff)
			 */
			totalConservedRunOff = totalRunOff - totalExcessRunOff;

			microBasinWSCData.setRunOff(totalRunOff);
			microBasinWSCData.setExcessRunOff(totalExcessRunOff);
			microBasinWSCData.setRunOffCaptured(totalConservedRunOff);
			microBasinWSCData.setTotalStructCount(structInfo.get(LocationConstants.TOTAL_STRUCTURES).getCount());
			microBasinWSCData.setTotalStructCapacity(structInfo.get(LocationConstants.TOTAL_STRUCTURES).getCapacity());

			/*
			 * Reset the startDate to it's original value
			 */
			startDate = startDateCopy;
		}

		return resultMap;
	}

	/**
	 * Returns WSCReservoirData for all the microbasins
	 * 
	 * Takes in the reference Date ("ddMMYYYY") Returns Map of LocationFullName
	 * to WSCReservoirData.
	 * 
	 * @param refDate
	 * @return
	 * @throws DSPException
	 * @throws ObjectNotFoundException
	 */
	protected Map<String, IWSCReservoirData> getWSCReservoirDataForAllMicroBasin(long referenceTs)
			throws DSPException, ObjectNotFoundException {

		long startTs = referenceTs - DateUtils.getLastMonthSameTimeTs(referenceTs);
		
		Map<String, List<String>> microBasinUUIDToReservoirUUIDList = locService
				.getAllLocForParentChildTypes(LocationConstants.MICROBASIN, LocationConstants.RESERVOIR);

		Map<String, IWaterQuantityEventData> reservoirUUIDToDataMap = iwmService.getLastKnownData(
				(int) SourceTypeConstants.RESERVOIR, (int) EventConstants.COMPUTED_EVENT, null, startTs, referenceTs);

		Map<String, IWSCReservoirData> resultMap = new HashMap<>();

		IExtensionValue extension = null;

		for (String microBasin : microBasinUUIDToReservoirUUIDList.keySet()) {

			int totalCount = 0;
			double capacity = 0;
			double currentStorage = 0;

			List<String> reservoirsUnderMicroBasin = microBasinUUIDToReservoirUUIDList.get(microBasin);

			for (String reservoirUUID : reservoirsUnderMicroBasin) {
				
				IWaterQuantityEventData data = reservoirUUIDToDataMap.get(reservoirUUID);

				extension = resCache.getExtension(locService.getLocIdForLocUUID(reservoirUUID),
						ExtensionConstants.RESERVOIR_CAPACITY_EXTENSION, ExtensionConstants.DESIGN_CAPACITY_EXTENSION);
				double grossCapacity = 0;
				if (extension != null && extension.getValue() != null) {
					try {
						grossCapacity = Double.parseDouble(extension.getValue().trim());
					} catch (NumberFormatException e) {
						e.printStackTrace();
					}
				}

				totalCount += 1;

				if (grossCapacity >= 0) {
					capacity += grossCapacity;
				}
				if (currentStorage >= 0) {
					currentStorage += data.getStorageValue1();
				}
			}

			IWSCReservoirData microBasinReservoirData = new WSCReservoirData();

			microBasinReservoirData.setLocationName(locService.getLocNameForLocUUID(microBasin));
			microBasinReservoirData.setTotalNumberOfReservoirs(totalCount);
			microBasinReservoirData.setCapacity(capacity);
			microBasinReservoirData.setCurrentStorage(currentStorage);

			resultMap.put(locService.getLocationNameFromUUID(microBasin), microBasinReservoirData);
		}

		return resultMap;

	}
	/**
	 * Returns WSCReservoirData for all the Mandals
	 * 
	 * @param referenceTs
	 * @return
	 * @throws DSPException
	 * @throws ObjectNotFoundException
	 */
	protected Map<String, IWSCReservoirData> getWSCReservoirDataForAllMandals(long referenceTs)
			throws DSPException, ObjectNotFoundException {

		long startTs = referenceTs - DateUtils.getLastMonthSameTimeTs(referenceTs);
		
		Map<String, List<String>> mandalUUIDToReservoirUUIDList = locService
				.getAllLocForParentChildTypes(LocationConstants.MANDAL, LocationConstants.RESERVOIR);

		Map<String, IWaterQuantityEventData> reservoirUUIDToDataMap = iwmService.getLastKnownData(
				(int) SourceTypeConstants.RESERVOIR, (int) EventConstants.COMPUTED_EVENT, null, startTs, referenceTs);

		Map<String, IWSCReservoirData> resultMap = new HashMap<>();

		IExtensionValue extension = null;

		for (String mandalUUID : mandalUUIDToReservoirUUIDList.keySet()) {

			int totalCount = 0;
			double capacity = 0;
			double currentStorage = 0;

			List<String> reservoirsUnderMandal = mandalUUIDToReservoirUUIDList.get(mandalUUID);

			for (String reservoirUUID : reservoirsUnderMandal) {
				
				IWaterQuantityEventData data = reservoirUUIDToDataMap.get(reservoirUUID);

				extension = resCache.getExtension(locService.getLocIdForLocUUID(reservoirUUID),
						ExtensionConstants.RESERVOIR_CAPACITY_EXTENSION, ExtensionConstants.DESIGN_CAPACITY_EXTENSION);
				double grossCapacity = 0;
				if (extension != null && extension.getValue() != null) {
					try {
						grossCapacity = Double.parseDouble(extension.getValue().trim());
					} catch (NumberFormatException e) {
						e.printStackTrace();
					}
				}

				totalCount += 1;

				if (grossCapacity >= 0) {
					capacity += grossCapacity;
				}
				if (currentStorage >= 0) {
					currentStorage += data.getStorageValue1();
				}
			}

			IWSCReservoirData mandalReservoirData = new WSCReservoirData();
		
			mandalReservoirData.setLocationName(locService.getLocNameForLocUUID(mandalUUID));
			mandalReservoirData.setTotalNumberOfReservoirs(totalCount);
			mandalReservoirData.setCapacity(capacity);
			mandalReservoirData.setCurrentStorage(currentStorage);

			resultMap.put(locService.getLocNameForLocUUID(mandalUUID), mandalReservoirData);
		}

		return resultMap;

	}

	/**
	 * Returns WSCRainfallData for all the microbasins
	 * 
	 * Takes in the reference Date ("ddMMYYYY") 
	 * Returns Map of LocationFullName to WSCRainfallData.
	 * 
	 * @param refDate
	 * @return
	 * @throws DSPException
	 * @throws ObjectNotFoundException
	 */
	protected Map<String, IWSCRainfallData> getWSCRainfallDataForAllMicroBasin(long referenceTs)
			throws DSPException, ObjectNotFoundException {

		/**
		 * 1. Get start of season from given reference date.
		 * 2. Get the map of micro-basin to rainfall stations
		 * 3. Get data normal and actual data for all rainfall stations from june1st
		 * 
		 * 4. For each microbasin location
		 * 5.		Get the list of rainfall stations under the micro-basin
		 * 6.		Iterate over each rainfall station and compute average
		 * 
		 */
		long seasonStartTs = DateUtils.getStartOfMonsoon(referenceTs);
		
		
		Map<String, List<String>> microBasinUUIDToRainfallUUIDList = locService
				.getAllLocForParentChildTypes(LocationConstants.MICROBASIN, LocationConstants.RAINFALL);
	
		/*
		 * TODO : Have to plug in actual services
		 */
		Map<String, Double> allStaionLevelNormalData = getAllStaionLevelNormalData(seasonStartTs, referenceTs, microBasinUUIDToRainfallUUIDList);
		Map<Integer, IWaterQuantityEventData> actualStationLevelData = iwmsDSP.getAverageWQData2(SourceTypeConstants.RAINFALL, EventConstants.COMPUTED_EVENT, 
				LocationConstants.RAINFALL, seasonStartTs, referenceTs);
		
		Map<String, IWSCRainfallData> resultMap = new HashMap<>();
		WaterQuantityEventData rfWQData = null;

		
		for (String microBasinUUID : microBasinUUIDToRainfallUUIDList.keySet()) {

			double normalValue = 0;
			double actualValue = 0;
			
			int normalDataCount = 0;
			int actualDataCount = 0;

			List<String> rfStationsUnderMB = microBasinUUIDToRainfallUUIDList.get(microBasinUUID);
			
			if (rfStationsUnderMB == null) {
				rfStationsUnderMB = new ArrayList<>();
			}
			
			for (String rfLocUUID : rfStationsUnderMB) {
				
				rfWQData = (WaterQuantityEventData) actualStationLevelData.get(locService.getLocIdForLocUUID(rfLocUUID));
				 
				if (rfWQData != null && rfWQData.getLevelValue1() >= 0) {
					actualValue += rfWQData.getLevelValue1();
					actualDataCount++;	
				}
				
				if (allStaionLevelNormalData.containsKey(rfLocUUID)) {
					normalValue += allStaionLevelNormalData.get(rfLocUUID);
					normalDataCount++;					
				}
			}

			IWSCRainfallData microBasinRainfallData = new WSCRainfallData();
			microBasinRainfallData.setLocationName(locService.getLocNameForLocUUID(microBasinUUID));
			microBasinRainfallData.setCount(rfStationsUnderMB.size());
			
			if (actualDataCount > 0) {
				microBasinRainfallData.setActual(actualValue / actualDataCount);
			}
			
			if (normalDataCount > 0) {
				microBasinRainfallData.setNormal(normalValue / normalDataCount);
			}
			
			resultMap.put(microBasinRainfallData.getLocationName(), microBasinRainfallData);
		}
		
		return resultMap;
	}
	
	protected Map<String, IWSCRainfallData> getWSCRainfallDataForAllMandals(long referenceTs)
			throws DSPException, ObjectNotFoundException {

		/**
		 * 1. Get start of season from given reference date.
		 * 2. Get the map of mandal to rainfall stations
		 * 3. Get data normal and actual data for all rainfall stations
		 * 
		 * 4. For each mandal location
		 * 5.		Get the list of rainfall stations under the mandal
		 * 6.		Iterate over each rainfall station and compute average
		 * 
		 */
		long seasonStartTs = DateUtils.getStartOfMonsoon(referenceTs);
		
		Map<String, List<String>> mandalUUIDToRainfallUUIDList = locService.getAllLocForParentChildTypes(LocationConstants.MANDAL, LocationConstants.RAINFALL);
		
		Map<Integer, Map<String, Double>> normalMandalLevelDataMap = iwmsDSP.getLocationNormalRFData(seasonStartTs, referenceTs);
		Map<String, IWaterQuantityEventData> actualStationLevelData = new HashMap<>();
		
		Map<String, IWSCRainfallData> resultMap = new HashMap<>();
		
		for (String mandalUUID : mandalUUIDToRainfallUUIDList.keySet()) {
			
			double actualValue = 0.0;
			double normalValue = 0.0;
		
			int normalDataCount = 0;
			int actualDataCount = 0;

			int mandalLocId = locationHierarchyService.getLocIdForLocUUID(mandalUUID);
			List<String> rfStationsUnderMandal = mandalUUIDToRainfallUUIDList.get(mandalUUID);
			if (rfStationsUnderMandal == null) {
				rfStationsUnderMandal = new ArrayList<>();
			}
			
			Map<String, Double> mandalNormalData = normalMandalLevelDataMap.get(mandalLocId);
			if (mandalNormalData == null) {
				mandalNormalData = new HashMap<>();
			}
			
			for(String date : mandalNormalData.keySet()){
				if (mandalNormalData.get(date) != null && mandalNormalData.get(date) >= 0) {
					actualValue += mandalNormalData.get(date);
					normalDataCount++;
				}
			}
			
			for (String rfLocUUID : rfStationsUnderMandal) {
				
				IWaterQuantityEventData rfWQData = (WaterQuantityEventData) actualStationLevelData.get(locService.getLocIdForLocUUID(rfLocUUID));
				 
				if (rfWQData != null && rfWQData.getLevelValue1() >= 0) {
					actualValue += rfWQData.getLevelValue1();
					actualDataCount++;	
				}
			}

			IWSCRainfallData mandalRainfallData = new WSCRainfallData();
			mandalRainfallData.setLocationName(locService.getLocNameForLocUUID(mandalUUID));
			
			mandalRainfallData.setCount(rfStationsUnderMandal.size());
			
			if (actualDataCount > 0) {
				mandalRainfallData.setActual(actualValue / actualDataCount);
			}
			
			if (normalDataCount > 0) {
				mandalRainfallData.setNormal(normalValue / normalDataCount);
			}
			
			resultMap.put(mandalRainfallData.getLocationName(), mandalRainfallData);

			if (normalDataCount <= 0) {
				normalValue = DSPConstants.NO_DATA;
			}
			
			if (actualDataCount <= 0) {
				actualValue = DSPConstants.NO_DATA;
			}
		}
		
		return resultMap;
	}
	
	/**
	 * Returns a map of station UUID to it's normal value
	 * @param startTs
	 * @param endTs
	 * @param microBasinUUIDToRainfallUUIDList
	 * @return
	 * @throws FetchingObjectException
	 * @throws DSPException
	 * @throws ObjectNotFoundException
	 */
	protected Map<String, Double> getAllStaionLevelNormalData(long startTs, long endTs, 
			Map<String, List<String>> microBasinUUIDToRainfallUUIDList) throws FetchingObjectException, DSPException, ObjectNotFoundException{
		
		Map<String, String> rainfallUUIDToMicroBasinUUIDMap = new HashMap<>();
		
		for(String microBasinUUID : microBasinUUIDToRainfallUUIDList.keySet()){
			List<String> rainfallUUIDs = microBasinUUIDToRainfallUUIDList.get(microBasinUUID);
			for(String rainfallUUID : rainfallUUIDs){
				if(!rainfallUUIDToMicroBasinUUIDMap.containsKey(rainfallUUID)){
					rainfallUUIDToMicroBasinUUIDMap.put(rainfallUUID, microBasinUUID);
				}
			}
		}
		
		Map<String, Double> stationNormalRainfallData = new HashMap<>();
		
		Map<Integer, Map<String, Double>> cumulativeData = iwmsDSP.getLocationNormalRFData(startTs, endTs);
		
		for(String rfLocationUUID : rainfallUUIDToMicroBasinUUIDMap.keySet()){
			
			int microBasinID = locService.getLocIdForLocUUID(rainfallUUIDToMicroBasinUUIDMap.get(rfLocationUUID));

			double aggregatedValue = 0;
			Map<String, Double> rainfallData = cumulativeData.get(microBasinID);
			
			for(String date : rainfallData.keySet()){
				if (rainfallData.get(date) != null && rainfallData.get(date) >= 0) {
					aggregatedValue += rainfallData.get(date);
				}
			}
			
			stationNormalRainfallData.put(rfLocationUUID, aggregatedValue);
		}
		
		return stationNormalRainfallData;
		
	}
	
	/**
	 * Returns WSCData for all the mandals
	 * 
	 * Takes in the startDate and endDate ("ddMMYYYY") Takes in the bucket size
	 * to be considered during computation
	 * 
	 * @param startDate
	 * @param endDate
	 * @param bucketSize
	 * @param MandalLocFullNameMap
	 * @return
	 * @throws DSPException
	 */
	protected Map<String, IWSCData> getAllWSCDataForMandals(int startDate, int endDate, int bucketSize,
			Map<String, String> mandalLocFullNameMap, Map<Long, Map<Integer, Double>> allGridLevelRunoffDataMap)
			throws DSPException {

		/*
		 * 1. Get all RunOff data at Mandal Level 2. Iterate over each of the
		 * Mandal locations 3. Iterate over the given period in steps of
		 * 'bucket-size' 4. Compute weekly values for Excess-RunOff,
		 * Conserved-RunOff,...
		 * 
		 */

		int startDateCopy = startDate;
		Map<String, IWSCData> resultMap = new HashMap<>();
		Map<String, Map<Integer, Double>> mandalToRunoffDataMap = nrscDataService.getIWMMandalLevelRunOffData(startDate,
				endDate, allGridLevelRunoffDataMap);

		for (String mandalLocName : mandalLocFullNameMap.keySet()) {

			String mandalLocFullName = mandalLocFullNameMap.get(mandalLocName);

			/*
			 * 1. Creating an empty POJO and assign it to NO_DATA 2. Add it to
			 * the resultant map
			 */
			IWSCData mandalWSCData = new WSCData();
			assignNoDataToAttributes(mandalWSCData);
			mandalWSCData.setLocationName(mandalLocName);
			resultMap.put(mandalLocFullName, mandalWSCData);

			/*
			 * Get run-off data for current mandal
			 */
			Map<Integer, Double> mandalDataMap = mandalToRunoffDataMap.get(mandalLocName);

			/*
			 * Get the water structure information to compute total capacity and
			 * total loss under the micro-basin
			 */
			Map<String, ILocationStructureInfo> structInfo = waterStructureMasterService
					.getStructuresDataForLocation(mandalLocFullName);
			ILocationStructureInfo aggregatedStructInfo = waterStructureMasterService
					.getAggregatedStructDataForLocation(mandalLocFullName);
			double wStructLoss = waterStructureMasterService.getAllWSLossForLocation(mandalLocFullName, bucketSize);

			if (structInfo == null || aggregatedStructInfo == null || wStructLoss < 0
			/* || !structInfo.containsKey(MI_TANK) */) {
				continue;
			}

			double totalRunOff = 0;
			double totalExcessRunOff = 0;
			double totalConservedRunOff = 0;
			double wStructCapacity = aggregatedStructInfo.getCapacity();

			/*
			 * TODO : Validate the logic
			 * 
			 * double miTankCapacity = structInfo.get(MI_TANK).getCapacity();
			 * double miTanksFraction = miTankCapacity / (wStructCapacity +
			 * miTankCapacity);
			 */
			double miTanksFraction = IWMConstants.WSC_DEFAULT_MITANK_ALLOCATION_FRACTION;

			/*
			 * Dividing the entire period into slots equal to the bucketSize.
			 * Compute total run-off water conserved by all the water
			 * structures.
			 */
			while (startDate <= endDate) {

				double qtyEndOfWeek = 0.0;
				double qtyStartOfWeek = 0.0;
				double qtyWeeklyRunOff = 0.0;
				double qtyWeeklyExcessRunOff = 0.0;

				/*
				 * 1. Compute week's runoff value by aggregating day-wise NRSC
				 * runoff data 2. Removing fraction of MITanks runoff 3.
				 * Computing excess runoff of the week
				 */
				qtyWeeklyRunOff = getBucketsRunOffData(startDate, endDate, bucketSize, mandalDataMap);
				qtyWeeklyRunOff -= (miTanksFraction * qtyWeeklyRunOff);
				qtyEndOfWeek = qtyStartOfWeek + qtyWeeklyRunOff - wStructLoss;

				if (qtyEndOfWeek < 0) {
					qtyEndOfWeek = 0.0;
				}

				if (qtyEndOfWeek > wStructCapacity) {
					qtyWeeklyExcessRunOff = qtyEndOfWeek - wStructCapacity;
					qtyEndOfWeek = wStructCapacity;
				}

				/*
				 * 1. Computing Σ(WeeklyRunOff) 2. Computing
				 * Σ(WeeklyExcessRunOff)
				 */
				totalRunOff += qtyWeeklyRunOff;
				totalExcessRunOff += qtyWeeklyExcessRunOff;

				qtyStartOfWeek = qtyEndOfWeek;
				startDate = DateUtils.addNDaysToModelDate(startDate, bucketSize);
			}

			/*
			 * RunOffConserved = Σ(WeeklyRunOff) - Σ(WeeklyExcessRunOff)
			 */
			totalConservedRunOff = totalRunOff - totalExcessRunOff;

			mandalWSCData.setRunOff(totalRunOff);
			mandalWSCData.setExcessRunOff(totalExcessRunOff);
			mandalWSCData.setRunOffCaptured(totalConservedRunOff);
			mandalWSCData.setTotalStructCount(structInfo.get(LocationConstants.TOTAL_STRUCTURES).getCount());
			mandalWSCData.setTotalStructCapacity(structInfo.get(LocationConstants.TOTAL_STRUCTURES).getCapacity());

			/*
			 * Reset the startDate to it's original value
			 */
			startDate = startDateCopy;
		}

		return resultMap;

	}

	private double getBucketsRunOffData(int startDate, int endDate, int bucketSize,
			Map<Integer, Double> dateToDataMap) {

		if (dateToDataMap == null)
			return 0;

		int weekStartDate = startDate;
		int weekEndDate = DateUtils.addNDaysToModelDate(startDate, bucketSize);

		/*
		 * Iterate over each day and sum up the runoff data
		 * 
		 */
		double qtyWeeklyRunOff = 0.0;

		while (weekStartDate < weekEndDate && weekStartDate <= endDate) {

			if (dateToDataMap.containsKey(weekStartDate)) {
				double microBasinCurrentData = dateToDataMap.get(weekStartDate);
				if (microBasinCurrentData >= 0) {
					qtyWeeklyRunOff += microBasinCurrentData;
				}
			}

			weekStartDate = DateUtils.addNDaysToModelDate(weekStartDate, 1);
		}

		return qtyWeeklyRunOff;
	}

	/**
	 * 
	 * This method adds the fields of child {@link IWSCData} to the
	 * corresponding parent {@link IWSCData} fields. It also increments the
	 * corresponding count of each field in the input Integer array.
	 * 
	 * @param parentStressData
	 *            {@link IWSCData}
	 * @param childStressData
	 *            {@link IWSCData}
	 * @param parentToChildDataIndicator
	 *            - Indicates whether data is present
	 * @return
	 */
	private void addWSCData(IWSCData parentData, IWSCData childData, MultiBoolean parentToChildDataIndicator) {

		if (childData.getRunOff() >= 0) {
			parentData.setRunOff(parentData.getRunOff() + childData.getRunOff());
			parentToChildDataIndicator.setRunOffPresent(true);
		}

		if (childData.getExcessRunOff() >= 0) {
			parentData.setExcessRunOff(parentData.getExcessRunOff() + childData.getExcessRunOff());
			parentToChildDataIndicator.setExcessRunOffPresent(true);
		}

		if (childData.getRunOffCaptured() >= 0) {
			parentData.setRunOffCaptured(parentData.getRunOffCaptured() + childData.getRunOffCaptured());
			parentToChildDataIndicator.setRunOffConservedPresent(true);
		}

		if (childData.getTotalStructCount() >= 0) {
			parentData.setTotalStructCount(parentData.getTotalStructCount() + childData.getTotalStructCount());
			parentToChildDataIndicator.setTotalStructCountPresent(true);
		}

		if (childData.getTotalStructCapacity() >= 0) {
			parentData.setTotalStructCapacity(parentData.getTotalStructCapacity() + childData.getTotalStructCapacity());
			parentToChildDataIndicator.setTotalStructCapacityPresent(true);
		}
	}

	/**
	 * This method checks the child-data count for each attribute and sets them
	 * to NO_DATA if child data count is zero.
	 * 
	 * @param subBsainWSCData
	 * @param subBasinToMicroBasinCount
	 */
	private void validateWCSData(IWSCData parentData, MultiBoolean parentToChildDataIndicator) {

		if (!parentToChildDataIndicator.isRunOffPresent()) {
			parentData.setRunOff(DSPConstants.NO_DATA);
		}

		if (!parentToChildDataIndicator.isExcessRunOffPresent()) {
			parentData.setExcessRunOff(DSPConstants.NO_DATA);
		}

		if (!parentToChildDataIndicator.isRunOffConservedPresent()) {
			parentData.setRunOffCaptured(DSPConstants.NO_DATA);
		}

		if (!parentToChildDataIndicator.isTotalStructCountPresent()) {
			parentData.setTotalStructCount(DSPConstants.NO_DATA);
		}

		if (!parentToChildDataIndicator.isTotalStructCapacityPresent()) {
			parentData.setTotalStructCapacity(DSPConstants.NO_DATA);
		}

		Map<String, ILocationStructureInfo> structInfo = waterStructureMasterService
				.getStructuresDataForLocation(parentData.getLocationFullName());
		if (structInfo != null) {
			parentData.setTotalStructCount(structInfo.get(LocationConstants.TOTAL_STRUCTURES).getCount());
			parentData.setTotalStructCapacity(structInfo.get(LocationConstants.TOTAL_STRUCTURES).getCapacity());
		}
	}

	/**
	 * Sets all the business data attributes of {@link IWSCData} to NO_DATA
	 * constants
	 * 
	 * @param wscData
	 */
	private void assignNoDataToAttributes(IWSCData wscData) {
		wscData.setRunOff(DSPConstants.NO_DATA);
		wscData.setExcessRunOff(DSPConstants.NO_DATA);
		wscData.setRunOffCaptured(DSPConstants.NO_DATA);
		wscData.setTotalStructCount(DSPConstants.NO_DATA);
		wscData.setTotalStructCapacity(DSPConstants.NO_DATA);
	}

	private void assignNoDataToAttributesForReservoir(IWSCReservoirData reservoirData) {
		reservoirData.setTotalNumberOfReservoirs(DSPConstants.NO_DATA_AVAILABLE);
		reservoirData.setCapacity(DSPConstants.NO_DATA);
		reservoirData.setCurrentStorage(DSPConstants.NO_DATA);
	}

	/**
	 * Returns a mapping of all microBasinName to their corresponding
	 * microBasinFullName
	 * 
	 * @param stateToBasinUUIDMap
	 * @param basinToSubBasinUUIDMap
	 * @param subBasinToMicroBasinUUIDMap
	 * @return
	 * @throws DSPException
	 */
	private Map<String, String> prepareMicroBasinNameToFullNameMapping(Map<String, List<String>> stateToBasinUUIDMap,
			Map<String, List<String>> basinToSubBasinUUIDMap, Map<String, List<String>> subBasinToMicroBasinUUIDMap)
			throws DSPException {

		/*
		 * 1. For each state 2. For each basin under the state 3. For each
		 * sub-basin under the basin 4. For each micro-basin under the sub-basin
		 * 5. concatenate the names of all heirarcies
		 * (state##basin##subbasin##microbasin)
		 * 
		 */

		Map<String, String> microBasinNameToFullNameMap = new HashMap<>();

		for (String stateUUID : stateToBasinUUIDMap.keySet()) {
			String stateLocName = locationHierarchyService.getLocNameForLocUUID(stateUUID);

			List<String> basinUUIDList = stateToBasinUUIDMap.get(stateUUID);
			if (basinUUIDList == null) {
				continue;
			}

			for (String basinUUID : basinUUIDList) {
				String basinLocName = locationHierarchyService.getLocNameForLocUUID(basinUUID);

				List<String> subBasinUUIDList = basinToSubBasinUUIDMap.get(basinUUID);
				if (subBasinUUIDList == null) {
					continue;
				}

				for (String subBasinUUID : subBasinUUIDList) {
					String subBasinLocName = locationHierarchyService.getLocNameForLocUUID(subBasinUUID);

					List<String> microBasinUUIDList = subBasinToMicroBasinUUIDMap.get(subBasinUUID);
					if (microBasinUUIDList == null) {
						continue;
					}

					for (String microBasinUUID : microBasinUUIDList) {
						String microBasinLocName = locationHierarchyService.getLocNameForLocUUID(microBasinUUID);
						String microBasinFullName = stateLocName + IWMConstants.DOUBLE_HASH_DELIMITER + basinLocName
								+ IWMConstants.DOUBLE_HASH_DELIMITER + subBasinLocName
								+ IWMConstants.DOUBLE_HASH_DELIMITER + microBasinLocName;

						microBasinNameToFullNameMap.put(microBasinLocName, microBasinFullName);
					}
				}
			}
		}

		return microBasinNameToFullNameMap;
	}

	/**
	 * Returns a mapping of all MandalName to their corresponding MandalFullName
	 * 
	 * @param stateToDistrictUUIDMap
	 * @param districtToMandalUUIDMap
	 * @return
	 * @throws DSPException
	 */
	private Map<String, String> prepareMandalNameToFullNameMapping(Map<String, List<String>> stateToDistrictUUID,
			Map<String, List<String>> districtToMandalUUID) throws DSPException {
		/*
		 * 1. For each state 2. For each district under the state 3. For each
		 * mandal under the district 4. concatenate the names of all heirarcies
		 * (state##basin##subbasin##microbasin)
		 * 
		 */

		Map<String, String> mandalNameToFullNameMap = new HashMap<>();

		for (String stateUUID : stateToDistrictUUID.keySet()) {
			String stateLocName = locationHierarchyService.getLocNameForLocUUID(stateUUID);

			List<String> districtUUIDList = stateToDistrictUUID.get(stateUUID);
			if (districtUUIDList == null) {
				continue;
			}

			for (String districtUUID : districtUUIDList) {
				String districtLocName = locationHierarchyService.getLocNameForLocUUID(districtUUID);

				List<String> mandalUUIDList = districtToMandalUUID.get(districtUUID);
				if (mandalUUIDList == null) {
					continue;
				}

				for (String mandalUUID : mandalUUIDList) {
					String mandalLocName = locationHierarchyService.getLocNameForLocUUID(mandalUUID);
					String mandalLocFullName = stateLocName + IWMConstants.DOUBLE_HASH_DELIMITER + districtLocName
							+ IWMConstants.DOUBLE_HASH_DELIMITER + mandalLocName;

					mandalNameToFullNameMap.put(mandalLocName, mandalLocFullName);
				}
			}
		}
		return mandalNameToFullNameMap;
	}

	/**
	 * While iterating over the children to compute parent's data, the
	 * corresponding attributes are set if some of the child has data.
	 * 
	 * If none of the children has data, then the corresponding attributes will
	 * be set to false.
	 */
	protected class MultiBoolean {

		private boolean isRunOffPresent;
		private boolean isExcessRunOffPresent;
		private boolean isRunOffConservedPresent;
		private boolean isTotalStructCountPresent;
		private boolean isTotalStructCapacityPresent;

		public MultiBoolean() {
			isRunOffPresent = false;
			isExcessRunOffPresent = false;
			isRunOffConservedPresent = false;
			isTotalStructCountPresent = false;
			isTotalStructCapacityPresent = false;
		}

		public boolean isRunOffPresent() {
			return isRunOffPresent;
		}

		public void setRunOffPresent(boolean isRunOffPresent) {
			this.isRunOffPresent = isRunOffPresent;
		}

		public boolean isExcessRunOffPresent() {
			return isExcessRunOffPresent;
		}

		public void setExcessRunOffPresent(boolean isExcessRunOffPresent) {
			this.isExcessRunOffPresent = isExcessRunOffPresent;
		}

		public boolean isRunOffConservedPresent() {
			return isRunOffConservedPresent;
		}

		public void setRunOffConservedPresent(boolean isRunOffConservedPresent) {
			this.isRunOffConservedPresent = isRunOffConservedPresent;
		}

		public boolean isTotalStructCountPresent() {
			return isTotalStructCountPresent;
		}

		public void setTotalStructCountPresent(boolean isTotalStructCountPresent) {
			this.isTotalStructCountPresent = isTotalStructCountPresent;
		}

		public boolean isTotalStructCapacityPresent() {
			return isTotalStructCapacityPresent;
		}

		public void setTotalStructCapacityPresent(boolean isTotalStructCapacityPresent) {
			this.isTotalStructCapacityPresent = isTotalStructCapacityPresent;
		}
	}

}
