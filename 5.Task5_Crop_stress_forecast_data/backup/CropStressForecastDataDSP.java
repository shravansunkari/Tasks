package com.vassarlabs.iwm.soilmoisture.cropstress.dsp.impl;

import static com.vassarlabs.iwm.utils.IWMConstants.IS_DEBUG_ENABLED;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.vassarlabs.common.dsp.context.DataStoreContext;
import com.vassarlabs.common.dsp.err.DSPException;
import com.vassarlabs.common.dsp.err.FetchingObjectException;
import com.vassarlabs.common.dsp.rdbms.api.IRDBMSDataStore;
import com.vassarlabs.common.dsp.utils.DSPConstants;
import com.vassarlabs.common.utils.DateUtils;
import com.vassarlabs.common.utils.StringUtils;
import com.vassarlabs.iwm.soilmoisture.cropstress.api.CropStressConstants;
import com.vassarlabs.iwm.soilmoisture.cropstress.dsp.api.ICropStressForecastDataDSP;
import com.vassarlabs.iwm.soilmoisture.cropstress.pojo.api.ICropGrowthStageMD;
import com.vassarlabs.iwm.soilmoisture.cropstress.pojo.api.ICropSownData;
import com.vassarlabs.iwm.soilmoisture.cropstress.pojo.api.ICropStressForecastData;
import com.vassarlabs.iwm.soilmoisture.cropstress.pojo.api.ICropStressPropagatedData;
import com.vassarlabs.iwm.soilmoisture.cropstress.pojo.api.ICropVillageLocData;
import com.vassarlabs.iwm.soilmoisture.cropstress.pojo.api.IFarmData;
import com.vassarlabs.iwm.soilmoisture.cropstress.pojo.api.ISownData;
import com.vassarlabs.iwm.soilmoisture.cropstress.pojo.api.IVillageWaterAvailableData;
import com.vassarlabs.iwm.soilmoisture.cropstress.pojo.impl.CropSownData;
import com.vassarlabs.iwm.soilmoisture.cropstress.pojo.impl.CropStressForecastData;
import com.vassarlabs.iwm.soilmoisture.cropstress.pojo.impl.CropStressPropagatedData;
import com.vassarlabs.iwm.soilmoisture.cropstress.pojo.impl.FarmData;
import com.vassarlabs.iwm.soilmoisture.cropstress.pojo.impl.VillageWaterAvailableData;

@Component
public class CropStressForecastDataDSP
	implements ICropStressForecastDataDSP {

	@Autowired
	@Qualifier("business_data")
	protected IRDBMSDataStore dataStore;

	@Override
	public void insertOrUpdateCropStressData(
			List<ICropStressForecastData> newCropStressForecastData,
			List<ICropStressForecastData> updateCropStressForecastData)
		throws DSPException {

		// Logic
		// 1. Insert new records in batches of 500
		// 2. Update existing records in batches of 500
		// 3. Commit all at once

		String dataStoreOwnerKey = null;
		String transactionId = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		boolean result = false;
		
		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
			transactionId = dataStore.beginTransaction();

			StringBuffer sql = new StringBuffer();			
			sql.append("insert into crop_stress_forecast_data");
			sql.append(" (crop_sown_data_id, village_water_available_data_id, model_date");
			sql.append(", crop_stage_id, cumm_water_supplied, optimal_water_req, minimal_water_req");
			sql.append(", owr_stress_factor_1, owr_stress_factor_2, owr_stress_factor_3, owr_stress_factor_4, owr_is_crop_under_stress");
			sql.append(", mwr_stress_factor_1, mwr_stress_factor_2, mwr_stress_factor_3, mwr_stress_factor_4, mwr_is_crop_under_stress");
			sql.append(", insert_ts, update_ts, deleted, user_session_id");
			sql.append(", pest_names, owr_next_7_days, mwr_next_7_days, water_available_next_7_days, last_n_days_computed_asm");
			sql.append(", crop_sown_data_source, curr_actual_etc, curr_potential_etc, cumm_actual_etc, cumm_potential_etc, estimated_yield, soil_water_critical_level");
			sql.append(") values (");
			sql.append(" ?, ?, ?");
			sql.append(", ?, ?, ?, ?");
			sql.append(", ?, ?, ?, ?, ?");
			sql.append(", ?, ?, ?, ?, ?");
			sql.append(", ?, null, 0, 0");
			sql.append(", ?, ?, ?, ?, ?");
			sql.append(", ?, ?, ?, ?, ?, ?, ? );");

			ps = dataStore.createPreparedStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);

			int batchRowCount = 0;
			for (ICropStressForecastData csfcData : newCropStressForecastData) {
				
				if(null !=  csfcData.getSownData() )
					ps.setLong(1, csfcData.getSownData().getDataId());
				
				
				ps.setLong(2, csfcData.getVillageWaterAvailableData().getVillageWaterAvailableDataId());
				ps.setInt(3, csfcData.getModelDate());
				ps.setInt(4, csfcData.getCropGrowthStageMD().getCropStageId());
				ps.setDouble(5, csfcData.getCummWaterSupplied());
				ps.setDouble(6, csfcData.getOptimalWaterReq());
				ps.setDouble(7, csfcData.getMinimalWaterReq());
				ps.setDouble(8, csfcData.getOWRStressFactor1());
				ps.setDouble(9, csfcData.getOWRStressFactor2());
				ps.setDouble(10, csfcData.getOWRStressFactor3());
				ps.setDouble(11, csfcData.getOWRStressFactor4());
				ps.setInt(12, (csfcData.isOWRCropUnderStress() == true ? 1 : 0));
				ps.setDouble(13, csfcData.getMWRStressFactor1());
				ps.setDouble(14, csfcData.getMWRStressFactor2());
				ps.setDouble(15, csfcData.getMWRStressFactor3());
				ps.setDouble(16, csfcData.getMWRStressFactor4());
				ps.setInt(17, (csfcData.isMWRCropUnderStress() == true ? 1 : 0));
				ps.setLong(18, DataStoreContext.getCurrentTS());
				
				String pestNames = "";
				if (null != csfcData.getPestNames()) {
					for(String pest : csfcData.getPestNames()) {
						pestNames += pest +"|";
					}
				}
				ps.setString(19,pestNames);
				ps.setDouble(20, csfcData.getOwrNext7Days());
				ps.setDouble(21, csfcData.getMwrNext7Days());
				ps.setDouble(22, csfcData.getWaterAvailableNext7Days());
				
				// inserting lastNDays of computed ASM in ascending order of date
				Map<Integer, Double> lastNDaysComputedASMMap = csfcData.getLast10DaysComputedASM();
				String lastNDaysComputedASM = String.valueOf(0);
				
				if(null != lastNDaysComputedASMMap && null != lastNDaysComputedASMMap.keySet() ){
					
					TreeSet<Integer> modelDateSortedList = new TreeSet<Integer>(lastNDaysComputedASMMap.keySet());
					for(int date : modelDateSortedList){
						lastNDaysComputedASM += lastNDaysComputedASMMap.get(date) + "|";
					}
				}
				ps.setString(23, lastNDaysComputedASM);
				ps.setShort(24, csfcData.getSownData().getCropDataSource());
				ps.setDouble(25, csfcData.getTodaysAETc());
				ps.setDouble(26, csfcData.getTodaysPETc());
				ps.setDouble(27, csfcData.getCumulativeAETc());
				ps.setDouble(28, csfcData.getCumulativePETc());
				ps.setDouble(29, csfcData.getEstimatedYield());
				ps.setDouble(30, csfcData.getSoilWaterCriticalLevel());
				
				
				ps.addBatch();
				++batchRowCount;
				
				if (batchRowCount % 500 == 0) {
					ps.executeBatch();
					batchRowCount = 0;
				}
			}
			// TODO: Update the cropStressForecastDataId post insert
			
			if (batchRowCount > 0) {
				ps.executeBatch();
			}
			ps.close();
			ps = null;
			
			// Update row
			sql = new StringBuffer();			
			sql.append("update crop_stress_forecast_data ");
			sql.append(" set crop_sown_data_id = ?, village_water_available_data_id = ?, model_date = ?");
			sql.append(", crop_stage_id = ?, cumm_water_supplied = ?, optimal_water_req = ?, minimal_water_req = ?");
			sql.append(", owr_stress_factor_1 = ?, owr_stress_factor_2 = ?, owr_stress_factor_3 = ?, owr_stress_factor_4 = ?, owr_is_crop_under_stress = ?");
			sql.append(", mwr_stress_factor_1 = ?, mwr_stress_factor_2 = ?, mwr_stress_factor_3 = ?, mwr_stress_factor_4 = ?, mwr_is_crop_under_stress = ?");
			sql.append(", update_ts = ? , pest_names = ?, owr_next_7_days = ?, mwr_next_7_days = ?, water_available_next_7_days = ?, last_n_days_computed_asm = ?");
			sql.append(", crop_sown_data_source = ?, curr_actual_etc = ?, curr_potential_etc = ?, cumm_actual_etc = ?, cumm_potential_etc = ?, estimated_yield = ?, soil_water_critical_level = ? ");
			sql.append(" where crop_stress_forecast_data_id = ? and deleted = 0;");
			
			
			ps = dataStore.createPreparedStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);
			
			batchRowCount = 0;
			for (ICropStressForecastData csfcData : updateCropStressForecastData) {
				
				ps.setLong(1, csfcData.getSownData().getDataId());
				ps.setLong(2, csfcData.getVillageWaterAvailableData().getVillageWaterAvailableDataId());
				ps.setInt(3, csfcData.getModelDate());
				ps.setInt(4, csfcData.getCropGrowthStageMD().getCropStageId());
				ps.setDouble(5, csfcData.getCummWaterSupplied());
				ps.setDouble(6, csfcData.getOptimalWaterReq());
				ps.setDouble(7, csfcData.getMinimalWaterReq());
				ps.setDouble(8, csfcData.getOWRStressFactor1());
				ps.setDouble(9, csfcData.getOWRStressFactor2());
				ps.setDouble(10, csfcData.getOWRStressFactor3());
				ps.setDouble(11, csfcData.getOWRStressFactor4());
				ps.setInt(12, (csfcData.isOWRCropUnderStress() == true ? 1 : 0));
				ps.setDouble(13, csfcData.getMWRStressFactor1());
				ps.setDouble(14, csfcData.getMWRStressFactor2());
				ps.setDouble(15, csfcData.getMWRStressFactor3());
				ps.setDouble(16, csfcData.getMWRStressFactor4());
				ps.setInt(17, (csfcData.isMWRCropUnderStress() == true ? 1 : 0));
				ps.setLong(18, DataStoreContext.getCurrentTS());
				
				String pestNames = "";
				if(null != csfcData.getPestNames()){
					for(String pest : csfcData.getPestNames()){
						pestNames += pest +"|";
					}
				}
				ps.setString(19,pestNames);
				ps.setDouble(20, csfcData.getOwrNext7Days());
				ps.setDouble(21, csfcData.getMwrNext7Days());
				ps.setDouble(22, csfcData.getWaterAvailableNext7Days());
				
				// inserting lastNDays of computed ASM in ascending order of date
				Map<Integer, Double> lastNDaysComputedASMMap = csfcData.getLast10DaysComputedASM();
				String lastNDaysComputedASM = String.valueOf(0);
				
				if(null != lastNDaysComputedASMMap && null != lastNDaysComputedASMMap.keySet() ){
					
					TreeSet<Integer> modelDateSortedList = new TreeSet<Integer>(lastNDaysComputedASMMap.keySet());
					for(int date : modelDateSortedList){
						lastNDaysComputedASM += lastNDaysComputedASMMap.get(date) + "|";
					}
				}
				ps.setString(23, lastNDaysComputedASM);
				ps.setShort(24, csfcData.getSownData().getCropDataSource());
				ps.setDouble(25, csfcData.getTodaysAETc());
				ps.setDouble(26, csfcData.getTodaysPETc());
				ps.setDouble(27, csfcData.getCumulativeAETc());
				ps.setDouble(28, csfcData.getCumulativePETc());
				ps.setDouble(29, csfcData.getEstimatedYield());
				ps.setDouble(30, csfcData.getSoilWaterCriticalLevel());
				
				ps.setLong(31, csfcData.getCropStressForecastDataId());
				ps.addBatch();
				
				++batchRowCount;
				
				if (batchRowCount % 500 == 0) {
					ps.executeBatch();
					batchRowCount = 0;
				}
			}
			
			if (batchRowCount > 0) {
				ps.executeBatch();
			}
				
			dataStore.commitTransaction(transactionId);
			result = true;
		} catch (SQLException e) {
			System.out.println("Error inserting or updating CropStress Forecast Data - " + e.getMessage());
			throw new DSPException("Error inserting or updating CropStress Forecast Data", e);
		} finally {
			if (!result) {
				dataStore.rollbackTransaction(transactionId);
			}
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				rs = null;
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				ps = null;
			}
			DataStoreContext.clearDataStoreContext(dataStoreOwnerKey);
		}
	}

	@Override
	public Map<String, Map<String, List<ICropStressForecastData>>> getVillageCropStressData(
			int modelDate, List<String> villageNameList, List<String> cropNames,
			Map<Integer, ICropGrowthStageMD> cropGrowthStageMDMap, Map<Integer, ICropVillageLocData> allCropVillageLocData)
		throws DSPException {
				
		
		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		Map<String, Map<String, List<ICropStressForecastData>>> allVillageCSD = new HashMap<String, Map<String,List<ICropStressForecastData>>>();

		/* Using HashSet because contains() method takes O(1), unlinke O(n) for a list */
		HashSet<String> villageNameSet = new HashSet<>();
		HashSet<String> cropNameSet = new HashSet<>();

		if (villageNameList != null && !villageNameList.isEmpty()) {
			villageNameSet = new HashSet<String>(villageNameList);
		}

		if (cropNames != null && !cropNames.isEmpty()) {
			cropNameSet = new HashSet<String>(cropNames);
		}
		

		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
			
			StringBuffer sql = new StringBuffer();
			sql.append("select *");
			sql.append(" from crop_stress_forecast_data cf join crop_sown_data cs join village_water_available_data vw ");
			sql.append(" on cf.crop_sown_data_id = cs.crop_sown_data_id");
			sql.append(" and cf.village_water_available_data_id = vw.village_water_available_data_id");
			sql.append(" where cf.model_date = ? and cf.deleted = 0 and cs.deleted = 0 and vw.deleted = 0 ");
			
			ps = dataStore.createPreparedStatement(sql.toString());
			ps.setInt(1, modelDate);
			
			rs = ps.executeQuery();
			while (rs.next()) {
				// preparing village water available data 
				
				IVillageWaterAvailableData vwaData = new VillageWaterAvailableData();
				
				vwaData.setVillageWaterAvailableDataId(rs.getLong("village_water_available_data_id"));
				vwaData.setVillageCode(rs.getLong("village_code"));
				vwaData.setModelDate(rs.getInt("model_date"));
				vwaData.setAvailableSM(rs.getDouble("soil_moisture"));
				vwaData.setSoilMoistureDate(rs.getInt("soil_moisture_date")); // SMCS-135
				vwaData.setLastNDayASMInMM(rs.getDouble("asm_in_mm_last_N_day"));
				vwaData.setCurrentRainfall(rs.getDouble("curr_rainfall"));
				
				double[] rfForecast = new double[7];
				rfForecast[0] = rs.getDouble("rf_forecast_1");
				rfForecast[1] = rs.getDouble("rf_forecast_2");
				rfForecast[2] = rs.getDouble("rf_forecast_3");
				rfForecast[3] = rs.getDouble("rf_forecast_4");
				rfForecast[4] = rs.getDouble("rf_forecast_5");
				rfForecast[5] = rs.getDouble("rf_forecast_6");
				rfForecast[6] = rs.getDouble("rf_forecast_7");
				vwaData.setRFForecastData(rfForecast);
				// IWM-617 , IWM-614
				vwaData.setLastNDayASMInMM(rs.getDouble("asm_in_mm_last_N_day"));
				
							
				Map<Integer, Double>  last10DaysRF = new HashMap<Integer, Double>  ();
				int noOfDay = CropStressConstants.DEFAULT_SF3_COMPUTE_LAST_N_DAYS_SM_WINDOW;
				
				if(null != rs.getString("rf_last_N_days") && "" != rs.getString("rf_last_N_days")){
					String [] rfData = rs.getString("rf_last_N_days").split("\\|");	
					for(String data : rfData){
						last10DaysRF.put(DateUtils.substractDaysFromModelDate(vwaData.getModelDate(), noOfDay), Double.parseDouble(data));
						noOfDay--;
					}
				}
				
				
				
				Map<Integer, String>  etoLast10Days = new HashMap<Integer, String> ();
				noOfDay = CropStressConstants.DEFAULT_SF3_COMPUTE_LAST_N_DAYS_SM_WINDOW;
				if(null != rs.getString("eto_last_N_days") && ""!= rs.getString("eto_last_N_days")){
					String [] etoLastNdDayslist = rs.getString("eto_last_N_days").split("\\|");
					for(String ETO : etoLastNdDayslist){
						int date = DateUtils.substractDaysFromModelDate(vwaData.getModelDate(),noOfDay);
						etoLast10Days.put(date,ETO);
						noOfDay--;
					}
				}
				
				
				vwaData.setLastNDaysRF(last10DaysRF);
				vwaData.setLastNDaysETo(etoLast10Days);
				vwaData.setPotentialEvapotranspiration(rs.getDouble("potential_evapotranspiration"));
				
				String [] etoForecastNdDayslist = null;
				Map<Integer, String>  etoForecastDays = new HashMap<Integer, String> ();
				if( null != rs.getString("eto_forecast_N_days") && "" !=  rs.getString("eto_forecast_N_days")){
					 etoForecastNdDayslist = rs.getString("eto_forecast_N_days").split("\\|");
					
						noOfDay = 0;
						
						while(noOfDay <= CropStressConstants.DEFAULT_NO_OF_RAINFALL_FORECAST_DAYS){
							int date = Integer.parseInt(DateUtils.getNDayInFormat("yyyyMMdd",String.valueOf(vwaData.getModelDate()),noOfDay));
							if(etoForecastNdDayslist.length > noOfDay)
								etoForecastDays.put(date,etoForecastNdDayslist[noOfDay]);
							else
								etoForecastDays.put(date,String.valueOf(0));
							++noOfDay;
						}
				}
				
				vwaData.setNDaysForecastETo(etoForecastDays);
				// Preparing crop sown data ..
				
				ISownData csData = new CropSownData();
				
				csData.setDataId(rs.getLong("crop_sown_data_id"));
				csData.setVillageCode(rs.getInt("village_code"));
				csData.setPeriodName(rs.getString("period_name").toUpperCase());
				csData.setCropName(rs.getString("crop_name").toUpperCase());
				csData.setTotalAreaSown(rs.getDouble("total_area_sown"));
				csData.setSowingDate(rs.getInt("avg_sowing_date"));
				csData.setExpectedHarvestDate(rs.getInt("expected_harvest_date"));				
				csData.setInsertTs(rs.getLong("insert_ts"));
				csData.setUpdateTs(rs.getLong("update_ts"));
				csData.setDeleted(rs.getInt("deleted"));
				csData.setUserSessionId(rs.getLong("user_session_id"));
				csData.setMonsoonYear(rs.getInt("monsoon_year"));
				
				ICropStressForecastData csfData = new CropStressForecastData();
				
				csfData.setCropStressForecastDataId(rs.getLong("crop_stress_forecast_data_id"));
				csfData.setCropGrowthStageMD(cropGrowthStageMDMap.get(rs.getInt("crop_stage_id")));
				csfData.setCropSownDataSource(rs.getShort("crop_sown_data_source"));
				csfData.setVillageWaterAvailableData(vwaData);
				csfData.setModelDate(modelDate);
				csfData.setCummWaterSupplied(rs.getDouble("cumm_water_supplied"));
				csfData.setOptimalWaterReq(rs.getDouble("optimal_water_req"));
				csfData.setMinimalWaterReq(rs.getDouble("minimal_water_req"));
				csfData.setOWRStressFactor1(rs.getDouble("owr_stress_factor_1"));
				csfData.setOWRStressFactor2(rs.getDouble("owr_stress_factor_2"));
				csfData.setOWRStressFactor3(rs.getDouble("owr_stress_factor_3"));
				csfData.setOWRStressFactor4(rs.getDouble("owr_stress_factor_4"));
				csfData.setOWRCropUnderStress(rs.getInt("owr_is_crop_under_stress") == 1 ? true : false);
				csfData.setMWRStressFactor1(rs.getDouble("mwr_stress_factor_1"));
				csfData.setMWRStressFactor2(rs.getDouble("mwr_stress_factor_2"));
				csfData.setMWRStressFactor3(rs.getDouble("mwr_stress_factor_3"));
				csfData.setMWRStressFactor4(rs.getDouble("mwr_stress_factor_4"));
				csfData.setMWRCropUnderStress(rs.getInt("mwr_is_crop_under_stress") == 1 ? true : false);
				csfData.setSownData(csData);
				
				if (csfData.getSownData() == null) {
					/* This condition will happen, if we compute for irrigated crops
					 * Just an extra measure to make sure that we don't have any computed data for irrigated crops
					 * */
					continue;
				}
				Set <String> pestnames = null;
				String pest = rs.getString("pest_names");
				if(null != pest && pest.contains("|")){
					pestnames = new HashSet<>(Arrays.asList(pest.split("\\|")));
				}
				
				csfData.setPestNames(pestnames);
				csfData.setOwrNext7Days(rs.getDouble("owr_next_7_days"));
				csfData.setMwrNext7Days(rs.getDouble("mwr_next_7_days"));
				csfData.setWaterAvailableNext7Days(rs.getDouble("water_available_next_7_days"));
				
				// SMCS-153,154 Store computed ASM
				
				Map<Integer, Double>  last10DaysComputedASM = new HashMap<>();
				
				noOfDay = CropStressConstants.DEFAULT_SF3_COMPUTE_LAST_N_DAYS_SM_WINDOW;
				
				String[] asmData = null ;
				if(null != rs.getString("last_n_days_computed_asm") && "" != rs.getString("last_n_days_computed_asm") ){
					asmData = rs.getString("last_n_days_computed_asm").split("\\|");
					for(String data : asmData) {
						last10DaysComputedASM.put(DateUtils.substractDaysFromModelDate(csfData.getModelDate(), noOfDay), Double.parseDouble(data));
						noOfDay--;
					}
				}
				
				csfData.setTodaysAETc(rs.getDouble("curr_actual_etc"));
				csfData.setTodaysPETc(rs.getDouble("curr_potential_etc"));
				csfData.setCumulativeAETc(rs.getDouble("cumm_actual_etc"));
				csfData.setCumulativePETc(rs.getDouble("cumm_potential_etc"));
				csfData.setEstimatedYield(rs.getDouble("estimated_yield"));
				csfData.setSoilWaterCriticalLevel(rs.getDouble("soil_water_critical_level"));
				
				csfData.setLast10DaysComputedASM(last10DaysComputedASM);
				csfData.setInsertTs(rs.getLong("insert_ts"));
				csfData.setUpdateTs(rs.getLong("update_ts"));
				csfData.setDeleted(rs.getInt("deleted"));
				csfData.setUserSessionId(rs.getLong("user_session_id"));

				String villageName = allCropVillageLocData.get(csfData.getSownData().getVillageCode()).getVillageFullName();
				String cropNameTmp = csfData.getSownData().getCropName();
				
				// TODO: Inefficient way of filtering
				if (villageNameSet != null
						&& !villageNameSet.contains(villageName)) {
					continue;
				}
				if (cropNameSet != null
						&& !cropNameSet.contains(cropNameTmp)) {
					continue;
				}

				Map<String, List<ICropStressForecastData>> villageCSData = allVillageCSD.get(villageName);
				if (villageCSData == null) {
					villageCSData = new HashMap<String, List<ICropStressForecastData>>();
					allVillageCSD.put(villageName, villageCSData);
				}
				List<ICropStressForecastData> cropStressDataList = villageCSData.get(cropNameTmp);
				if (cropStressDataList == null) {
					cropStressDataList = new ArrayList<ICropStressForecastData>();
					villageCSData.put(cropNameTmp, cropStressDataList);
				}
				cropStressDataList.add(csfData);
			}

			return allVillageCSD;
		} catch (SQLException se) {
			System.out.println("Error retrieving all ICropStressForecastData for model date : " + modelDate
					+ " -- villageNameList : " + villageNameList + " -- cropNamesList : " + cropNames + " --- " +se.getMessage());
			throw new FetchingObjectException("Error retrieving all ICropStressForecastData for model date : "
					+ modelDate + " -- villageNameList : " + villageNameList + " -- cropNamesList : " + cropNames, se);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				rs = null;
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				ps = null;
			}
			DataStoreContext.clearDataStoreContext(dataStoreOwnerKey);
		}
	}

	/**
	 * Returns all village's {@link IVillageWaterAvailableData} by Id
	 * 
	 * Map of villageWaterAvailableDataId and {@link IVillageWaterAvailableData}
	 * @param modelDate
	 * @return
	 * @throws DSPException 
	 */
	protected Map<Long, IVillageWaterAvailableData> getAllVillageWaterAvailableDataById(
			int modelDate)
		throws DSPException {

		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		Map<Long, IVillageWaterAvailableData> allVillageWaterAvailableData = new HashMap<Long, IVillageWaterAvailableData>();

		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
			
			StringBuffer sql = new StringBuffer();
			sql.append("select *");
			sql.append(" from village_water_available_data");
			sql.append(" where model_date = ? and deleted = 0");

			ps = dataStore.createPreparedStatement(sql.toString());
			ps.setInt(1, modelDate);

			rs = ps.executeQuery();
			
			while (rs.next()) {
				IVillageWaterAvailableData vwaData = new VillageWaterAvailableData();
				
				vwaData.setVillageWaterAvailableDataId(rs.getLong("village_water_available_data_id"));
				vwaData.setVillageCode(rs.getLong("village_code"));
				vwaData.setModelDate(rs.getInt("model_date"));
				vwaData.setAvailableSM(rs.getDouble("soil_moisture"));
				vwaData.setLastNDayASMInMM(rs.getDouble("asm_in_mm_last_N_day"));
				vwaData.setCurrentRainfall(rs.getDouble("curr_rainfall"));
				vwaData.setSoilMoistureDate(rs.getInt("soil_moisture_date"));
				
				double[] rfForecast = new double[7];
				rfForecast[0] = rs.getDouble("rf_forecast_1");
				rfForecast[1] = rs.getDouble("rf_forecast_2");
				rfForecast[2] = rs.getDouble("rf_forecast_3");
				rfForecast[3] = rs.getDouble("rf_forecast_4");
				rfForecast[4] = rs.getDouble("rf_forecast_5");
				rfForecast[5] = rs.getDouble("rf_forecast_6");
				rfForecast[6] = rs.getDouble("rf_forecast_7");
				vwaData.setRFForecastData(rfForecast);
				// IWM-617 , IWM-614
				vwaData.setLastNDayASMInMM(rs.getDouble("asm_in_mm_last_N_day"));
				Map<Integer, Double>  last10DaysRF = new HashMap<Integer, Double>  ();
				
				String [] rfData = null;
				int noOfDay = CropStressConstants.DEFAULT_SF3_COMPUTE_LAST_N_DAYS_SM_WINDOW;
				if(null != rs.getString("rf_last_N_days") && "" != rs.getString("rf_last_N_days")){
					rfData = rs.getString("rf_last_N_days").split("\\|");
					
					for(String data : rfData){
						last10DaysRF.put(DateUtils.substractDaysFromModelDate(vwaData.getModelDate(), noOfDay), Double.parseDouble(data));
						noOfDay--;
					}
				}
				
				
				Map<Integer, String>  etoLast10Days = new HashMap<Integer, String> ();
				noOfDay = CropStressConstants.DEFAULT_SF3_COMPUTE_LAST_N_DAYS_SM_WINDOW;
				String [] etoData = null;
				
				if(null != rs.getString("eto_last_N_days") && ""!= rs.getString("eto_last_N_days")){
					etoData = rs.getString("eto_last_N_days").split("\\|");
					for(String ETO : etoData){
						int date = DateUtils.substractDaysFromModelDate(vwaData.getModelDate(),noOfDay);
						etoLast10Days.put(date,ETO);
						noOfDay--;
					}
				}
				
				
				
				vwaData.setLastNDaysRF(last10DaysRF);
				vwaData.setLastNDaysETo(etoLast10Days);
				vwaData.setPotentialEvapotranspiration(rs.getDouble("potential_evapotranspiration"));
				
				String [] etoForecastNdDayslist = null;
				Map<Integer, String>  etoForecastDays = new HashMap<Integer, String> ();
				if( null != rs.getString("eto_forecast_N_days") && "" !=  rs.getString("eto_forecast_N_days")){
					 etoForecastNdDayslist = rs.getString("eto_forecast_N_days").split("\\|");
					
						noOfDay = 0;
						
						while(noOfDay <= CropStressConstants.DEFAULT_NO_OF_RAINFALL_FORECAST_DAYS){
							int date = Integer.parseInt(DateUtils.getNDayInFormat("yyyyMMdd",String.valueOf(vwaData.getModelDate()),noOfDay));
							if(etoForecastNdDayslist.length > noOfDay)
								etoForecastDays.put(date,etoForecastNdDayslist[noOfDay]);
							else
								etoForecastDays.put(date,String.valueOf(0));
							++noOfDay;
						}
					
				}
				
				vwaData.setNDaysForecastETo(etoForecastDays);
				vwaData.setInsertTs(rs.getLong("insert_ts"));
				vwaData.setUpdateTs(rs.getLong("update_ts"));
				vwaData.setDeleted(rs.getInt("deleted"));
				vwaData.setUserSessionId(rs.getLong("user_session_id"));

				allVillageWaterAvailableData.put(vwaData.getVillageWaterAvailableDataId(), vwaData);
			}

			return allVillageWaterAvailableData;
		} catch (SQLException se) {
			// TODO: Log and throw the right exception
			System.out.println("Error retrieving all IVillageWaterAvailableData for model date : " + modelDate + " --- " + se.getMessage());
			throw new FetchingObjectException(
				"Error retrieving all IVillageWaterAvailableData for model date : " + modelDate, se);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				rs = null;
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				ps = null;
			}
			DataStoreContext.clearDataStoreContext(dataStoreOwnerKey);
		}
	}
	
	@Override
	public Map<Integer, IVillageWaterAvailableData> getAllVillageWaterAvailableData(
			int modelDate)
		throws DSPException {

		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		Map<Integer, IVillageWaterAvailableData> allVillageWaterAvailableData = new HashMap<Integer, IVillageWaterAvailableData>();

		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
			
			StringBuffer sql = new StringBuffer();
			sql.append("select *");
			sql.append(" from village_water_available_data");
			sql.append(" where model_date = ? and deleted = 0");

			ps = dataStore.createPreparedStatement(sql.toString());
			ps.setInt(1, modelDate);

			rs = ps.executeQuery();
			
			while (rs.next()) {
				IVillageWaterAvailableData vwaData = new VillageWaterAvailableData();
				
				vwaData.setVillageWaterAvailableDataId(rs.getLong("village_water_available_data_id"));
				vwaData.setVillageCode(rs.getLong("village_code"));
				vwaData.setModelDate(rs.getInt("model_date"));
				vwaData.setAvailableSM(rs.getDouble("soil_moisture"));
				vwaData.setSoilMoistureDate(rs.getInt("soil_moisture_date")); // SMCS-135
				vwaData.setLastNDayASMInMM(rs.getDouble("asm_in_mm_last_N_day"));
				vwaData.setCurrentRainfall(rs.getDouble("curr_rainfall"));
				
				double[] rfForecast = new double[7];
				rfForecast[0] = rs.getDouble("rf_forecast_1");
				rfForecast[1] = rs.getDouble("rf_forecast_2");
				rfForecast[2] = rs.getDouble("rf_forecast_3");
				rfForecast[3] = rs.getDouble("rf_forecast_4");
				rfForecast[4] = rs.getDouble("rf_forecast_5");
				rfForecast[5] = rs.getDouble("rf_forecast_6");
				rfForecast[6] = rs.getDouble("rf_forecast_7");
				vwaData.setRFForecastData(rfForecast);
				
				// IWM-617 , IWM-614
				vwaData.setLastNDayASMInMM(rs.getDouble("asm_in_mm_last_N_day"));				
				
				Map<Integer, Double>  last10DaysRF = new HashMap<Integer, Double>  ();
				int noOfDay = CropStressConstants.DEFAULT_SF3_COMPUTE_LAST_N_DAYS_SM_WINDOW;
				
				if(null != rs.getString("rf_last_N_days") && "" != rs.getString("rf_last_N_days")){
					String [] rfData = rs.getString("rf_last_N_days").split("\\|");
					for(String data : rfData){					
						last10DaysRF.put(DateUtils.substractDaysFromModelDate(vwaData.getModelDate(), noOfDay), Double.parseDouble(data));
						noOfDay--;
					}
				}
				
				
				Map<Integer, String>  etoLast10Days = new HashMap<Integer, String> ();
				noOfDay = CropStressConstants.DEFAULT_SF3_COMPUTE_LAST_N_DAYS_SM_WINDOW;
				
				if(null != rs.getString("eto_last_N_days") && ""!= rs.getString("eto_last_N_days")){
					String [] etoData = rs.getString("eto_last_N_days").split("\\|");
					for(String data : etoData){
						int date = DateUtils.substractDaysFromModelDate(vwaData.getModelDate(),noOfDay);
						etoLast10Days.put(date,data);
						noOfDay--;
					}
				}
				
				
				vwaData.setLastNDaysRF(last10DaysRF);
				vwaData.setLastNDaysETo(etoLast10Days);
				
				vwaData.setPotentialEvapotranspiration(rs.getDouble("potential_evapotranspiration"));
				Map<Integer, String>  etoForecastDays = null;
				
				if(null != rs.getString("eto_forecast_N_days") && "" !=  rs.getString("eto_forecast_N_days")){
					String [] etoForecastNdDayslist = rs.getString("eto_forecast_N_days").split("\\|");
					etoForecastDays = new HashMap<Integer, String> ();
					noOfDay = 0;
					
					while(noOfDay <= CropStressConstants.DEFAULT_NO_OF_RAINFALL_FORECAST_DAYS){
						int date = Integer.parseInt(DateUtils.getNDayInFormat("yyyyMMdd",String.valueOf(vwaData.getModelDate()),noOfDay));
						if(etoForecastNdDayslist.length > noOfDay)
							etoForecastDays.put(date,etoForecastNdDayslist[noOfDay]);
						else
							etoForecastDays.put(date,String.valueOf(0));
						++noOfDay;
					}
				}
				vwaData.setNDaysForecastETo(etoForecastDays);
				vwaData.setInsertTs(rs.getLong("insert_ts"));
				vwaData.setUpdateTs(rs.getLong("update_ts"));
				vwaData.setDeleted(rs.getInt("deleted"));
				vwaData.setUserSessionId(rs.getLong("user_session_id"));

				allVillageWaterAvailableData.put((int)vwaData.getVillageCode(), vwaData);
			}

			return allVillageWaterAvailableData;
			
		} catch (SQLException se) {
			// TODO: Log and throw the right exception
			System.out.println("Error retrieving all IVillageWaterAvailableData for model date : " + modelDate + " --- " + se.getMessage());
			throw new FetchingObjectException(
				"Error retrieving all IVillageWaterAvailableData for model date : " + modelDate, se);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				rs = null;
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				ps = null;
			}
			DataStoreContext.clearDataStoreContext(dataStoreOwnerKey);
		}
	}

	@Override
	public void insertOrUpdateVillageWaterAvailableData(
			List<IVillageWaterAvailableData> newVillageWaterAvailableData,
			List<IVillageWaterAvailableData> updateVillageWaterAvailableData)
		throws DSPException {

		// Logic
		// 1. Insert new records in batches of 500
		// 2. Update existing records in batches of 500
		// 3. Commit all at once

		String dataStoreOwnerKey = null;
		String transactionId = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		boolean result = false;
		
		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
			transactionId = dataStore.beginTransaction();

			StringBuffer sql = new StringBuffer();			
			sql.append("insert into village_water_available_data");
			sql.append(" (village_code, model_date, soil_moisture, soil_moisture_date, asm_in_mm_last_N_day, curr_rainfall");
			sql.append(", rf_forecast_1, rf_forecast_2, rf_forecast_3, rf_forecast_4");
			sql.append(", rf_forecast_5, rf_forecast_6, rf_forecast_7, rf_last_N_days");
			sql.append(", potential_evapotranspiration, eto_last_N_days");
			sql.append(", eto_forecast_N_days");
			sql.append(", insert_ts, update_ts, deleted, user_session_id");
			sql.append(") values (");
			sql.append(" ?, ?, ?, ?, ?, ?");
			sql.append(", ?, ?, ?, ?");
			sql.append(", ?, ?, ?, ?");
			sql.append(", ?,?");
			sql.append(", ?");
			sql.append(", ?, null, 0, 0);");
			
			ps = dataStore.createPreparedStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);
			
			int batchRowCount = 0;
			List<IVillageWaterAvailableData> tmpList = new ArrayList<IVillageWaterAvailableData>();
			for (IVillageWaterAvailableData vwaData : newVillageWaterAvailableData) {

				// tmpList to set the village water available data id
				tmpList.add(vwaData);
				
				ps.setLong(1, vwaData.getVillageCode());
				ps.setInt(2, vwaData.getModelDate());
				ps.setDouble(3, vwaData.getAvailableSM());
				ps.setInt(4, vwaData.getSoilMoistureDate());  // SMCS-135
				ps.setDouble(5, vwaData.getLastNDayASMInMM());
				ps.setDouble(6, vwaData.getCurrentRainfall());
				ps.setDouble(7, vwaData.getRFForecastData()[0]);
				ps.setDouble(8, vwaData.getRFForecastData()[1]);
				ps.setDouble(9, vwaData.getRFForecastData()[2]);
				ps.setDouble(10, vwaData.getRFForecastData()[3]);
				ps.setDouble(11, vwaData.getRFForecastData()[4]);
				ps.setDouble(12, vwaData.getRFForecastData()[5]);
				// TODO: The RF forecast is only for 6 days - to be fixed
				ps.setDouble(13, -1.0); // vwaData.getRFForecastData()[6]);
				
				Map<Integer, Double>   rfData = vwaData.getLastNDaysRF();
				
				// inserting rain fall of  last days in ascending order IWM-617, IWM-614
				SortedSet<Integer> modelDateSortedList = new TreeSet<Integer>(rfData.keySet());
				String	rainfalldata = "";
				for(int date:modelDateSortedList){
					rainfalldata += rfData.get(date)+"|";
				}
				
				
				// inserting ETO of  last days in ascending order 
				Map<Integer, String>   etolastNDayMap = vwaData.getLastNDaysETo();
				
				String	etoLastNDayString = String.valueOf(0);
				if(null != etolastNDayMap && null != etolastNDayMap.keySet() ){
					modelDateSortedList = new TreeSet<Integer>(etolastNDayMap.keySet());
					for(int date:modelDateSortedList){
						etoLastNDayString += etolastNDayMap.get(date)+"|";
					}
				}
				
				ps.setString(14,rainfalldata);
				ps.setDouble(15, vwaData.getPotentialEvapotranspiration());
				ps.setString(16,etoLastNDayString);
				
				modelDateSortedList = null;	
				String	etoForecastNDayString = String.valueOf(0) ;
				
				Map<Integer, String> etoForecastDayMap = vwaData.getNDaysForecastETo();
				if(null != etoForecastDayMap && etoForecastDayMap.keySet().size() > 0){
					modelDateSortedList  = new TreeSet<Integer>(etoForecastDayMap.keySet());
					
					for(int date:modelDateSortedList){
						
						etoForecastNDayString += etoForecastDayMap.get(date)+"|";
					}
				}
				ps.setString(17, etoForecastNDayString);
				
				ps.setLong(18, DataStoreContext.getCurrentTS());
				
				ps.addBatch();
				
				++batchRowCount;
				
				if (batchRowCount % 500 == 0) {
					int[] vwaDataIdList = ps.executeBatch();
					batchRowCount = 0;
					for (int index = 0; index < vwaDataIdList.length;index++) {
						tmpList.get(index).setVillageWaterAvailableDataId(vwaDataIdList[index]);
					}
					tmpList.clear();
				}
			}
			// TODO: Update the cropStressForecastDataId post insert
			
			if (batchRowCount > 0) {
				int[] vwaDataIdList = ps.executeBatch();
				batchRowCount = 0;
				for (int index = 0; index < vwaDataIdList.length;index++) {
					tmpList.get(index).setVillageWaterAvailableDataId(vwaDataIdList[index]);
				}
				tmpList.clear();
			}
			ps.close();
			ps = null;
			
			// Update row
			sql = new StringBuffer();			
			sql.append("update village_water_available_data ");
			sql.append(" set village_code = ?, model_date = ?, soil_moisture = ?,  soil_moisture_date = ?, asm_in_mm_last_N_day = ?, curr_rainfall = ?");
			sql.append(", rf_forecast_1 = ?, rf_forecast_2 = ?, rf_forecast_3 = ?, rf_forecast_4 = ?");
			sql.append(", rf_forecast_5 = ?, rf_forecast_6 = ?, rf_forecast_7 = ?, rf_last_N_days = ?");
			sql.append(", potential_evapotranspiration = ? , eto_last_N_days = ?");
			sql.append(", eto_forecast_N_days = ?, update_ts = ?");
			sql.append(" where village_water_available_data_id = ? and deleted = 0");

			ps = dataStore.createPreparedStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);
			batchRowCount = 0;
			for (IVillageWaterAvailableData vwaData : updateVillageWaterAvailableData) {

				ps.setLong(1, vwaData.getVillageCode());
				ps.setInt(2, vwaData.getModelDate());
				ps.setDouble(3, vwaData.getAvailableSM());
				ps.setInt(4, vwaData.getSoilMoistureDate()); // SMCS-135
				ps.setDouble(5, vwaData.getLastNDayASMInMM());
				ps.setDouble(6, vwaData.getCurrentRainfall());
				ps.setDouble(7, vwaData.getRFForecastData()[0]);
				ps.setDouble(8, vwaData.getRFForecastData()[1]);
				ps.setDouble(9, vwaData.getRFForecastData()[2]);
				ps.setDouble(10, vwaData.getRFForecastData()[3]);
				ps.setDouble(11, vwaData.getRFForecastData()[4]);
				ps.setDouble(12, vwaData.getRFForecastData()[5]);
				// TODO: The RF forecast is only for 6 days - to be fixed
				ps.setDouble(13, -1.0); // vwaData.getRFForecastData()[6]);
				
				// inserting rain fall of  last days in ascending order IWM-617, IWM-614
				Map<Integer, Double>   rfData = vwaData.getLastNDaysRF();
				SortedSet<Integer> modelDateSortedList = new TreeSet<Integer>(rfData.keySet());
				String	rainfalldata = "";
				for(int date:modelDateSortedList){
					rainfalldata += rfData.get(date)+"|";
				}
				
				// inserting ETO of  last days in ascending order 
				Map<Integer, String>   etoData = vwaData.getLastNDaysETo();
				String	etoDataString = "";
				for(int date:modelDateSortedList){
					if(null!= etoData && null != etoData.get(date)){
					etoDataString +=  etoData.get(date) +"|";
					}else {
						etoDataString += 0 +"|";
					}
				}
				ps.setString(14, rainfalldata);
				ps.setDouble(15, vwaData.getPotentialEvapotranspiration());
				ps.setString(16, etoDataString);
				
				modelDateSortedList = null;	
				String	etoForecastNDayString = String.valueOf(0) ;
				Map<Integer, String> etoForecastDayMap = vwaData.getNDaysForecastETo();
				if(null != etoForecastDayMap && etoForecastDayMap.keySet().size() > 0){
					
					modelDateSortedList  = new TreeSet<Integer>(etoForecastDayMap.keySet());
					
					for(int date:modelDateSortedList){
						
						etoForecastNDayString += etoForecastDayMap.get(date)+"|";
					}
				}
				ps.setString(17, etoForecastNDayString);
				ps.setLong(18, DataStoreContext.getCurrentTS());
				ps.setLong(19, vwaData.getVillageWaterAvailableDataId());
				
				ps.addBatch();
				
				++batchRowCount;
				
				if (batchRowCount % 500 == 0) {
					ps.executeBatch();
					batchRowCount = 0;
				}
			}
			
			if (batchRowCount > 0) {
				ps.executeBatch();
			}
			dataStore.commitTransaction(transactionId);
			result = true;
		} catch (SQLException e) {
			System.out.println("Error inserting or updating Village Water Available Data - " + e.getMessage());
			throw new DSPException("Error inserting or updating Village Water Available  Data", e);
		} finally {
			if (!result) {
				dataStore.rollbackTransaction(transactionId);
			}
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				rs = null;
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				ps = null;
			}
			DataStoreContext.clearDataStoreContext(dataStoreOwnerKey);
		}
	}
	
	/**
	 * Returns all {@link ICropSownData} for input modelDate
	 * Uses the monsoon year field to retrieve {@link ICropSownData}
	 * 
	 * Map of cropSownDataId and {@link ICropSownData}
	 * 
	 * @param modelDate
	 * @return
	 * @throws DSPException 
	 */
	@Override
	public Map<Long, ICropSownData> getAllCropSownDataByModelData(int modelDate, List<String> cropNamesList)
		throws DSPException {

		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		
		Map<Long, ICropSownData> allCropSownData = new HashMap<Long, ICropSownData>();
		
		if (cropNamesList == null) {
			if (IS_DEBUG_ENABLED) {
				System.out.println("ZBXXX : CSFDDSP : getAllCropSownDataByModelData - "
					+ "Empty Crop Names List received");
			}
			return allCropSownData;
		}
		
		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
			
			StringBuffer sql = new StringBuffer();
			sql.append("select *");
			sql.append(" from crop_sown_data");
			sql.append(" where avg_sowing_date <= ? and expected_harvest_date >= ? and total_area_sown > 0 and crop_name in ("
						+ StringUtils.commaSeparatedQuotedStringsForSQL(cropNamesList) + ") and (deleted = 0 or deleted is null)");

			ps = dataStore.createPreparedStatement(sql.toString());
			ps.setInt(1, modelDate);
			ps.setInt(2, modelDate);
			
			rs = ps.executeQuery();
			while (rs.next()) {
				ICropSownData csData = new CropSownData();
				
				csData.setDataId(rs.getLong("crop_sown_data_id"));
				csData.setVillageCode(rs.getInt("village_code"));
				csData.setPeriodName(rs.getString("period_name").toUpperCase());
				csData.setBucketName(rs.getString("bucket_name"));
				csData.setCropName(rs.getString("crop_name").toUpperCase());
				csData.setTotalAreaSown(rs.getDouble("total_area_sown"));
				csData.setSowingDate(rs.getInt("avg_sowing_date"));
				csData.setExpectedHarvestDate(rs.getInt("expected_harvest_date"));
				csData.setFirstSowingDate(rs.getInt("first_sowing_date"));
				csData.setLastSowingDate(rs.getInt("last_sowing_date"));
				csData.setMonsoonYear(rs.getInt("monsoon_year"));
				csData.setCropYear(rs.getInt("crop_year"));
				csData.setInsertTs(rs.getLong("insert_ts"));
				csData.setUpdateTs(rs.getLong("update_ts"));
				csData.setDeleted(rs.getInt("deleted"));
				csData.setUserSessionId(rs.getLong("user_session_id"));
				  
				allCropSownData.put(csData.getDataId(),  csData);
			}

			return allCropSownData;
		} catch (SQLException se) {
			// TODO: Log and throw the right exception
			System.out.println("Error retrieving all ICropSownData for model date : " + modelDate + " --- " + se.getMessage());
			throw new FetchingObjectException(
				"Error retrieving all ICropSownData for model date : " + modelDate, se);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				rs = null;
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				ps = null;
			}
			DataStoreContext.clearDataStoreContext(dataStoreOwnerKey);
		}
	}

	@Override
	public Map<Long, Double> getLastSevenDaysVillageET0(int modelDate, List<Long> villageCodeList)
		throws DSPException {

		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		Map<Long, Double> villageCodeET0Map = new HashMap<Long, Double>();
		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
			long sevenDaysBackTs = DateUtils.getModelDateInMillis(Integer.valueOf(modelDate));
			// Using modelDate format - yyyyMMdd.
			int sevenDaysBackDate = Integer.valueOf(DateUtils.getDateInFormat("yyyyMMdd", DateUtils.getLastNDaysTs(sevenDaysBackTs, 7)));
			
			StringBuffer sql = new StringBuffer();
			sql.append("select village_code, sum(potential_evapotranspiration) as sumET0 "); 
			sql.append("from village_water_available_data ");
			sql.append("where model_date >= ? and model_date <= ?  ");
			if (villageCodeList != null && !villageCodeList.isEmpty()) {
				sql.append("and village_code in (" + StringUtils.commaSeparatedStringForSQL(villageCodeList) + ") ");
			}
			sql.append("and deleted = 0 ");
			sql.append(" group by village_code;");
			
			ps = dataStore.createPreparedStatement(sql.toString());
			ps.setInt(1, sevenDaysBackDate);
			ps.setInt(2, modelDate);
			rs = ps.executeQuery();
			while (rs.next()) {
				villageCodeET0Map.put(rs.getLong("village_code"), rs.getDouble("sumET0"));
			}
			return villageCodeET0Map;
		} catch (SQLException se) {
			// TODO: Log and throw the right exception
			System.out.println("Error retrieving all potential_evapotranspiration data for model date : " 
			+ modelDate + " --- and villageCodes : " + villageCodeList + " --- " + se.getMessage());
			throw new FetchingObjectException(
				"Error retrieving all potential_evapotranspiration for model date : " + modelDate 
				+ " --- and villageCodes : " + villageCodeList, se);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				rs = null;
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				ps = null;
			}
			DataStoreContext.clearDataStoreContext(dataStoreOwnerKey);
		}
	}

	@Override
	public List<Long> insertCropSownData(List<ICropSownData> cropSownDatalist) throws DSPException {
		String dataStoreOwnerKey = null;
		String transactionId = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		boolean result = false;
		int counter=0;
		List<Long> resultantKeys = new ArrayList<Long>();
		
		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
			if (IS_DEBUG_ENABLED) {
				System.out.println("CSForecastDataDSP - Info : Transaction Started");
			}
			transactionId = dataStore.beginTransaction();
			
			StringBuffer sql = new StringBuffer();			
			sql.append("INSERT INTO crop_sown_data "
					+ "(village_code, period_name, bucket_name, "
					+ "crop_name, total_area_sown, avg_sowing_date, crop_year, "
					+ "expected_harvest_date, first_sowing_date, last_sowing_date, "
					+ "monsoon_year, insert_ts, deleted, user_session_id)VALUES "
					+ "(?, ?, ?, "
					+ "?, ?, ?, ?, "
					+ "?, ?, ?, "
					+ "?, ?, ?, ?)");
			
			ps = dataStore.createPreparedStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);
			
		
			for(ICropSownData cropSownData : cropSownDatalist){
				try{
					int monsoonYear = DateUtils.getMonsoonYear(cropSownData.getSowingDate()/100);
					
					ps.setInt(1, cropSownData.getVillageCode());
					ps.setString(2, cropSownData.getPeriodName());
					ps.setString(3, cropSownData.getBucketName());
					
					ps.setString(4, cropSownData.getCropName());
					ps.setDouble(5, cropSownData.getTotalAreaSown());
					ps.setInt(6, cropSownData.getSowingDate());
					ps.setInt(7, cropSownData.getCropYear());
					
					ps.setInt(8, cropSownData.getExpectedHarvestDate());
					//Setting first_sown_date and last_sown_date
					ps.setInt(9, cropSownData.getSowingDate());
					ps.setInt(10, cropSownData.getSowingDate());
					
					ps.setInt(11, monsoonYear);
					ps.setLong(12, DataStoreContext.getCurrentTS());
					ps.setInt(13, cropSownData.getDeleted());
					ps.setLong(14, cropSownData.getUserSessionId());
					
					ps.addBatch();
				
					if((++counter%1000)==0){
						ps.executeBatch();
						if (IS_DEBUG_ENABLED) {
							System.out.println("CropStressForecastDSP : insertCropSownData - inserted a batch successfully ");
						}
						rs = ps.getGeneratedKeys();
						while(rs.next()){
							resultantKeys.add(rs.getLong(1));
						}
					}
					
				}catch (SQLException e) {
					System.out.println("CropStressForecastDSP : insertCropSownData - Error inserting Crop Sown Data record with object, "+e.getMessage());
					throw new DSPException("CropStressForecastDSP : insertCropSownData - Error inserting  record with object, ",e);
				}
				
			}
			ps.executeBatch();
			rs.close();
			rs = ps.getGeneratedKeys();
			while(rs.next()){
				resultantKeys.add(rs.getLong(1));
			}
			dataStore.commitTransaction(transactionId);

			if (IS_DEBUG_ENABLED) {
				System.out.println("Info: Commit Transaction Is Completed");
			}
			result = true;
			
		} catch (SQLException e) {
			System.out.println("CropStressForecastDSP : insertCropSownData - Error inserting  record with object, "+e.getMessage());
			throw new DSPException("CropStressForecastDSP : insertCropSownData - Error inserting  record with object, ",e);
		}finally {
			if (!result) {
				dataStore.rollbackTransaction(transactionId);
			}
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				rs = null;
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				ps = null;
			}
			DataStoreContext.clearDataStoreContext(dataStoreOwnerKey);
			if (IS_DEBUG_ENABLED) {
				System.out.println("CropStressForecastDSP : insertCropSownData Info: DataStored Cleared");
			}	
		}
		return resultantKeys;
	}
	
	@Override
	public List<Integer> updateCropSownDataBasedOnCropDetails(List<ICropSownData> cropSownDataList)
			throws DSPException {
		// TODO Auto-generated method stub
		String dataStoreOwnerKey = null;
		String transactionId = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		boolean result = false;
		int[] returnCount;
		List<Integer> resultList = new ArrayList<Integer>();
		int batchCount = 0;
		
		try {
			dataStoreOwnerKey = DataStoreContext
					.initReusableDataStoreContext(dataStore);
			transactionId = dataStore.beginTransaction();
			
			StringBuffer sql = new StringBuffer();
			sql.append("UPDATE crop_sown_data"
					+ " SET village_code = ?,period_name = ?"
					+ ", bucket_name = ?,crop_name = ?"
					+ ", total_area_sown = ?,avg_sowing_date = ?"
					+ ", expected_harvest_date = ?,first_sowing_date = ?"
					+ ", last_sowing_date = ?, monsoon_year = ?, crop_year = ?"
					+ ", update_ts = ?, deleted = ?"
					+ " WHERE"
					+ " village_code = ? and period_name = ? and"
					+ " bucket_name = ? and crop_name = ? and"
					+ " crop_year = ?");
			
			ps = dataStore.createPreparedStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);
			for(ICropSownData cropSownData:cropSownDataList){
				ps.setInt(1, cropSownData.getVillageCode());
				ps.setString(2, cropSownData.getPeriodName());
				
				ps.setString(3, cropSownData.getBucketName());
				ps.setString(4, cropSownData.getCropName());
				
				ps.setDouble(5, cropSownData.getTotalAreaSown());
				ps.setInt(6, cropSownData.getSowingDate());
				
				ps.setInt(7, cropSownData.getExpectedHarvestDate());
				ps.setInt(8, cropSownData.getFirstSowingDate());
				
				ps.setInt(9, cropSownData.getLastSowingDate());
				ps.setInt(10, cropSownData.getMonsoonYear());
				ps.setInt(11, cropSownData.getCropYear());
				
				ps.setLong(12, DataStoreContext.getCurrentTS());
				ps.setInt(13, cropSownData.getDeleted());
				
				ps.setInt(14, cropSownData.getVillageCode());
				ps.setString(15, cropSownData.getPeriodName());
				
				ps.setString(16, cropSownData.getBucketName());
				ps.setString(17, cropSownData.getCropName());
				
				ps.setInt(18, cropSownData.getCropYear());
				
				
				
				ps.addBatch();
				batchCount++;
				
				if(batchCount >= 1000){
					 returnCount = ps.executeBatch();
					 if (returnCount.length != batchCount) {
                         // Error in updating
                     	//rollback the transaction
                     	dataStore.rollbackTransaction(transactionId);
        				if (IS_DEBUG_ENABLED) {
        					System.out.println("CropStressForecastDSP : updateCropSownDataBasedOnCropDetails - Error updating  records List, "+cropSownDataList);
        				}
                        throw new DSPException("CropStressForecastDSP : updateCropSownDataBasedOnCropDetails - Error updating  records,");
                     }
					 for(int i=0; i<returnCount.length; i++){
						 resultList.add(returnCount[i]);
					 }
				}
			}
			if(batchCount > 0){
				 returnCount = ps.executeBatch();
				 if (returnCount.length != batchCount) {
                     // Error in updating
                 	//rollback the transaction
                 	dataStore.rollbackTransaction(transactionId);
    				if (IS_DEBUG_ENABLED) {
    					System.out.println("CropStressForecastDSP : updateCropSownDataBasedOnCropDetails - Error updating  records, "+cropSownDataList);
    				}
    				throw new DSPException("CropStressForecastDSP : updateCropSownDataBasedOnCropDetails - Error updating  records ");
                 }
				 for(int i=0; i<returnCount.length; i++){
					 resultList.add(returnCount[i]);
				 }
			}
			dataStore.commitTransaction(transactionId);
			if (IS_DEBUG_ENABLED) {
				System.out.println("Info: Commit Transaction Is Completed");
			}
		} catch (SQLException e) {
			System.out.println("CropStressForecastDSP : updateCropSownDataBasedOnCropDetails - Error updating  record with object, ");
			throw new DSPException(" CropStressForecastDSP : updateCropSownDataBasedOnCropDetails -  Error updating  record ="+ e);
		} finally {
			if (!result) {
				dataStore.rollbackTransaction(transactionId);
			}
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				rs = null;
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				ps = null;
			}
			DataStoreContext.clearDataStoreContext(dataStoreOwnerKey);
			System.out.println("Info: DataStored Cleared");
		}
		return resultList;
	}
	
	@Override
	public Map<String, Map<String, List<ICropStressForecastData>>> getFarmCropStressData(
			int modelDate, List<Long> villageCodeList, List<String> cropNames,
			Map<Integer, ICropGrowthStageMD> cropGrowthStageMDMap,Map<Integer, ICropVillageLocData> allCropVillageLocData)
		throws DSPException{
		
		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		/* Using HashSet because contains() method takes O(1), unlike O(n) for a list 
		 * These are subsequently used for filtering records
		 * */
		HashSet<String> cropNameSet = new HashSet<>();
		if (cropNames != null && !cropNames.isEmpty()) {
			cropNameSet = new HashSet<String>(cropNames);
		}

		

		Map<String, Map<String, List<ICropStressForecastData>>> allVillageCSD = new HashMap<String, Map<String,List<ICropStressForecastData>>>();

		try {
			
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
			
			StringBuffer sql = new StringBuffer();
			sql.append("select *");
			sql.append(" from crop_stress_forecast_data cf join farm_data cs join  ");
			sql.append("(select * from village_water_available_data where village_water_available_data_id in ("+StringUtils.commaSeparatedStringForSQL(villageCodeList)+ ")) vw");
			sql.append(" on cf.crop_sown_data_id = cs.farm_data_id and cf.village_water_available_data_id = vw.village_water_available_data_id ");
			sql.append(" where cs.deleted = 0 and  cs.expected_harvest_year >= ? ");
			sql.append(" and cs.expected_harvest_date >= ? and cf.model_date = ? and vw.model_date = ? ");
			sql.append(" and cf.deleted = 0  and vw.deleted = 0 ");
			
			ps = dataStore.createPreparedStatement(sql.toString());
			ps.setInt(1, modelDate/10000);
			ps.setInt(2, modelDate);
			ps.setInt(3, modelDate);
			ps.setInt(4, modelDate);
			
			rs = ps.executeQuery();
			
			while (rs.next()) {
				// preparing village water available data 
				
				IVillageWaterAvailableData vwaData = new VillageWaterAvailableData();
				
				vwaData.setVillageWaterAvailableDataId(rs.getLong("village_water_available_data_id"));
				vwaData.setVillageCode(rs.getLong("village_code"));
				vwaData.setModelDate(rs.getInt("model_date"));
				vwaData.setAvailableSM(rs.getDouble("soil_moisture"));
				vwaData.setLastNDayASMInMM(rs.getDouble("asm_in_mm_last_N_day"));
				vwaData.setCurrentRainfall(rs.getDouble("curr_rainfall"));
				
				double[] rfForecast = new double[7];
				rfForecast[0] = rs.getDouble("rf_forecast_1");
				rfForecast[1] = rs.getDouble("rf_forecast_2");
				rfForecast[2] = rs.getDouble("rf_forecast_3");
				rfForecast[3] = rs.getDouble("rf_forecast_4");
				rfForecast[4] = rs.getDouble("rf_forecast_5");
				rfForecast[5] = rs.getDouble("rf_forecast_6");
				rfForecast[6] = rs.getDouble("rf_forecast_7");
				vwaData.setRFForecastData(rfForecast);
				// IWM-617 , IWM-614
				vwaData.setLastNDayASMInMM(rs.getDouble("asm_in_mm_last_N_day"));
				
							
				Map<Integer, Double>  last10DaysRF = new HashMap<Integer, Double>  ();
				int noOfDay = CropStressConstants.DEFAULT_SF3_COMPUTE_LAST_N_DAYS_SM_WINDOW;
				
				if(null != rs.getString("rf_last_N_days") && "" != rs.getString("rf_last_N_days")){
					String [] rfData = rs.getString("rf_last_N_days").split("\\|");	
					for(String data : rfData){
						last10DaysRF.put(DateUtils.substractDaysFromModelDate(vwaData.getModelDate(), noOfDay), Double.parseDouble(data));
						noOfDay--;
					}
				}
				
				
				
				Map<Integer, String>  etoLast10Days = new HashMap<Integer, String> ();
				noOfDay = CropStressConstants.DEFAULT_SF3_COMPUTE_LAST_N_DAYS_SM_WINDOW;
				if(null != rs.getString("eto_last_N_days") && ""!= rs.getString("eto_last_N_days")){
					String [] etoLastNdDayslist = rs.getString("eto_last_N_days").split("\\|");
					for(String ETO : etoLastNdDayslist){
						int date = DateUtils.substractDaysFromModelDate(vwaData.getModelDate(),noOfDay);
						etoLast10Days.put(date,ETO);
						noOfDay--;
					}
				}
				
				
				vwaData.setLastNDaysRF(last10DaysRF);
				vwaData.setLastNDaysETo(etoLast10Days);
				vwaData.setPotentialEvapotranspiration(rs.getDouble("potential_evapotranspiration"));
				
				String [] etoForecastNdDayslist = null;
				Map<Integer, String>  etoForecastDays = new HashMap<Integer, String> ();
				if( null != rs.getString("eto_forecast_N_days") && "" !=  rs.getString("eto_forecast_N_days")){
					 etoForecastNdDayslist = rs.getString("eto_forecast_N_days").split("\\|");
					
						noOfDay = 0;
						
						while(noOfDay <= CropStressConstants.DEFAULT_NO_OF_RAINFALL_FORECAST_DAYS){
							int date = Integer.parseInt(DateUtils.getNDayInFormat("yyyyMMdd",String.valueOf(vwaData.getModelDate()),noOfDay));
							if(etoForecastNdDayslist.length > noOfDay)
								etoForecastDays.put(date,etoForecastNdDayslist[noOfDay]);
							else
								etoForecastDays.put(date,String.valueOf(0));
							++noOfDay;
						}
				}
				
				vwaData.setNDaysForecastETo(etoForecastDays);
				// Preparing farm data ..
				
				IFarmData farmData = new FarmData();
				farmData.setDataId(rs.getLong("farm_data_id"));
				farmData.setVillageCode(rs.getInt("village_code"));				
				farmData.setMandalCode(rs.getInt("mandal_code"));				
				farmData.setDistrictCode(rs.getInt("district_code"));	
				farmData.setFarmerName(rs.getString("farmer_name"));
				farmData.setFatherName(rs.getString("father_name"));
				farmData.setAadharNo(rs.getString("aadhar_no"));
				farmData.setMobileNo(rs.getString("mobile_no"));
				farmData.setKhataNo(rs.getString("khata_no"));
				farmData.setOccupantExtent(rs.getDouble("occupant_extent"));
				farmData.setTotalAreaSown(rs.getDouble("total_extent"));
				farmData.setSurveyNo(rs.getString("survey_no"));
				farmData.setCategoryOfFarmer(rs.getString("category_of_farmer"));
		    	farmData.setCropName(rs.getString("crop_name").toUpperCase());		    	
		    	farmData.setTotalAreaSown(rs.getDouble("area_sown"));		    	
		    	farmData.setSowingDate(rs.getInt("sowing_date"));		 
		    	farmData.setSourceOfIrrigation(rs.getString("source_of_irrigation"));
		    	farmData.setExpectedHarvestDate(rs.getInt("expected_harvest_date"));
		    	farmData.setExpectedHarvestedYear(rs.getInt("expected_harvest_year"));
		    	farmData.setPeriodName(rs.getString("period_name").toUpperCase());		    	
		    	farmData.setMonsoonYear(rs.getInt("monsoon_year"));
		    	farmData.setCropYear(rs.getInt("crop_year"));
		    	farmData.setLatitude(rs.getDouble("latitude"));
		    	farmData.setLongitude(rs.getDouble("longitude"));
		    	farmData.setInsertTs(rs.getLong("insert_ts"));
				farmData.setUpdateTs(rs.getLong("update_ts"));
				farmData.setDeleted(rs.getInt("deleted"));
				farmData.setUserSessionId(rs.getInt("user_session_id"));
				
				
				ICropStressForecastData csfData = new CropStressForecastData();
				
				csfData.setCropStressForecastDataId(rs.getLong("crop_stress_forecast_data_id"));
				csfData.setCropGrowthStageMD(cropGrowthStageMDMap.get(rs.getInt("crop_stage_id")));
				csfData.setCropSownDataSource(rs.getShort("crop_sown_data_source"));
				csfData.setVillageWaterAvailableData(vwaData);
				csfData.setModelDate(modelDate);
				csfData.setCummWaterSupplied(rs.getDouble("cumm_water_supplied"));
				csfData.setOptimalWaterReq(rs.getDouble("optimal_water_req"));
				csfData.setMinimalWaterReq(rs.getDouble("minimal_water_req"));
				csfData.setOWRStressFactor1(rs.getDouble("owr_stress_factor_1"));
				csfData.setOWRStressFactor2(rs.getDouble("owr_stress_factor_2"));
				csfData.setOWRStressFactor3(rs.getDouble("owr_stress_factor_3"));
				csfData.setOWRStressFactor4(rs.getDouble("owr_stress_factor_4"));
				csfData.setOWRCropUnderStress(rs.getInt("owr_is_crop_under_stress") == 1 ? true : false);
				csfData.setMWRStressFactor1(rs.getDouble("mwr_stress_factor_1"));
				csfData.setMWRStressFactor2(rs.getDouble("mwr_stress_factor_2"));
				csfData.setMWRStressFactor3(rs.getDouble("mwr_stress_factor_3"));
				csfData.setMWRStressFactor4(rs.getDouble("mwr_stress_factor_4"));
				csfData.setMWRCropUnderStress(rs.getInt("mwr_is_crop_under_stress") == 1 ? true : false);
				csfData.setSownData(farmData);
				csfData.setTodaysAETc(rs.getDouble("curr_actual_etc"));
				csfData.setTodaysPETc(rs.getDouble("curr_potential_etc"));
				csfData.setCumulativeAETc(rs.getDouble("cumm_actual_etc"));
				csfData.setCumulativePETc(rs.getDouble("cumm_potential_etc"));
				csfData.setEstimatedYield(rs.getDouble("estimated_yield"));
				csfData.setSoilWaterCriticalLevel(rs.getDouble("soil_water_critical_level"));
				
				if (csfData.getSownData() == null) {
					/* This condition will happen, if we compute for irrigated crops
					 * Just an extra measure to make sure that we don't have any computed data for irrigated crops
					 * */
					continue;
				}
				Set <String> pestnames = null;
				String pest = rs.getString("pest_names");
				if(null != pest && pest.contains("|")){
					pestnames = new HashSet<>(Arrays.asList(pest.split("\\|")));
				}
				
				csfData.setPestNames(pestnames);
				csfData.setOwrNext7Days(rs.getDouble("owr_next_7_days"));
				csfData.setMwrNext7Days(rs.getDouble("mwr_next_7_days"));
				csfData.setWaterAvailableNext7Days(rs.getDouble("water_available_next_7_days"));
				
				// SMCS-153,154 Store computed ASM
				
				Map<Integer, Double>  last10DaysComputedASM = new HashMap<>();
				noOfDay = CropStressConstants.DEFAULT_SF3_COMPUTE_LAST_N_DAYS_SM_WINDOW;
				
				String[] asmData = null ;
				if(null != rs.getString("last_n_days_computed_asm") && "" != rs.getString("last_n_days_computed_asm") ){
					asmData = rs.getString("last_n_days_computed_asm").split("\\|");
					for(String data : asmData) {
						last10DaysComputedASM.put(DateUtils.substractDaysFromModelDate(csfData.getModelDate(), noOfDay), Double.parseDouble(data));
						noOfDay--;
					}
				}
				
				csfData.setLast10DaysComputedASM(last10DaysComputedASM);
				csfData.setInsertTs(rs.getLong("insert_ts"));
				csfData.setUpdateTs(rs.getLong("update_ts"));
				csfData.setDeleted(rs.getInt("deleted"));
				csfData.setUserSessionId(rs.getLong("user_session_id"));

				String villageName = allCropVillageLocData.get(csfData.getSownData().getVillageCode()).getVillageFullName();
				String cropNameTmp = csfData.getSownData().getCropName();
				
				if (cropNameSet != null
						&& !cropNameSet.contains(cropNameTmp)) {
					continue;
				}

				Map<String, List<ICropStressForecastData>> villageCSData = allVillageCSD.get(villageName);
				if (villageCSData == null) {
					villageCSData = new HashMap<String, List<ICropStressForecastData>>();
					allVillageCSD.put(villageName, villageCSData);
				}
				List<ICropStressForecastData> cropStressDataList = villageCSData.get(cropNameTmp);
				if (cropStressDataList == null) {
					cropStressDataList = new ArrayList<ICropStressForecastData>();
					villageCSData.put(cropNameTmp, cropStressDataList);
				}
				cropStressDataList.add(csfData);
			}
			
			return allVillageCSD;
		} catch (SQLException se) {
			// TODO: Log and throw the right exception
			System.out.println("Error retrieving all ICropStressForecastData for model date : " + modelDate
					+ " -- villageCodeList : " + villageCodeList + " -- cropNamesList : " + cropNames + " --- "
					+ se.getMessage());
			throw new FetchingObjectException("Error retrieving all ICropStressForecastData for model date : "
					+ modelDate + " -- villageCodeList : " + villageCodeList + " -- cropNamesList : " + cropNames, se);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				rs = null;
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				ps = null;
			}
			DataStoreContext.clearDataStoreContext(dataStoreOwnerKey);
		}
		
		
	}

	@Override
	public List<ICropStressForecastData> getFarmCropStressDataForVillage(int modelDate, 
			int villageCode, String cropName, Map<Integer, ICropGrowthStageMD> cropGrowthStageMDMap) 
			throws DSPException{
		
		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		List<ICropStressForecastData> allVillageCSD = new ArrayList<>();

		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
			
			StringBuffer sql = new StringBuffer();
			
			sql.append("select *");
			sql.append(" from crop_stress_forecast_data cf join farm_data cs join village_water_available_data vw ");
			sql.append(" on cf.crop_sown_data_id = cs.farm_data_id and cf.village_water_available_data_id = vw.village_water_available_data_id where cs.village_code = vw.village_code ");
			sql.append(" and cs.deleted = 0 and cs.expected_harvest_year >= ? and cs.source_of_irrigation = ? and cs.crop_name = ? and cs.village_code = ? and cs.expected_harvest_date >= ? ");
			sql.append(" and cf.model_date = ? and vw.model_date = ? and cf.deleted = 0  and vw.deleted = 0 ");
						
			ps = dataStore.createPreparedStatement(sql.toString());
			ps.setInt(1, modelDate/10000);
			ps.setString(2, CropStressConstants.RAIN_FED_IRRIGATION_SOURCE);
			ps.setString(3, cropName);
			ps.setInt(4, villageCode);
			ps.setInt(5, modelDate);
			ps.setInt(6, modelDate);
			ps.setInt(7, modelDate);
			
			rs = ps.executeQuery();
			while (rs.next()) {
				// preparing village water available data 
				
				IVillageWaterAvailableData vwaData = new VillageWaterAvailableData();
				
				vwaData.setVillageWaterAvailableDataId(rs.getLong("village_water_available_data_id"));
				vwaData.setVillageCode(rs.getLong("village_code"));
				vwaData.setModelDate(rs.getInt("model_date"));
				vwaData.setAvailableSM(rs.getDouble("soil_moisture"));
				vwaData.setLastNDayASMInMM(rs.getDouble("asm_in_mm_last_N_day"));
				vwaData.setCurrentRainfall(rs.getDouble("curr_rainfall"));
				vwaData.setSoilMoistureDate(rs.getInt("soil_moisture_date"));
				
				double[] rfForecast = new double[7];
				rfForecast[0] = rs.getDouble("rf_forecast_1");
				rfForecast[1] = rs.getDouble("rf_forecast_2");
				rfForecast[2] = rs.getDouble("rf_forecast_3");
				rfForecast[3] = rs.getDouble("rf_forecast_4");
				rfForecast[4] = rs.getDouble("rf_forecast_5");
				rfForecast[5] = rs.getDouble("rf_forecast_6");
				rfForecast[6] = rs.getDouble("rf_forecast_7");
				vwaData.setRFForecastData(rfForecast);
				// IWM-617 , IWM-614
				vwaData.setLastNDayASMInMM(rs.getDouble("asm_in_mm_last_N_day"));
				
							
				Map<Integer, Double>  last10DaysRF = new HashMap<Integer, Double>  ();
				int noOfDay = CropStressConstants.DEFAULT_SF3_COMPUTE_LAST_N_DAYS_SM_WINDOW;
				
				if(null != rs.getString("rf_last_N_days") && "" != rs.getString("rf_last_N_days")){
					String [] rfData = rs.getString("rf_last_N_days").split("\\|");	
					for(String data : rfData){
						last10DaysRF.put(DateUtils.substractDaysFromModelDate(vwaData.getModelDate(), noOfDay), Double.parseDouble(data));
						noOfDay--;
					}
				}
				
				Map<Integer, String>  etoLast10Days = new HashMap<Integer, String> ();
				noOfDay = CropStressConstants.DEFAULT_SF3_COMPUTE_LAST_N_DAYS_SM_WINDOW;
				if(null != rs.getString("eto_last_N_days") && ""!= rs.getString("eto_last_N_days")){
					String [] etoLastNdDayslist = rs.getString("eto_last_N_days").split("\\|");
					for(String ETO : etoLastNdDayslist){
						int date = DateUtils.substractDaysFromModelDate(vwaData.getModelDate(),noOfDay);
						etoLast10Days.put(date,ETO);
						noOfDay--;
					}
				}
				
				vwaData.setLastNDaysRF(last10DaysRF);
				vwaData.setLastNDaysETo(etoLast10Days);
				vwaData.setPotentialEvapotranspiration(rs.getDouble("potential_evapotranspiration"));
				
				String [] etoForecastNdDayslist = null;
				Map<Integer, String>  etoForecastDays = new HashMap<Integer, String> ();
				if( null != rs.getString("eto_forecast_N_days") && "" !=  rs.getString("eto_forecast_N_days")){
					 etoForecastNdDayslist = rs.getString("eto_forecast_N_days").split("\\|");
					
						noOfDay = 0;
						
						while(noOfDay <= CropStressConstants.DEFAULT_NO_OF_RAINFALL_FORECAST_DAYS){
							int date = Integer.parseInt(DateUtils.getNDayInFormat("yyyyMMdd",String.valueOf(vwaData.getModelDate()),noOfDay));
							if(etoForecastNdDayslist.length > noOfDay)
								etoForecastDays.put(date,etoForecastNdDayslist[noOfDay]);
							else
								etoForecastDays.put(date,String.valueOf(0));
							++noOfDay;
						}
				}
				
				vwaData.setNDaysForecastETo(etoForecastDays);

				IFarmData farmData = new FarmData();
				farmData.setDataId(rs.getLong("farm_data_id"));
				farmData.setVillageCode(rs.getInt("village_code"));				
				farmData.setMandalCode(rs.getInt("mandal_code"));				
				farmData.setDistrictCode(rs.getInt("district_code"));	
				farmData.setFarmerName(rs.getString("farmer_name"));
				farmData.setFatherName(rs.getString("father_name"));
				farmData.setAadharNo(rs.getString("aadhar_no"));
				farmData.setMobileNo(rs.getString("mobile_no"));
				farmData.setKhataNo(rs.getString("khata_no"));
				farmData.setOccupantExtent(rs.getDouble("occupant_extent"));
				farmData.setTotalAreaSown(rs.getDouble("total_extent"));
				farmData.setSurveyNo(rs.getString("survey_no"));
				farmData.setCategoryOfFarmer(rs.getString("category_of_farmer"));
		    	farmData.setCropName(rs.getString("crop_name").toUpperCase());		    	
		    	farmData.setTotalAreaSown(rs.getDouble("area_sown"));		    	
		    	farmData.setSowingDate(rs.getInt("sowing_date"));		 
		    	farmData.setSourceOfIrrigation(rs.getString("source_of_irrigation"));
		    	farmData.setExpectedHarvestDate(rs.getInt("expected_harvest_date"));
		    	farmData.setExpectedHarvestedYear(rs.getInt("expected_harvest_year"));
		    	farmData.setPeriodName(rs.getString("period_name").toUpperCase());		    	
		    	farmData.setMonsoonYear(rs.getInt("monsoon_year"));
		    	farmData.setCropYear(rs.getInt("crop_year"));
		    	farmData.setLatitude(rs.getDouble("latitude"));
		    	farmData.setLongitude(rs.getDouble("longitude"));
		    	farmData.setInsertTs(rs.getLong("insert_ts"));
				farmData.setUpdateTs(rs.getLong("update_ts"));
				farmData.setDeleted(rs.getInt("deleted"));
				farmData.setUserSessionId(rs.getInt("user_session_id"));
				
				
				ICropStressForecastData csfData = new CropStressForecastData();
				

				csfData.setCropStressForecastDataId(rs.getLong("crop_stress_forecast_data_id"));
				csfData.setCropGrowthStageMD(cropGrowthStageMDMap.get(rs.getInt("crop_stage_id")));
				csfData.setCropSownDataSource(rs.getShort("crop_sown_data_source"));
				csfData.setVillageWaterAvailableData(vwaData);
				csfData.setModelDate(modelDate);
				csfData.setCummWaterSupplied(rs.getDouble("cumm_water_supplied"));
				csfData.setOptimalWaterReq(rs.getDouble("optimal_water_req"));
				csfData.setMinimalWaterReq(rs.getDouble("minimal_water_req"));
				csfData.setOWRStressFactor1(rs.getDouble("owr_stress_factor_1"));
				csfData.setOWRStressFactor2(rs.getDouble("owr_stress_factor_2"));
				csfData.setOWRStressFactor3(rs.getDouble("owr_stress_factor_3"));
				csfData.setOWRStressFactor4(rs.getDouble("owr_stress_factor_4"));
				csfData.setOWRCropUnderStress(rs.getInt("owr_is_crop_under_stress") == 1 ? true : false);
				csfData.setMWRStressFactor1(rs.getDouble("mwr_stress_factor_1"));
				csfData.setMWRStressFactor2(rs.getDouble("mwr_stress_factor_2"));
				csfData.setMWRStressFactor3(rs.getDouble("mwr_stress_factor_3"));
				csfData.setMWRStressFactor4(rs.getDouble("mwr_stress_factor_4"));
				csfData.setMWRCropUnderStress(rs.getInt("mwr_is_crop_under_stress") == 1 ? true : false);
				csfData.setSownData(farmData);
				csfData.setTodaysAETc(rs.getDouble("curr_actual_etc"));
				csfData.setTodaysPETc(rs.getDouble("curr_potential_etc"));
				csfData.setCumulativeAETc(rs.getDouble("cumm_actual_etc"));
				csfData.setCumulativePETc(rs.getDouble("cumm_potential_etc"));
				csfData.setEstimatedYield(rs.getDouble("estimated_yield"));
				csfData.setSoilWaterCriticalLevel(rs.getDouble("soil_water_critical_level"));
				
				if (csfData.getSownData() == null) {
					/* This condition will happen, if we compute for irrigated crops
					 * Just an extra measure to make sure that we don't have any computed data for irrigated crops
					 * */
					continue;
				}
				
				Set <String> pestnames = null;
				String pest = rs.getString("pest_names");
				if(null != pest && pest.contains("|")){
					pestnames = new HashSet<>(Arrays.asList(pest.split("\\|")));
				}
				
				csfData.setPestNames(pestnames);
				csfData.setOwrNext7Days(rs.getDouble("owr_next_7_days"));
				csfData.setMwrNext7Days(rs.getDouble("mwr_next_7_days"));
				csfData.setWaterAvailableNext7Days(rs.getDouble("water_available_next_7_days"));
				
				// SMCS-153,154 Store computed ASM
				
				Map<Integer, Double>  last10DaysComputedASM = new HashMap<>();
				
				String[] asmData = null ;
				noOfDay = CropStressConstants.DEFAULT_SF3_COMPUTE_LAST_N_DAYS_SM_WINDOW; 
				if(null != rs.getString("last_n_days_computed_asm") && "" != rs.getString("last_n_days_computed_asm") ){
					asmData = rs.getString("last_n_days_computed_asm").split("\\|");
					for(String data : asmData) {
						last10DaysComputedASM.put(DateUtils.substractDaysFromModelDate(csfData.getModelDate(), noOfDay), Double.parseDouble(data));
						noOfDay--;
					}
				}
				
				csfData.setLast10DaysComputedASM(last10DaysComputedASM);
				csfData.setInsertTs(rs.getLong("insert_ts"));
				csfData.setUpdateTs(rs.getLong("update_ts"));
				csfData.setDeleted(rs.getInt("deleted"));
				csfData.setUserSessionId(rs.getLong("user_session_id"));

				allVillageCSD.add(csfData);
			}

			return allVillageCSD;
			
		} catch (SQLException se) {
			// TODO: Log and throw the right exception
			System.out.println("Error retrieving all ICropStressForecastData for [ model date = "
					+ "" + modelDate + ", villageCode  = " + villageCode + ", cropName : " + cropName + " ] " + se.getMessage());
			throw new FetchingObjectException("Error retrieving all ICropStressForecastData for [ model date = "
					+ "" + modelDate + ", villageCode  = " + villageCode + ", cropName : " + cropName + " ] ", se);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				rs = null;
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				ps = null;
			}
			DataStoreContext.clearDataStoreContext(dataStoreOwnerKey);
		}
		
	}
	
	@Override
	public void insertOrUpdateCropStressPropagatedData(List<ICropStressPropagatedData> newCropStressPropagatedData,
			List<ICropStressPropagatedData> updateCropStressPropagatedData) throws DSPException {

		/* 
		 * 1. Insert new records in batches of 1000
		 * 2. Update existing records in batches of 1000
		 * 3. Commit all at once
		 * */

		String dataStoreOwnerKey = null;
		String transactionId = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		boolean result = false;
		
		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
			transactionId = dataStore.beginTransaction();

			StringBuffer sql = new StringBuffer();			
			sql.append("insert into crop_stress_propagated_data");
			sql.append(" (location_full_name, nrsc_location_full_name, model_date, crop_name");
			sql.append(", critical_stage_count, critical_stress_count, moderate_stress_count, pest_alert_count");
			sql.append(", pest_names, asm_in_mm, owr_next_7_days, mwr_next_7_days, water_available_next_7_days");
			sql.append(", insert_ts, update_ts, deleted, user_session_id");
			sql.append(") values (");
			sql.append(" ?, ?, ?, ?");
			sql.append(", ?, ?, ?, ?, ?");
			sql.append(", ?, ?, ?, ?");
			sql.append(", ?, null, 0, 0);");

			ps = dataStore.createPreparedStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);

			int batchRowCount = 0;
			for (ICropStressPropagatedData forecastData : newCropStressPropagatedData) {
				
				ps.setString(1,forecastData.getLocationFullName());
				ps.setString(2,forecastData.getNrscLocationFullName());
				ps.setInt(3, forecastData.getModelDate());
				ps.setString(4, forecastData.getCropName());
				ps.setInt(5, forecastData.getCriticalStageCount());
				ps.setInt(6, forecastData.getCriticalStressCount());
				ps.setInt(7, forecastData.getModerateStressCount());
				ps.setInt(8, forecastData.getPestAlertCount());
				String pestNames = "";
				if (null != forecastData.getPestNames()) {
					for(String pest : forecastData.getPestNames()) {
						pestNames += pest +"|";
					}
				}
				ps.setString(9,pestNames);
				ps.setDouble(10, forecastData.getAvailableSoilMoistureInMM());
				ps.setDouble(11, forecastData.getWaterReqProductiveNext7Days());
				ps.setDouble(12, forecastData.getWaterReqProtectiveNext7Days());
				ps.setDouble(13, forecastData.getWaterAvailableNext7Days());
				ps.setLong(14, DataStoreContext.getCurrentTS());
				
				ps.addBatch();
				++batchRowCount;
				
				if (batchRowCount % 1000 == 0) {
					ps.executeBatch();
					batchRowCount = 0;
				}
			}
			// TODO: Update the cropStressPropagatedDataId post insert
			
			if (batchRowCount > 0) {
				ps.executeBatch();
			}
			ps.close();
			ps = null;
			
			sql = new StringBuffer();			
			sql.append("update crop_stress_propagated_data ");
			sql.append("set location_full_name = ?, nrsc_location_full_name = ?, model_date = ?, crop_name = ?");
			sql.append(", critical_stage_count = ?, critical_stress_count = ?, moderate_stress_count = ?, pest_alert_count = ?");
			sql.append(", pest_names = ?, asm_in_mm = ?, owr_next_7_days = ?, mwr_next_7_days = ?, water_available_next_7_days = ?");
			sql.append(", update_ts = ?, deleted = ?, user_session_id = ?");
			sql.append(" where crop_stress_propagated_data_id = ? and deleted = 0;");
			
			ps = dataStore.createPreparedStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);
			
			batchRowCount = 0;
			for (ICropStressPropagatedData forecastData : updateCropStressPropagatedData) {
				ps.setString(1,forecastData.getLocationFullName());
				ps.setString(2,forecastData.getNrscLocationFullName());
				ps.setInt(3, forecastData.getModelDate());
				ps.setString(4, forecastData.getCropName());
				ps.setInt(5, forecastData.getCriticalStageCount());
				ps.setInt(6, forecastData.getCriticalStressCount());
				ps.setInt(7, forecastData.getModerateStressCount());
				ps.setInt(8, forecastData.getPestAlertCount());
				
				String pestNames = "";
				if (null != forecastData.getPestNames()) {
					for(String pest : forecastData.getPestNames()) {
						pestNames += pest +"|";
					}
				}
				ps.setString(9,pestNames);
				ps.setDouble(10, forecastData.getAvailableSoilMoistureInMM());
				ps.setDouble(11, forecastData.getWaterReqProductiveNext7Days());
				ps.setDouble(12, forecastData.getWaterReqProtectiveNext7Days());
				ps.setDouble(13, forecastData.getWaterAvailableNext7Days());
				ps.setLong(14, DataStoreContext.getCurrentTS());
				ps.setLong(15, forecastData.getDeleted());
				ps.setLong(16, forecastData.getUserSessionId());
				ps.setLong(17,  forecastData.getCropStressPropagatedDataId());
				
				ps.addBatch();
				
				++batchRowCount;
				
				if (batchRowCount % 1000 == 0) {
					ps.executeBatch();
					batchRowCount = 0;
				}
			}
			
			if (batchRowCount > 0) {
				ps.executeBatch();
			}
				
			dataStore.commitTransaction(transactionId);
			result = true;
		} catch (SQLException e) {
			System.out.println("Error inserting or updating CropStress Propagated Forecast Data - " + e.getMessage());
			throw new DSPException("Error inserting or updating CropStress Propagated Forecast Data", e);
		} finally {
			if (!result) {
				dataStore.rollbackTransaction(transactionId);
			}
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				rs = null;
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				ps = null;
			}
			DataStoreContext.clearDataStoreContext(dataStoreOwnerKey);
		}
		
	}

	@Override
	public Map<String, Map<String, ICropStressPropagatedData>> getCropStressPropagatedData(int modelDate,
			List<String> locFullNamesList, List<String> cropNamesList) throws DSPException {
		
		
		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		Map<String, Map<String, ICropStressPropagatedData>> allCropPropagatedData = 
				new HashMap<String, Map<String, ICropStressPropagatedData>>();

		/* Using HashSet because contains() method takes O(1), unlinke O(n) for a list */
		HashSet<String> locFullNamesSet = new HashSet<>();
		HashSet<String> cropNameSet = new HashSet<>();

		if (locFullNamesList != null && !locFullNamesList.isEmpty()) {
			locFullNamesSet = new HashSet<String>(locFullNamesList);
		}

		if (cropNamesList != null && !cropNamesList.isEmpty()) {
			cropNameSet = new HashSet<String>(cropNamesList);
		}
		
		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
	
			StringBuffer sql = new StringBuffer();
			sql.append("select *");
			sql.append(" from crop_stress_propagated_data ");
			sql.append(" where model_date = ? and deleted = 0 ");
	
			ps = dataStore.createPreparedStatement(sql.toString());
			ps.setInt(1, modelDate);
	
			rs = ps.executeQuery();
			while (rs.next()) {
				ICropStressPropagatedData forecastData = new CropStressPropagatedData();
				forecastData.setCropStressPropagatedDataId(rs.getLong("crop_stress_propagated_data_id"));
				forecastData.setLocationFullName(rs.getString("location_full_name").toUpperCase());
				forecastData.setNrscLocationFullName(rs.getString("nrsc_location_full_name").toUpperCase());
				forecastData.setModelDate(rs.getInt("model_date"));
				forecastData.setCropName(rs.getString("crop_name").toUpperCase());
				forecastData.setCriticalStageCount(rs.getInt("critical_stage_count"));
				forecastData.setCriticalStressCount(rs.getInt("critical_stress_count"));
				forecastData.setModerateStressCount(rs.getInt("moderate_stress_count"));
				forecastData.setPestAlertCount(rs.getInt("pest_alert_count"));
				
				List <String> pestnames = null;
				String pest = rs.getString("pest_names");
				if(null != pest && pest.contains("|")){
					pestnames = Arrays.asList(pest.split("\\|"));
				}
		
				forecastData.setPestNames(pestnames);
				forecastData.setAvailableSoilMoistureInMM(rs.getDouble("asm_in_mm"));
				forecastData.setWaterReqProductiveNext7Days(rs.getDouble("owr_next_7_days"));
				forecastData.setWaterReqProtectiveNext7Days(rs.getDouble("mwr_next_7_days"));
				forecastData.setWaterAvailableNext7Days(rs.getDouble("water_available_next_7_days"));
				forecastData.setInsertTs(rs.getLong("insert_ts"));
				forecastData.setUpdateTs(rs.getLong("update_ts"));
				forecastData.setUserSessionId(rs.getLong("user_session_id"));
		
				String locationFN = forecastData.getLocationFullName();
				String cropName = forecastData.getCropName();
				
				if (!locFullNamesSet.isEmpty() && !locFullNamesSet.contains(locationFN)) {
					continue;
				}
				
				if (!cropNameSet.isEmpty() && !cropNameSet.contains(cropName)) {
					continue;
				}

				Map<String, ICropStressPropagatedData> locationCSDataMap = allCropPropagatedData.get(locationFN);
				if (locationCSDataMap == null) {
					locationCSDataMap = new HashMap<String, ICropStressPropagatedData>();
					allCropPropagatedData.put(locationFN, locationCSDataMap);
				}
				locationCSDataMap.put(cropName, forecastData);
			}

			return allCropPropagatedData;
		} catch (SQLException se) {
			throw new FetchingObjectException("Error retrieving all ICropStressPropagatedData for model date : "
					+ modelDate + " -- villageNameList : " + locFullNamesList + " -- cropNamesList : " + cropNamesList, se);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				rs = null;
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				ps = null;
			}
			DataStoreContext.clearDataStoreContext(dataStoreOwnerKey);
		}
	}
	

	@Override
	public int getLastKnownDateForCSPropData(int referenceDate) throws DSPException {
		
		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
	
			StringBuffer sql = new StringBuffer();
			sql.append("select max(model_date) as model_date");
			sql.append(" from crop_stress_propagated_data");
			sql.append(" where model_date <= ? and (deleted = 0 or deleted is null)");
	
			ps = dataStore.createPreparedStatement(sql.toString());
			ps.setInt(1, referenceDate);
			
			rs = ps.executeQuery();
			int modelDate = 0;
			while (rs.next()) {
				modelDate = rs.getInt("model_date");
				
			}
			return modelDate;
		} catch (SQLException se) {
			throw new FetchingObjectException("CSDS : Error retrieving last known model date");
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				rs = null;
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				ps = null;
			}
			DataStoreContext.clearDataStoreContext(dataStoreOwnerKey);
		}
	}
	
	
	@Override
	public Map<Integer, Integer> getVillageToFarmCount() throws DSPException {

		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		Map<Integer, Integer> villageToFarmCount = new HashMap<>();

		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
			
			StringBuffer sql = new StringBuffer();
			sql.append("select village_code, count(*) as farm_count");
			sql.append(" from farm_data where deleted = 0");
			sql.append(" group by village_code");

			ps = dataStore.createPreparedStatement(sql.toString());

			rs = ps.executeQuery();
			
			while (rs.next()) {
				villageToFarmCount.put(rs.getInt("village_code"), rs.getInt("farm_count"));
			}

			return villageToFarmCount;

		} catch (SQLException se) {
			// TODO: Log and throw the right exception
			System.out.println("Error retrieving all village to farm count --- " + se.getMessage());
			throw new FetchingObjectException(
					"Error retrieving all village to farm count : ", se);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				rs = null;
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				ps = null;
			}
			DataStoreContext.clearDataStoreContext(dataStoreOwnerKey);
		}
	}
	
	@Override 
	public int getLastKnownVillageCropStressDataModelDate()
		throws DSPException {
				
		
		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		
		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
			
			StringBuffer sql = new StringBuffer();
			sql.append("select max(model_date) as model_date from  crop_stress_forecast_data ");
			
			ps = dataStore.createPreparedStatement(sql.toString());
			
			
			rs = ps.executeQuery();
			int cropStressDataAvailablemodelDate = 0 ;
			while (rs.next()) {
				cropStressDataAvailablemodelDate = rs.getInt("model_date");				
			}

			return cropStressDataAvailablemodelDate;
		} catch (SQLException se) {
			// TODO: Log and throw the right exception
			System.out.println("Error retrieving  Last computed crop stress forecast data model date ");
			throw new FetchingObjectException(
				" Error retrieving  Last computed crop stress forecast data model date ");
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				rs = null;
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				ps = null;
			}
			DataStoreContext.clearDataStoreContext(dataStoreOwnerKey);
		}
	}

	@Override 
	public int getLastKnownCropStressPropagatedDataModelDate()
		throws DSPException {
				
		
		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		
		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
			
			StringBuffer sql = new StringBuffer();
			sql.append("select max(model_date) as model_date from  crop_stress_propagated_data ");
			
			ps = dataStore.createPreparedStatement(sql.toString());
			
			
			rs = ps.executeQuery();
			int cropStressDataAvailablemodelDate = 0 ;
			while (rs.next()) {
				cropStressDataAvailablemodelDate = rs.getInt("model_date");				
			}

			return cropStressDataAvailablemodelDate;
		} catch (SQLException se) {
			// TODO: Log and throw the right exception
			System.out.println("Error retrieving  Last known model date for computed propagated data  ");
			throw new FetchingObjectException(
				" Error retrieving  Last known model date for computed propagated data ");
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				rs = null;
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				ps = null;
			}
			DataStoreContext.clearDataStoreContext(dataStoreOwnerKey);
		}
	}

 
	@Override
	public Map<Long, ICropStressForecastData> getCropETForFarms(Map<Long, Integer> farmIDToSowingDateMap, 
			int modelDate, int windowSize) 
			throws DSPException {
		
		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		Map<Long, ICropStressForecastData> allVillageCSD = new HashMap<>();
		List<Integer> lastNDaysDates = DateUtils.substractNDaysFromModelDate(modelDate, windowSize);

		try {
			
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
			
			StringBuffer sql = new StringBuffer();
			sql.append("SELECT crop_sown_data_id, model_date, cumm_actual_etc, cumm_potential_etc");
			sql.append(" FROM crop_stress_forecast_data");
			sql.append(" WHERE crop_sown_data_id IN ("+StringUtils.commaSeparatedListForSQLForLongValues(farmIDToSowingDateMap.keySet())+") ");
			sql.append(" AND model_date in ("+StringUtils.commaSeparatedListForSQL(lastNDaysDates)+") AND deleted = 0");
			
			ps = dataStore.createPreparedStatement(sql.toString());
			rs = ps.executeQuery();
			
			
			/*
			 * Different cases that needs to be handled
			 *
			 * ---------------------------------------------------
			 * | Record present |	Data Present  |	Already Sown |
			 * ---------------------------------------------------
			 * | 	Yes		  	|		Yes		  |		Yes		 |
			 * | 	Yes		    | 		No		  |		Yes	     |
			 * | 	No			|		No		  |	    Yes		 |	
			 * | 	No			|		No		  |	    No		 |	
			 * ---------------------------------------------------
			 */
			
			while (rs.next()) {
				long farmDataID = rs.getLong("crop_sown_data_id");
				int forecastDate = rs.getInt("model_date");
				
				ICropStressForecastData csfData = allVillageCSD.get(farmDataID);
				if (csfData == null) {
					csfData = new CropStressForecastData();
					csfData.setCumulativeAETc(DSPConstants.NO_DATA);
					csfData.setCumulativePETc(DSPConstants.NO_DATA);
					allVillageCSD.put(farmDataID, csfData);
				}
				
				if (forecastDate <= csfData.getModelDate() && csfData.getCumulativeAETc()>=0 
						&& csfData.getCumulativePETc() >=0) {
					continue;
				}
				
				csfData.setModelDate(forecastDate);
				if (rs.getDouble("cumm_actual_etc") >=0) {
					csfData.setCumulativeAETc(rs.getDouble("cumm_actual_etc"));
				}
				if (rs.getDouble("cumm_potential_etc") >=0) {
					csfData.setCumulativePETc(rs.getDouble("cumm_potential_etc"));					
				}
			}

			for (Long farmDataID : farmIDToSowingDateMap.keySet()) {
				if (!allVillageCSD.containsKey(farmDataID)) {
					int sowingDate = farmIDToSowingDateMap.get(farmDataID);
					
					ICropStressForecastData csfData = new CropStressForecastData();
					csfData.setCumulativeAETc(DSPConstants.NO_DATA);
					csfData.setCumulativePETc(DSPConstants.NO_DATA);
					allVillageCSD.put(farmDataID, csfData);
					
					if (modelDate < sowingDate) {
						csfData.setCumulativeAETc(0.0);
						csfData.setCumulativePETc(0.0);
					}
				}
			}
			
		} catch (SQLException se) {
			// TODO: Log and throw the right exception
			System.out.println("Error retrieving all ICropStressForecastData for model date : " + modelDate
					+ " -- farmIDs : " + farmIDToSowingDateMap.keySet() + se.getMessage());
			throw new FetchingObjectException("Error retrieving all ICropStressForecastData for model date : "
					+ modelDate + " -- farmIDs : " + farmIDToSowingDateMap.keySet(), se);
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				rs = null;
			}
			if (ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					// Do Nothing
				}
				ps = null;
			}
			DataStoreContext.clearDataStoreContext(dataStoreOwnerKey);
		}
		
		return allVillageCSD;
	}
}