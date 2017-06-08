package com.vassarlabs.iwm.cleanup.dsp.impl;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.vassarlabs.common.dsp.context.DataStoreContext;
import com.vassarlabs.common.dsp.err.DSPException;
import com.vassarlabs.common.dsp.rdbms.api.IRDBMSDataStore;
import com.vassarlabs.common.utils.DateUtils;
import com.vassarlabs.iwm.cleanup.dsp.api.ICleanUpDSP;
import com.vassarlabs.iwm.utils.IWMConstants;

@Component
public class CleanUpDSPImpl implements ICleanUpDSP {

	@Autowired
	@Qualifier("business_data")
	IRDBMSDataStore businessDataStore;

	/**
	 * This method is intended for moving unused records (which are one week
	 * older and are not the last known data for a location) from rainfall
	 * staging to its backup table in order to reduce the insertion latency in
	 * Staging.
	 * 
	 * @throws DSPException
	 */
	@Override
	public void cleanUpRainfallStagingData() throws DSPException {

		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		String transactionId = null;
		ResultSet rs = null;
		boolean result = false;

		try {
			long lastWeekAtTheSameTime = System.currentTimeMillis() - (7 * IWMConstants.MILLIS_IN_24_HOURS);

			System.out.println(
					"CleanUpDSPImpl :: cleanUpRainfallStagingData: Moving records.. lastWeekTimeAtTheSameTime : "
							+ lastWeekAtTheSameTime);
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(businessDataStore);
			transactionId = businessDataStore.beginTransaction();

			/*
			 * Insert the records from staging_scrapped_data_rainfall to its
			 * backup table which are one week older and are not last known data
			 * for a location
			 */
			String sql = "insert into staging_scrapped_data_rainfall_backup select * from staging_scrapped_data_rainfall where "
					+ " last_updated_ts < ? " + " and staging_scrapped_data_rf_id not in "
					+ "(select distinct rf.staging_scrapped_data_rf_id from (select external_id, max(last_updated_ts) as last_updated_ts from "
					+ " staging_scrapped_data_rainfall group by external_id) as max_table join staging_scrapped_data_rainfall rf "
					+ " on max_table.last_updated_ts = rf.last_updated_ts and max_table.external_id= rf.external_id);";

			System.out.println("CleanUpDSPImpl :: cleanUpRainfallStagingData: insertSQL : " + sql);
			ps = businessDataStore.createPreparedStatement(sql);
			ps.setLong(1, lastWeekAtTheSameTime);
			ps.executeUpdate();
			ps.close();

			/*
			 * Delete the records from staging_scrapped_data_rainfall which are
			 * moved to backup table i.e from staging_scrapped_data_rainfall
			 * which are one week older and are not last known data for a
			 * location
			 */
			sql = "delete from staging_scrapped_data_rainfall where " + " last_updated_ts <  ? "
					+ " and staging_scrapped_data_rf_id not in "
					+ " (select last_known_data.staging_scrapped_data_rf_id from " // Redundant
																					// select
																					// clause
																					// so
																					// that
																					// MySQL
																					// allows
																					// the
																					// update
																					// to
																					// the
																					// same
																					// table
																					// in
																					// sub
																					// query
					+ "(select distinct rf.staging_scrapped_data_rf_id from (select external_id, max(last_updated_ts) as last_updated_ts from "
					+ " staging_scrapped_data_rainfall group by external_id) as max_table join staging_scrapped_data_rainfall rf "
					+ " on max_table.last_updated_ts = rf.last_updated_ts and max_table.external_id= rf.external_id)  as last_known_data);";

			System.out.println("CleanUpDSPImpl :: cleanUpRainfallStagingData: deleteSQL : " + sql);
			ps = businessDataStore.createPreparedStatement(sql);
			ps.setLong(1, lastWeekAtTheSameTime);
			ps.executeUpdate();

			businessDataStore.commitTransaction(transactionId);
			System.out.println(
					"CleanUpDSPImpl :: cleanUpRainfallStagingData :: moved records from staging_scrapped_data_rainfall to backup");
			result = true;
		} catch (SQLException se) {
			se.printStackTrace();
			throw new DSPException("Error while inserting into db", se);
		} finally {
			if (!result) {
				businessDataStore.rollbackTransaction(transactionId);
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
	public void cleanUpWaterQuantityData() throws DSPException {
		// TODO Method yet to be implemented
		/*
		 * String dataStoreOwnerKey = null; PreparedStatement ps = null;
		 * ResultSet rs = null;
		 * 
		 * try { dataStoreOwnerKey =
		 * DataStoreContext.initReusableDataStoreContext(businessDataStore);
		 * String sql = ""; ps = businessDataStore.createPreparedStatement(sql);
		 * 
		 * rs = ps.executeQuery();
		 * 
		 * System.out.
		 * println("CleanUpDSPImpl :: cleanUpWaterQuantityData :: moved staging_scrapped_data_reservoir to backup"
		 * ); } catch (SQLException se) { se.printStackTrace(); } finally { if
		 * (rs != null) { try { rs.close(); } catch (SQLException e) { // Do
		 * Nothing } rs = null; } if (ps != null) { try { ps.close(); } catch
		 * (SQLException e) { // Do Nothing } ps = null; }
		 * DataStoreContext.clearDataStoreContext(dataStoreOwnerKey); }
		 */
	}

	/**
	 * This method is intended for moving unused records (which are one week
	 * older and are not the last known data for a location) from ground water
	 * staging to its backup table in order to reduce the insertion latency in
	 * Staging.
	 * 
	 * @throws DSPException
	 */
	@Override
	public void cleanUpGWStagingData() throws DSPException {
		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		String transactionId = null;
		ResultSet rs = null;
		boolean result = false;

		try {
			long lastWeekAtTheSameTime = System.currentTimeMillis() - (7 * IWMConstants.MILLIS_IN_24_HOURS);

			System.out.println("CleanUpDSPImpl :: cleanUpGWStagingData: Moving records.. lastWeekTimeAtTheSameTime : "
					+ lastWeekAtTheSameTime);
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(businessDataStore);
			transactionId = businessDataStore.beginTransaction();

			/*
			 * Insert the records from staging_scrapped_data_groundwater to its
			 * backup table which are one week older and are not last known data
			 * for a location
			 */
			String sql = "insert into staging_scrapped_data_groundwater_backup select * from staging_scrapped_data_groundwater where "
					+ " last_updated_ts < ? " + " and staging_scrapped_data_gw_id not in "
					+ " (select distinct gw.staging_scrapped_data_gw_id from "
					+ " (select external_id, max(last_updated_ts) as last_updated_ts from staging_scrapped_data_groundwater group by external_id) as max_table "
					+ " join staging_scrapped_data_groundwater gw on max_table.last_updated_ts = gw.last_updated_ts and max_table.external_id= gw.external_id);";

			System.out.println("CleanUpDSPImpl :: cleanUpGWStagingData: insertSQL : " + sql);
			ps = businessDataStore.createPreparedStatement(sql);
			ps.setLong(1, lastWeekAtTheSameTime);
			ps.executeUpdate();
			ps.close();

			/*
			 * Delete the records from staging_scrapped_data_groundwater which
			 * are moved to backup table i.e from
			 * staging_scrapped_data_groundwater which are one week older and
			 * are not last known data for a location
			 */
			sql = "delete from staging_scrapped_data_groundwater where " + " last_updated_ts <  ? "
					+ " and staging_scrapped_data_gw_id not in "
					+ " (select last_known_data.staging_scrapped_data_gw_id from " // Redundant
																					// select
																					// clause
																					// so
																					// that
																					// MySQL
																					// allows
																					// the
																					// update
																					// to
																					// the
																					// same
																					// table
																					// in
																					// sub
																					// query
					+ " (select distinct gw.staging_scrapped_data_gw_id from "
					+ " (select external_id, max(last_updated_ts) as last_updated_ts from staging_scrapped_data_groundwater group by external_id) as max_table "
					+ " join staging_scrapped_data_groundwater gw on max_table.last_updated_ts = gw.last_updated_ts and max_table.external_id= gw.external_id) as last_known_data);";

			System.out.println("CleanUpDSPImpl :: cleanUpGWStagingData: deleteSQL : " + sql);
			ps = businessDataStore.createPreparedStatement(sql);
			ps.setLong(1, lastWeekAtTheSameTime);
			ps.executeUpdate();

			businessDataStore.commitTransaction(transactionId);
			System.out.println(
					"CleanUpDSPImpl :: cleanUpGWStagingData :: moved records from staging_scrapped_data_rainfall to backup");
			result = true;
		} catch (SQLException se) {
			se.printStackTrace();
			throw new DSPException("Error while inserting into db", se);
		} finally {
			if (!result) {
				businessDataStore.rollbackTransaction(transactionId);
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
	public void cleanUpTableData(String mainTable, String backupTable, int modelDate, int windowSize)
			throws DSPException {
		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		String transactionId = null;
		ResultSet rs = null;
		boolean result = false;

		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(businessDataStore);
			transactionId = businessDataStore.beginTransaction();
			int referenceDate = DateUtils.getSeasonStartDateFromModelDate(modelDate);
			referenceDate = 20170131;// hard coded date
			String sql = "INSERT INTO " + backupTable + "(crop_stress_forecast_data_id, " + "crop_sown_data_id, "
					+ "village_water_available_data_id, " + "model_date, " + "crop_stage_id, " + "cumm_water_supplied, "
					+ "optimal_water_req, " + "minimal_water_req, " + "owr_stress_factor_1, " + "owr_stress_factor_2, "
					+ "owr_stress_factor_3, " + "owr_stress_factor_4, " + "mwr_stress_factor_1, "
					+ "mwr_stress_factor_2, " + "mwr_stress_factor_3, " + "mwr_stress_factor_4, "
					+ "owr_is_crop_under_stress, " + "mwr_is_crop_under_stress, " + "insert_ts, " + "update_ts, "
					+ "deleted, " + "user_session_id, " + "pest_names, " + "owr_next_7_days, " + "mwr_next_7_days, "
					+ "water_available_next_7_days, " + "last_n_days_computed_asm, " + "curr_actual_etc, "
					+ "curr_potential_etc, " + "cumm_actual_etc, " + "cumm_potential_etc, " + "estimated_yield, "
					+ "soil_water_critical_level, " + "crop_sown_data_source)"

					+ " SELECT " + "crop_stress_forecast_data_id, " + "crop_sown_data_id, "
					+ "village_water_available_data_id, " + "model_date, " + "crop_stage_id, " + "cumm_water_supplied, "
					+ "optimal_water_req, " + "minimal_water_req, " + "owr_stress_factor_1, " + "owr_stress_factor_2, "
					+ "owr_stress_factor_3, " + "owr_stress_factor_4, " + "mwr_stress_factor_1, "
					+ "mwr_stress_factor_2, " + "mwr_stress_factor_3, " + "mwr_stress_factor_4, "
					+ "owr_is_crop_under_stress, " + "mwr_is_crop_under_stress, " + "csfd.insert_ts, "
					+ "csfd.update_ts, " + "csfd.deleted, " + "csfd.user_session_id, " + "pest_names, "
					+ "owr_next_7_days, " + "mwr_next_7_days, " + "water_available_next_7_days, "
					+ "last_n_days_computed_asm, " + "curr_actual_etc, " + "curr_potential_etc, " + "cumm_actual_etc, "
					+ "cumm_potential_etc, " + "estimated_yield, " + "soil_water_critical_level, "
					+ "crop_sown_data_source)"

					+ "FROM " + mainTable + " csfd JOIN farm_data fd ON csfd.crop_sown_data_id = fd.farm_data_id where "
					+ " (csfd.model_date <= " + referenceDate + ") OR (csfd.model_date < " + (modelDate - windowSize)
					+ " and expected_harvest_date >= " + (modelDate - windowSize)
					+ ") OR (csfd.model_date < fd.expected_harvest_date AND fd.expected_harvest_date < "
					+ (modelDate - windowSize) + "));";

			/**
			 * Insert the records from mainTable to its backupTable for all
			 * records satisfying condition
			 **/

			ps = businessDataStore.createPreparedStatement(sql);

			int resultInsert = ps.executeUpdate();
			if (resultInsert > 0) {
				System.out.println("CleanUpDSPImpl :: cleanUpTable :: moved " + resultInsert + " records from "
						+ mainTable + " to " + backupTable);
			}
			ps.close();

			/**
			 * Delete the records from mainTable which are moved to backupTable
			 **/
			sql = "DELETE FROM " + mainTable + " where " + "crop_stress_forecast_data_id IN"
					+ "(SELECT crop_stress_forecast_data_id "
					+ "FROM (SELECT * FROM crop_stress_forecast_data) AS csfd JOIN farm_data fd ON "
					+ "csfd.crop_sown_data_id = fd.farm_data_id" + " where " + "(csfd.model_date <= " + referenceDate
					+ ") OR " + "(csfd.model_date < " + (modelDate - windowSize) + " and expected_harvest_date >= "
					+ (modelDate - windowSize) + ") OR"
					+ "(csfd.model_date < fd.expected_harvest_date AND fd.expected_harvest_date < "
					+ (modelDate - windowSize) + "));";

			ps = businessDataStore.createPreparedStatement(sql);
			int resultUpdate = ps.executeUpdate();
			businessDataStore.commitTransaction(transactionId);
			System.out
					.println("CleanUpDSPImpl :: cleanUpTable :: Deleted " + resultUpdate + "records from " + mainTable);
			result = true;
		} catch (SQLException se) {
			se.printStackTrace();
			throw new DSPException("Error while inserting into db", se);
		} finally {
			if (!result) {
				businessDataStore.rollbackTransaction(transactionId);
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
}