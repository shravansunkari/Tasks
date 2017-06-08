package com.vassarlabs.iwm.rainfall.forecast.dsp.impl;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.vassarlabs.common.dsp.context.DataStoreContext;
import com.vassarlabs.common.dsp.err.DSPException;
import com.vassarlabs.common.dsp.err.FetchingObjectException;
import com.vassarlabs.common.dsp.err.MoreThanOneObjectFoundException;
import com.vassarlabs.common.dsp.err.ObjectCreationException;
import com.vassarlabs.common.dsp.rdbms.api.IRDBMSDataStore;
import com.vassarlabs.common.utils.DateUtils;
import com.vassarlabs.common.utils.StringUtils;
import com.vassarlabs.common.utils.err.ObjectNotFoundException;
import com.vassarlabs.iwm.rainfall.forecast.dsp.api.IRainfallForecastDSP;
import com.vassarlabs.iwm.rainfall.forecast.pojo.api.IRainfallForecast;
import com.vassarlabs.iwm.rainfall.forecast.pojo.api.IRainfallForecastBackup;
import com.vassarlabs.iwm.rainfall.forecast.pojo.impl.RainfallForecast;
import com.vassarlabs.iwm.rainfall.forecast.pojo.impl.RainfallForecastBackup;

@Component
public class RainfallForecastDSP
	implements IRainfallForecastDSP {

	@Autowired
	@Qualifier("business_data")
	protected IRDBMSDataStore dataStore;

	@Override
	public List<IRainfallForecast> getAllRainfallForecast(long forecastDay)
		throws DSPException {
		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
			
			StringBuffer sql = new StringBuffer();
			sql.append("select *");
			sql.append(" from rainfall_forecast");
			sql.append(" where forecast_day = ? and deleted = 0");
				
			ps = dataStore.createPreparedStatement(sql.toString());
			ps.setLong(1, forecastDay);

			rs = ps.executeQuery();
				
			List<IRainfallForecast> rainfallForecastList = new ArrayList<IRainfallForecast>();
			while (rs.next()) {
				IRainfallForecast rainfallForecast = new RainfallForecast();
				rainfallForecast.setRainfallForecastId(rs.getLong("rainfall_forecast_id"));
				rainfallForecast.setLocationUUID(rs.getString("location_uuid"));
				rainfallForecast.setEventGenTS(rs.getLong("event_gen_ts"));
				rainfallForecast.setForecastDay(rs.getLong("forecast_day"));
				rainfallForecast.setDay1(rs.getDouble("day_1"));
				rainfallForecast.setDay2(rs.getDouble("day_2"));
				rainfallForecast.setDay3(rs.getDouble("day_3"));
				rainfallForecast.setDay4(rs.getDouble("day_4"));
				rainfallForecast.setDay5(rs.getDouble("day_5"));
				rainfallForecast.setDay6(rs.getDouble("day_6"));
				rainfallForecast.setDay7(rs.getDouble("day_7"));
				rainfallForecast.setInsertTs(rs.getLong("insert_ts"));
				rainfallForecast.setUpdateTs(rs.getLong("update_ts"));
				rainfallForecast.setDeleted(rs.getInt("deleted"));
				
				rainfallForecastList.add(rainfallForecast);
			}
			
			if (rainfallForecastList.isEmpty()) {
				List<IRainfallForecastBackup> rainfallForecastBackupList = getAllRainfallForecastBackup(forecastDay);
				if (rainfallForecastBackupList.isEmpty()) {
					return rainfallForecastList;
				}
				for (IRainfallForecastBackup rainfallForecastBackup : rainfallForecastBackupList) {
					IRainfallForecast rainfallForecast = new RainfallForecast();
					rainfallForecast.copyFrom(rainfallForecastBackup);
					rainfallForecastList.add(rainfallForecast);
				}
			}
			return rainfallForecastList;
		} catch (SQLException se) {
			// TODO: Log and throw the right exception
			System.out.println("Error retrieving Rainforecast record  for forecast day : " + forecastDay + " -- " + se);
			throw new FetchingObjectException(
					"Error retrieving Rainforecast record  for forecast day : " + forecastDay, se);
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
	public IRainfallForecast getRainfallForecast(String locationUUID,
			long forecastDay) throws DSPException {

		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
			
			StringBuffer sql = new StringBuffer();
			sql.append("select *");
			sql.append(" from rainfall_forecast");
			sql.append(" where location_uuid = ? and forecast_day = ? and deleted = 0");
				
			ps = dataStore.createPreparedStatement(sql.toString());
			ps.setString(1, locationUUID);
			ps.setLong(2, forecastDay);

			rs = ps.executeQuery();
				
			IRainfallForecast rainfallForecast = null;
			while (rs.next()) {
				if (rainfallForecast != null) {
					// Eliminating duplicates
					System.out.println("Bug in code, there are duplicate rainfall forecast entries for Location : " + locationUUID + " and forecast day : " + forecastDay);
					throw new MoreThanOneObjectFoundException("Bug in code, there are duplicate rainfall forecast entries for Location : " + locationUUID + " and forecast day : " + forecastDay);
				}
				rainfallForecast = new RainfallForecast();
				rainfallForecast.setRainfallForecastId(rs.getLong("rainfall_forecast_id"));
				rainfallForecast.setLocationUUID(rs.getString("location_uuid"));
				rainfallForecast.setEventGenTS(rs.getLong("event_gen_ts"));
				rainfallForecast.setForecastDay(rs.getLong("forecast_day"));
				rainfallForecast.setDay1(rs.getDouble("day_1"));
				rainfallForecast.setDay2(rs.getDouble("day_2"));
				rainfallForecast.setDay3(rs.getDouble("day_3"));
				rainfallForecast.setDay4(rs.getDouble("day_4"));
				rainfallForecast.setDay5(rs.getDouble("day_5"));
				rainfallForecast.setDay6(rs.getDouble("day_6"));
				rainfallForecast.setDay7(rs.getDouble("day_7"));
				rainfallForecast.setInsertTs(rs.getLong("insert_ts"));
				rainfallForecast.setUpdateTs(rs.getLong("update_ts"));
				rainfallForecast.setDeleted(rs.getInt("deleted"));
			}
			
			if (rainfallForecast == null) {
				IRainfallForecastBackup rainfallForecastBackup = getRainfallForecastBackup(locationUUID, forecastDay);
				if (rainfallForecastBackup == null) {
					return null;
				}
				rainfallForecast = new RainfallForecast();
				rainfallForecast.copyFrom(rainfallForecastBackup);
			}
			return rainfallForecast;
		} catch (SQLException se) {
			// TODO: Log and throw the right exception
			System.out.println("Error retrieving RainforecastBackup record  for location =" + locationUUID + " - for forecast day : " + forecastDay + " -- " + se);
			throw new FetchingObjectException(
					"Error retrieving RainforecastBackup record  for location =" + locationUUID + " - for forecast day : " + forecastDay, se);
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
	public synchronized IRainfallForecast insertOrUpdateRainfallForecast(
			IRainfallForecast rainfallForecast) throws DSPException, ObjectNotFoundException {

		// Logic
		// 1. Load record for input location UUID and forecastDay
		// 2. If no record found then insert this record into rainfall_forecast table and return
		// 3. If record exists
		//     i. insert or update in rainfall_forecast_backup table
		//     ii. update record in rainfall_forecast table
		
		String dataStoreOwnerKey = null;
		String transactionId = null;
		boolean result = false;

		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
			//temporary fix
			DataStoreContext.setUserSessionID(0);
			transactionId = dataStore.beginTransaction();

			// Check if record exists
			IRainfallForecast existingRainfallForecast = getRainfallForecast(rainfallForecast.getLocationUUID(), rainfallForecast.getForecastDay());
			if (existingRainfallForecast == null) {
				// Insert new record
				rainfallForecast = insertRainfallForecast(rainfallForecast);
			} else {
				// Updating existing record
				rainfallForecast = updateRainfallForecast(rainfallForecast, existingRainfallForecast);
			}
			dataStore.commitTransaction(transactionId);
			result = true;
			return rainfallForecast;
		} catch (DSPException de) {
			de.printStackTrace();
			System.out.println("Error while inserting/updating rainfall forecast data record into db : " + rainfallForecast + " --- " + de);
			throw new DSPException("Error while inserting/updating rainfall forecast data record into db : " + rainfallForecast, de);
		} finally {
			if (!result) {
				dataStore.rollbackTransaction(transactionId);
			}
			DataStoreContext.clearDataStoreContext(dataStoreOwnerKey);
		}
	}

	protected IRainfallForecast insertRainfallForecast(IRainfallForecast rainfallForecast)
		throws DSPException {

		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			StringBuffer sql = new StringBuffer();
			sql.append("insert into rainfall_forecast (");
			sql.append(" location_uuid, event_gen_ts, forecast_day, day_1");
			sql.append(", day_2, day_3, day_4, day_5");
			sql.append(", day_6, day_7, insert_ts, update_ts");
			sql.append(", deleted, user_session_id )");
			sql.append(" values (");
			sql.append(" ?, ?, ?, ?");
			sql.append(", ?, ?, ?, ?");
			sql.append(", ?, ?, ?, null");
			sql.append(", 0, ?);");

			ps = dataStore.createPreparedStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);
		
			ps.setString(1, rainfallForecast.getLocationUUID());
			ps.setLong(2, rainfallForecast.getEventGenTS());
			ps.setLong(3, rainfallForecast.getForecastDay());
			ps.setDouble(4, rainfallForecast.getDay1());
			ps.setDouble(5, rainfallForecast.getDay2());
			ps.setDouble(6, rainfallForecast.getDay3());
			ps.setDouble(7, rainfallForecast.getDay4());
			ps.setDouble(8, rainfallForecast.getDay5());
			ps.setDouble(9, rainfallForecast.getDay6());
			ps.setDouble(10, rainfallForecast.getDay7());
			ps.setLong(11, DataStoreContext.getCurrentTS());
			ps.setLong(12, DataStoreContext.getUserSessionID());

			ps.executeUpdate();

			long rainfallForecastId = -1;
			rs = ps.getGeneratedKeys();
			if (rs.next()) {
				rainfallForecastId = rs.getLong(1);
			}
			if(rainfallForecastId <= 0) {
				throw new ObjectCreationException(
					"Error creating Rainfall Forecast Data record ="
						+ rainfallForecastId
						+ " - while getting auto generated keys for record : " + rainfallForecast);
			}
			rainfallForecast.setRainfallForecastId(rainfallForecastId);
			rainfallForecast.setInsertTs(DataStoreContext.getCurrentTS());
			rainfallForecast.setUserSessionId(DataStoreContext.getUserSessionID());
			return rainfallForecast;
		} catch (SQLException se) {
			System.out.println("Error while inserting rainfall forecast data record into db : " + rainfallForecast + " --- " + se);
			throw new DSPException("Error while inserting rainfall forecast data record into db : " + rainfallForecast, se);
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
		}
	}
	
	protected IRainfallForecast updateRainfallForecast(IRainfallForecast rainfallForecast, IRainfallForecast existingRainfallForeCast)
		throws DSPException, ObjectNotFoundException {

		// Logic
		// 1. Load record from backup table
		// 2. If record does not exist insert existing record into backup table
		// 3. If record exists update backup table record with existing record data
		// 4. Update rainfall_forecast table with new data
		
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			
			IRainfallForecastBackup rainfallForecastBackup = getRainfallForecastBackup(existingRainfallForeCast.getLocationUUID(), existingRainfallForeCast.getForecastDay());
			if (rainfallForecastBackup == null) {
				// Insert into rainfall_forecast_backup table
				insertRainfallForecastBackup(existingRainfallForeCast);
			} else {
				// Update into rainfall_forecast_backup table
				updateRainfallForecastBackup(existingRainfallForeCast, rainfallForecastBackup);
			}

			// Updating the record in rainfall_forecast table
			StringBuffer sql = new StringBuffer();
			sql.append("update rainfall_forecast ");
			sql.append(" set day_1 = ?, day_2 = ?, day_3 = ?, day_4 = ?");
			sql.append(", day_5 = ?, day_6 = ?, day_7 = ?, update_ts = ?");
			sql.append(", user_session_id = ?");
			sql.append(" where rainfall_forecast_id = ? and deleted = 0");
//			sql.append(" values ");
//			sql.append(", ?, ?, ?, ?");
//			sql.append(", ?, ?, ?, ?");
//			sql.append(", ?, ?");
				
			ps = dataStore.createPreparedStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);
			
			ps.setDouble(1, rainfallForecast.getDay1());
			ps.setDouble(2, rainfallForecast.getDay2());
			ps.setDouble(3, rainfallForecast.getDay3());
			ps.setDouble(4, rainfallForecast.getDay4());
			ps.setDouble(5, rainfallForecast.getDay5());
			ps.setDouble(6, rainfallForecast.getDay6());
			ps.setDouble(7, rainfallForecast.getDay7());
			ps.setLong(8, DataStoreContext.getCurrentTS());
			ps.setLong(9, DataStoreContext.getUserSessionID());
			ps.setLong(10, existingRainfallForeCast.getRainfallForecastId());
			int rowCount = ps.executeUpdate();
			if (rowCount < 1) {
				System.out.println("Error updating rainfall forecast record - no record found for : " + rainfallForecast);
				throw new ObjectNotFoundException("Error updating rainfall forecast record - no record found for : " + rainfallForecast);
			}
			if (rowCount > 1) {
				System.out.println("More than one rainfall forecast record found for updating : " + rainfallForecast);
				throw new MoreThanOneObjectFoundException("More than one raBackupinfall forecast record found for updating : " + rainfallForecast);
			}
			rainfallForecast.setUpdateTs(DataStoreContext.getCurrentTS());
			rainfallForecast.setUserSessionId(DataStoreContext.getUserSessionID());
			return rainfallForecast;
		} catch (SQLException se) {
			System.out.println("Error while inserting rainfall forecast data record into db : " + rainfallForecast + " --- " + se);
			throw new DSPException("Error while inserting rainfall forecast data record into db : " + rainfallForecast, se);
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
		}
	}
	
	/**
	 * Returns null if record does not exists
	 * 
	 * @param locationUUID
	 * @param forecastDay
	 * @return
	 * @throws DSPException 
	 */
	protected IRainfallForecastBackup getRainfallForecastBackup(String locationUUID, long forecastDay)
		throws DSPException {
		
		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
			
			StringBuffer sql = new StringBuffer();
			sql.append("select *");
			sql.append(" from rainfall_forecast_backup");
			sql.append(" where location_uuid = ? and forecast_day = ? and deleted = 0");
			
			ps = dataStore.createPreparedStatement(sql.toString());
			ps.setString(1, locationUUID);
			ps.setLong(2, forecastDay);

			rs = ps.executeQuery();
			
			IRainfallForecastBackup rainfallForecastBackup = null;
			while (rs.next()) {
				if (rainfallForecastBackup != null) {
					// Eliminating duplicates
					System.out.println("Bug in code, there are rainfall forecast backup duplicate entries for Location : " + locationUUID + " and forecast day : " + forecastDay);
					throw new MoreThanOneObjectFoundException("Bug in code, there are rainfall forecast backup duplicate entries for Location : " + locationUUID + " and forecast day : " + forecastDay);
				}
				rainfallForecastBackup = new RainfallForecastBackup();
				rainfallForecastBackup.setRainfallForecastBackupId(rs.getLong("rainfall_forecast_backup_id"));
				rainfallForecastBackup.setRainfallForecastId(rs.getLong("rainfall_forecast_id"));
				rainfallForecastBackup.setLocationUUID(rs.getString("location_uuid"));
				rainfallForecastBackup.setEventGenTS(rs.getLong("event_gen_ts"));
				rainfallForecastBackup.setForecastDay(rs.getLong("forecast_day"));
				rainfallForecastBackup.setDay1(rs.getDouble("day_1"));
				rainfallForecastBackup.setDay2(rs.getDouble("day_2"));
				rainfallForecastBackup.setDay3(rs.getDouble("day_3"));
				rainfallForecastBackup.setDay4(rs.getDouble("day_4"));
				rainfallForecastBackup.setDay5(rs.getDouble("day_5"));
				rainfallForecastBackup.setDay6(rs.getDouble("day_6"));
				rainfallForecastBackup.setDay7(rs.getDouble("day_7"));
				rainfallForecastBackup.setInsertTs(rs.getLong("insert_ts"));
				rainfallForecastBackup.setUpdateTs(rs.getLong("update_ts"));
				rainfallForecastBackup.setDeleted(rs.getInt("deleted"));
			}
			return rainfallForecastBackup;
		} catch (SQLException se) {
			// TODO: Log and throw the right exception
			System.out.println("Error retrieving RainforecastBackup record  for location =" + locationUUID + " - for forecast day : " + forecastDay + " -- " + se);
			throw new FetchingObjectException(
					"Error retrieving RainforecastBackup record  for location =" + locationUUID + " - for forecast day : " + forecastDay, se);
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
	 * Returns list of IRainfallForecastBackup
	 * 
	 * @param forecastDay
	 * @return
	 * @throws DSPException 
	 */
	protected List<IRainfallForecastBackup> getAllRainfallForecastBackup(long forecastDay)
		throws DSPException {
		
		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
			
			StringBuffer sql = new StringBuffer();
			sql.append("select *");
			sql.append(" from rainfall_forecast_backup");
			sql.append(" where forecast_day = ? and deleted = 0");
			
			ps = dataStore.createPreparedStatement(sql.toString());
			ps.setLong(1, forecastDay);

			rs = ps.executeQuery();
			
			List<IRainfallForecastBackup> rainfallForecastBackupList = new ArrayList<IRainfallForecastBackup>();
			while (rs.next()) {
				IRainfallForecastBackup rainfallForecastBackup = new RainfallForecastBackup();
				rainfallForecastBackup.setRainfallForecastBackupId(rs.getLong("rainfall_forecast_backup_id"));
				rainfallForecastBackup.setRainfallForecastId(rs.getLong("rainfall_forecast_id"));
				rainfallForecastBackup.setLocationUUID(rs.getString("location_uuid"));
				rainfallForecastBackup.setEventGenTS(rs.getLong("event_gen_ts"));
				rainfallForecastBackup.setForecastDay(rs.getLong("forecast_day"));
				rainfallForecastBackup.setDay1(rs.getDouble("day_1"));
				rainfallForecastBackup.setDay2(rs.getDouble("day_2"));
				rainfallForecastBackup.setDay3(rs.getDouble("day_3"));
				rainfallForecastBackup.setDay4(rs.getDouble("day_4"));
				rainfallForecastBackup.setDay5(rs.getDouble("day_5"));
				rainfallForecastBackup.setDay6(rs.getDouble("day_6"));
				rainfallForecastBackup.setDay7(rs.getDouble("day_7"));
				rainfallForecastBackup.setInsertTs(rs.getLong("insert_ts"));
				rainfallForecastBackup.setUpdateTs(rs.getLong("update_ts"));
				rainfallForecastBackup.setDeleted(rs.getInt("deleted"));
				
				rainfallForecastBackupList.add(rainfallForecastBackup);
			}
			return rainfallForecastBackupList;
		} catch (SQLException se) {
			// TODO: Log and throw the right exception
			System.out.println("Error retrieving RainforecastBackup record  for forecast day : " + forecastDay + " -- " + se);
			throw new FetchingObjectException(
					"Error retrieving RainforecastBackup record  for forecast day : " + forecastDay, se);
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

	protected IRainfallForecastBackup insertRainfallForecastBackup(IRainfallForecast existingRainfallForecast)
		throws DSPException {

		IRainfallForecastBackup rainfallForecastBackup = new RainfallForecastBackup();
		rainfallForecastBackup.copyFrom(existingRainfallForecast);
		rainfallForecastBackup.setInsertTs(DataStoreContext.getCurrentTS());
		
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			StringBuffer sql = new StringBuffer();
			sql.append("insert into rainfall_forecast_backup (");
			sql.append(" rainfall_forecast_id, location_uuid, event_gen_ts, forecast_day");
			sql.append(", day_1, day_2, day_3, day_4");
			sql.append(", day_5, day_6, day_7, insert_ts");
			sql.append(", update_ts, deleted, user_session_id )");
			sql.append(" values (");
			sql.append(" ?, ?, ?, ?");
			sql.append(", ?, ?, ?, ?");
			sql.append(", ?, ?, ?, ?");
			sql.append(", null, 0, ?)");
				
			ps = dataStore.createPreparedStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);
			
			ps.setLong(1, rainfallForecastBackup.getRainfallForecastId());
			ps.setString(2, rainfallForecastBackup.getLocationUUID());
			ps.setLong(3, rainfallForecastBackup.getEventGenTS());
			ps.setLong(4, rainfallForecastBackup.getForecastDay());
			ps.setDouble(5, rainfallForecastBackup.getDay1());
			ps.setDouble(6, rainfallForecastBackup.getDay2());
			ps.setDouble(7, rainfallForecastBackup.getDay3());
			ps.setDouble(8, rainfallForecastBackup.getDay4());
			ps.setDouble(9, rainfallForecastBackup.getDay5());
			ps.setDouble(10, rainfallForecastBackup.getDay6());
			ps.setDouble(11, rainfallForecastBackup.getDay7());
			ps.setLong(12, DataStoreContext.getCurrentTS());
			ps.setLong(13, DataStoreContext.getUserSessionID());

			ps.executeUpdate();
			
			long rainfallForecastBackupId = -1;
			rs = ps.getGeneratedKeys();
			if (rs.next()) {
				rainfallForecastBackupId = rs.getLong(1);
			}
			if(rainfallForecastBackupId <= 0) {
				throw new ObjectCreationException(
					"Error creating Rainfall Forecast Backup Data record ="
						+ rainfallForecastBackupId
						+ " - while getting auto generated keys for record : " + rainfallForecastBackup);
			}
			rainfallForecastBackup.setRainfallForecastBackupId(rainfallForecastBackupId);
			rainfallForecastBackup.setInsertTs(DataStoreContext.getCurrentTS());
			rainfallForecastBackup.setUserSessionId(DataStoreContext.getUserSessionID());
			return rainfallForecastBackup;
		} catch (SQLException se) {
			System.out.println("Error while inserting rainfall forecast data backup record into db : " + rainfallForecastBackup + " --- " + se);
			throw new DSPException("Error while inserting rainfall forecast data backup record into db : " + rainfallForecastBackup, se);
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
		}
	}
	
	protected void updateRainfallForecastBackup(IRainfallForecast existingRainfallForecast, IRainfallForecastBackup rainfallForecastBackup)
		throws DSPException, ObjectNotFoundException {

		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			StringBuffer sql = new StringBuffer();
			sql.append("update rainfall_forecast_backup ");
			sql.append(" set day_1 = ?, day_2 = ?, day_3 = ?, day_4 = ?");
			sql.append(", day_5 = ?, day_6 = ?, day_7 = ?, update_ts = ?");
			sql.append(", user_session_id = ? ");
			sql.append(" where rainfall_forecast_backup_id = ? and deleted = 0");
//			sql.append(" values (");
//			sql.append(", ?, ?, ?, ?");
//			sql.append(", ?, ?, ?, ?");
//			sql.append(", ?, ?)");
				
			ps = dataStore.createPreparedStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);
			
			ps.setDouble(1, existingRainfallForecast.getDay1());
			ps.setDouble(2, existingRainfallForecast.getDay2());
			ps.setDouble(3, existingRainfallForecast.getDay3());
			ps.setDouble(4, existingRainfallForecast.getDay4());
			ps.setDouble(5, existingRainfallForecast.getDay5());
			ps.setDouble(6, existingRainfallForecast.getDay6());
			ps.setDouble(7, existingRainfallForecast.getDay7());
			ps.setLong(8, DataStoreContext.getCurrentTS());
			ps.setLong(9, DataStoreContext.getUserSessionID());
			ps.setLong(10, rainfallForecastBackup.getRainfallForecastBackupId());

			int rowCount = ps.executeUpdate();
			if (rowCount < 1) {
				System.out.println("Error updating rainfall forecast backup record - no record found for : " + rainfallForecastBackup);
				throw new ObjectNotFoundException("Error updating rainfall forecast backup record - no record found for : " + rainfallForecastBackup);
			}
			if (rowCount > 1) {
				System.out.println("More than one rainfall forecast backup record found for updating : " + rainfallForecastBackup);
				throw new MoreThanOneObjectFoundException("More than one rainfall forecast backup record found for updating : " + rainfallForecastBackup);
			}
			rainfallForecastBackup.copyFrom(existingRainfallForecast);
			rainfallForecastBackup.setUpdateTs(DataStoreContext.getCurrentTS());
			rainfallForecastBackup.setUserSessionId(DataStoreContext.getUserSessionID());
		} catch (SQLException se) {
			System.out.println("Error while updating rainfall forecast data backup record into db : " + existingRainfallForecast + " --- " + se);
			throw new DSPException("Error while updating rainfall forecast data backup record into db : " + existingRainfallForecast, se);
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
		}
	}

	@Override
	public Map<String, IRainfallForecast> getLocationLevelForecast(
			long forecastDay) throws DSPException {
		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
			
			StringBuffer sql = new StringBuffer();
			sql.append("select *");
			sql.append(" from rainfall_forecast");
			sql.append(" where forecast_day = ? and deleted = 0");
				
			ps = dataStore.createPreparedStatement(sql.toString());
			ps.setLong(1, forecastDay);

			rs = ps.executeQuery();
				
			Map<String, IRainfallForecast> locForecastMap = new HashMap<String, IRainfallForecast>();
			
			while (rs.next()) {
				String locUUID = rs.getString("location_uuid");
				
				IRainfallForecast rainfallForecast = new RainfallForecast();
				rainfallForecast.setRainfallForecastId(rs.getLong("rainfall_forecast_id"));
				rainfallForecast.setLocationUUID(rs.getString("location_uuid"));
				rainfallForecast.setEventGenTS(rs.getLong("event_gen_ts"));
				rainfallForecast.setForecastDay(rs.getLong("forecast_day"));
				rainfallForecast.setDay1(rs.getDouble("day_1"));
				rainfallForecast.setDay2(rs.getDouble("day_2"));
				rainfallForecast.setDay3(rs.getDouble("day_3"));
				rainfallForecast.setDay4(rs.getDouble("day_4"));
				rainfallForecast.setDay5(rs.getDouble("day_5"));
				rainfallForecast.setDay6(rs.getDouble("day_6"));
				rainfallForecast.setDay7(rs.getDouble("day_7"));
				rainfallForecast.setInsertTs(rs.getLong("insert_ts"));
				rainfallForecast.setUpdateTs(rs.getLong("update_ts"));
				rainfallForecast.setDeleted(rs.getInt("deleted"));
				
				locForecastMap.put(locUUID, rainfallForecast);
			}
			
			
			return locForecastMap;
		} catch (SQLException se) {
			// TODO: Log and throw the right exception
			System.out.println("Error retrieving Rainforecast record  for forecast day : " + forecastDay + " -- " + se);
			throw new FetchingObjectException(
					"Error retrieving Rainforecast record  for forecast day : " + forecastDay, se);
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
	public long getLastKnownForecastDate() throws DSPException {
		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
			
			StringBuffer sql = new StringBuffer();
			sql.append("select max(forecast_day) as forecast_day ");
			sql.append("from rainfall_forecast ");
			sql.append("where deleted = 0 ");
				
			ps = dataStore.createPreparedStatement(sql.toString());
			rs = ps.executeQuery();
				
			int forecastDate = 0;
			while (rs.next()) {
				forecastDate = rs.getInt("forecast_day");
			}
			return forecastDate;
			
		} catch (SQLException se) {
			System.out.println("Error retrieving last known forecast date "+ se);
			throw new FetchingObjectException(
					"Error retrieving last known forecast date ", se);
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
	public List<IRainfallForecast> getAllLastKnownRainfallForecast(
			long forecastDay) throws DSPException {
		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
			
			long prevFcastDayInMS = DateUtils.getModelDateInMillis(new Long(forecastDay).intValue()) - DateUtils.TWENTY_FOUR_HOURS_IN_SECONDS - 1;
			
			long prevFcastDay = DateUtils.getYYYYMMdd(prevFcastDayInMS);		
					
			StringBuffer sql = new StringBuffer();
			sql.append("select *");
			sql.append(" from rainfall_forecast");
			sql.append(" where forecast_day in (");
			sql.append(String.valueOf(forecastDay));
			sql.append(",");
			sql.append(String.valueOf(prevFcastDay));
			sql.append(" ) and deleted = 0");
			
			ps = dataStore.createPreparedStatement(sql.toString());

			rs = ps.executeQuery();
				
			Map<String, IRainfallForecast> locForecasDayMap = new HashMap<String, IRainfallForecast>();
			while (rs.next()) {
				IRainfallForecast rainfallForecast = new RainfallForecast();
				rainfallForecast.setRainfallForecastId(rs.getLong("rainfall_forecast_id"));
				rainfallForecast.setLocationUUID(rs.getString("location_uuid"));
				rainfallForecast.setEventGenTS(rs.getLong("event_gen_ts"));
				long foreCastDayInDB = rs.getLong("forecast_day");
				String locUUID = rs.getString("location_uuid");
				rainfallForecast.setForecastDay(foreCastDayInDB);
				rainfallForecast.setInsertTs(rs.getLong("insert_ts"));
				rainfallForecast.setUpdateTs(rs.getLong("update_ts"));
				rainfallForecast.setDeleted(rs.getInt("deleted"));
				
				if(foreCastDayInDB == forecastDay){
					rainfallForecast.setDay1(rs.getDouble("day_1"));
					rainfallForecast.setDay2(rs.getDouble("day_2"));
					rainfallForecast.setDay3(rs.getDouble("day_3"));
					rainfallForecast.setDay4(rs.getDouble("day_4"));
					rainfallForecast.setDay5(rs.getDouble("day_5"));
					rainfallForecast.setDay6(rs.getDouble("day_6"));
					rainfallForecast.setDay7(rs.getDouble("day_7"));
				}else if(foreCastDayInDB == prevFcastDay){
					rainfallForecast.setDay1(rs.getDouble("day_2"));
					rainfallForecast.setDay2(rs.getDouble("day_3"));
					rainfallForecast.setDay3(rs.getDouble("day_4"));
					rainfallForecast.setDay4(rs.getDouble("day_5"));
					rainfallForecast.setDay5(rs.getDouble("day_6"));
					rainfallForecast.setDay6(rs.getDouble("day_7"));
					rainfallForecast.setDay7(-1.0);
				}	
				if(null != locForecasDayMap.get(locUUID)){
					IRainfallForecast rfForecast = locForecasDayMap.get(locUUID);
					if(foreCastDayInDB > rfForecast.getForecastDay()){
						locForecasDayMap.put(locUUID, rainfallForecast);
					}
				}else{		
					locForecasDayMap.put(locUUID, rainfallForecast);
				}
			}
			List<IRainfallForecast> rainfallForecastList = new ArrayList<IRainfallForecast>(locForecasDayMap.values());
			
			return rainfallForecastList;
		} catch (SQLException se) {
			// TODO: Log and throw the right exception
			System.out.println("Error retrieving Rainforecast record  for forecast day : " + forecastDay + " -- " + se);
			throw new FetchingObjectException(
					"Error retrieving Rainforecast record  for forecast day : " + forecastDay, se);
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
	
	
	public long getLastKnownForecastDayBeforeDay(long forecastDay) throws DSPException{
		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
			

			StringBuffer sql = new StringBuffer();
			sql.append("SELECT max(forecast_day) as max_forecast_day ");
			sql.append("FROM rainfall_forecast ");
			sql.append("where forecast_day < ? and deleted =0");

			ps = dataStore.createPreparedStatement(sql.toString());
			ps.setLong(1, forecastDay);
			rs = ps.executeQuery();
			long maxForecastDate = 0;
			while (rs.next()) {
				maxForecastDate = rs.getLong("max_forecast_day");
			}
			return maxForecastDate;
			
		} catch (SQLException se) {
			System.out.println("Error retrieving last known forecast date "+ se);
			throw new FetchingObjectException(
					"Error retrieving last known forecast date ", se);
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
	/*
	 * 1. If forecastDate data exists then return the data.
	 * 2. else
	 * 		getLastKnownForecastDate.
	 * 		If requested forecastDate - lastKnownForecastDate <=6
	 * 			then 
	 * 			return the data starting from column day_{{diff}}
	 * 		else
	 * 			throw NoDataFoundException
	 * 
	 */
	@Override
	public Map<String, IRainfallForecast> getLocationLevelForecastFor(
			long forecastDay) throws DSPException {
		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			Map<String, IRainfallForecast> locForecastMap = getLocationLevelForecast(forecastDay);
			if (locForecastMap != null && locForecastMap.size() > 0)
				return locForecastMap;
			else {
				long maxForecastDate = getLastKnownForecastDayBeforeDay(forecastDay);
				long dayDiff = forecastDay - maxForecastDate; 
				if(dayDiff <= 6){
					dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
					StringBuffer sql = new StringBuffer();
					sql.append("select *");
					sql.append(" from rainfall_forecast");
					sql.append(" where forecast_day = ? and deleted = 0");

					ps = dataStore.createPreparedStatement(sql.toString());
					ps.setLong(1, maxForecastDate);
					rs = ps.executeQuery();
					locForecastMap = new HashMap<String, IRainfallForecast>();
					long day1Col = dayDiff + 1;
					long day2Col = dayDiff + 2;
					long day3Col = dayDiff + 3;
					long day4Col = dayDiff + 4;
					long day5Col = dayDiff + 5;
					long day6Col = dayDiff + 6;
					String dayCollName = "day_";
					String day1ColName = dayCollName + day1Col;
					String day2ColName = dayCollName + day2Col;
					String day3ColName = dayCollName + day3Col;
					String day4ColName = dayCollName + day4Col;
					String day5ColName = dayCollName + day5Col;
					String day6ColName = dayCollName + day6Col;

					System.out.println(" day1Col "+day1Col+" "+day1ColName);
					System.out.println(" day2Col "+day2Col+" "+day2ColName);
					System.out.println(" day3Col "+day3Col+" "+day3ColName);
					System.out.println(" day4Col "+day4Col+" "+day4ColName);
					System.out.println(" day5Col "+day5Col+" "+day5ColName);
					System.out.println(" day6Col "+day6Col+" "+day6ColName);
					while (rs.next()) {
						String locUUID = rs.getString("location_uuid");
						
						IRainfallForecast rainfallForecast = new RainfallForecast();
						rainfallForecast.setRainfallForecastId(rs.getLong("rainfall_forecast_id"));
						rainfallForecast.setLocationUUID(rs.getString("location_uuid"));
						rainfallForecast.setEventGenTS(rs.getLong("event_gen_ts"));
						rainfallForecast.setForecastDay(maxForecastDate);
						/*
						 * Setting -1 to all days Initially.
						 * If day$Col < 7 then -1 will be overwritten by value
						 */

						rainfallForecast.setDay1(-1.0);
						rainfallForecast.setDay2(-1.0);
						rainfallForecast.setDay3(-1.0);
						rainfallForecast.setDay4(-1.0);
						rainfallForecast.setDay5(-1.0);
						rainfallForecast.setDay6(-1.0);
						rainfallForecast.setDay7(-1.0);
						
						if(day1Col <= 7)
							rainfallForecast.setDay1(rs.getDouble(day1ColName));
						if(day2Col <= 7)
							rainfallForecast.setDay2(rs.getDouble(day2ColName));
						if(day3Col <= 7)
							rainfallForecast.setDay3(rs.getDouble(day3ColName));
						if(day4Col <= 7)
							rainfallForecast.setDay4(rs.getDouble(day4ColName));
						if(day5Col <= 7)
							rainfallForecast.setDay5(rs.getDouble(day5ColName));
						if(day6Col <= 7)
							rainfallForecast.setDay6(rs.getDouble(day6ColName));
						
						rainfallForecast.setInsertTs(rs.getLong("insert_ts"));
						rainfallForecast.setUpdateTs(rs.getLong("update_ts"));
						rainfallForecast.setDeleted(rs.getInt("deleted"));
						
						locForecastMap.put(locUUID, rainfallForecast);
					}
				}
				else{
					/*
					 * Throw NoData found Exception
					 */
					System.out.println("getLocationLevelForecastFor :: No Data Found For ForeCastDate "+forecastDay);
				}
				return locForecastMap;
			}
		} catch (SQLException se) {
			// TODO: Log and throw the right exception
			System.out.println("Error retrieving Rainforecast record  for forecast day : " + forecastDay + " -- " + se);
			throw new FetchingObjectException("Error retrieving Rainforecast record  for forecast day : " + forecastDay,
					se);
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
	 * If ForecastDay exists in DB
	 * 		then return
	 * else find the max forecast date available
	 * 		get all rows from maxForecastdate to modelDate - 6
	 * 
	 * TODO :: IWM - 409 What if all the values are in history / half in history and half in table
	 * @param forecastDay
	 * @return
	 * @throws DSPException
	 */
	@Override
	public Map<String, IRainfallForecast> getLastKnownRainfallForecast(
			long forecastDay) throws DSPException {
		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		double DEFAULT_NO_FORECAST_VALUE = -1.0;
		try {
			Map<String, IRainfallForecast> locForecasDayMap = new HashMap<String, IRainfallForecast>();
			long tempDate = forecastDay;
			int dayCount = 6;
			List<Integer> dayList = new ArrayList<Integer>();
			while (dayCount-- >= 0) {
				dayList.add(new Long(tempDate).intValue());
				long prevDayMillis = DateUtils.getModelDateInMillis(new Long(tempDate).intValue())
						- DateUtils.TWENTY_FOUR_HOURS_IN_SECONDS;
				tempDate = DateUtils.getModelDateFromTs(prevDayMillis);
			}
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);

			StringBuffer sql = new StringBuffer();
			sql.append("select *");
			sql.append(" from rainfall_forecast");
			sql.append(" where forecast_day in (");
			sql.append(StringUtils.commaSeparatedListForSQL(dayList));
			sql.append(") and deleted = 0 order by forecast_day desc");
			//System.out.println(sql.toString());
			ps = dataStore.createPreparedStatement(sql.toString());

			rs = ps.executeQuery();
			long dayDiff = 0;
			long previousCheckForecastDay = forecastDay;
			long day1Col = dayDiff + 1;
			long day2Col = dayDiff + 2;
			long day3Col = dayDiff + 3;
			long day4Col = dayDiff + 4;
			long day5Col = dayDiff + 5;
			long day6Col = dayDiff + 6;
			long day7Col = dayDiff + 7;
			String dayCollName = "day_";
			String day1ColName = dayCollName + day1Col;
			String day2ColName = dayCollName + day2Col;
			String day3ColName = dayCollName + day3Col;
			String day4ColName = dayCollName + day4Col;
			String day5ColName = dayCollName + day5Col;
			String day6ColName = dayCollName + day6Col;
			String day7ColName = dayCollName + day7Col;
			IRainfallForecast rainfallForecast;
			while (rs.next()) {
				String locUUID = rs.getString("location_uuid");
				long foreCastDayInDB = rs.getLong("forecast_day");

				if (previousCheckForecastDay != foreCastDayInDB) {
					dayDiff = forecastDay - foreCastDayInDB;
					day1Col = dayDiff + 1;
					day2Col = dayDiff + 2;
					day3Col = dayDiff + 3;
					day4Col = dayDiff + 4;
					day5Col = dayDiff + 5;
					day6Col = dayDiff + 6;
					day7Col = dayDiff + 7;
					dayCollName = "day_";
					day1ColName = dayCollName + day1Col;
					day2ColName = dayCollName + day2Col;
					day3ColName = dayCollName + day3Col;
					day4ColName = dayCollName + day4Col;
					day5ColName = dayCollName + day5Col;
					day6ColName = dayCollName + day6Col;
					day7ColName = dayCollName + day7Col;
					previousCheckForecastDay = foreCastDayInDB;
				}

				if (locForecasDayMap.get(locUUID) != null) {
					rainfallForecast = locForecasDayMap.get(locUUID);
				} else {
					rainfallForecast = new RainfallForecast();
					locForecasDayMap.put(locUUID, rainfallForecast);
					rainfallForecast.setForecastDay(forecastDay);
					rainfallForecast.setLocationUUID(rs.getString("location_uuid"));
					rainfallForecast.setDay1(DEFAULT_NO_FORECAST_VALUE);
					rainfallForecast.setDay2(DEFAULT_NO_FORECAST_VALUE);
					rainfallForecast.setDay3(DEFAULT_NO_FORECAST_VALUE);
					rainfallForecast.setDay4(DEFAULT_NO_FORECAST_VALUE);
					rainfallForecast.setDay5(DEFAULT_NO_FORECAST_VALUE);
					rainfallForecast.setDay6(DEFAULT_NO_FORECAST_VALUE);
					rainfallForecast.setDay7(DEFAULT_NO_FORECAST_VALUE);
				}

				if (day1Col <= 7 && rainfallForecast.getDay1() == DEFAULT_NO_FORECAST_VALUE) {
					rainfallForecast.setRainfallForecastId(rs.getLong("rainfall_forecast_id"));
					rainfallForecast.setEventGenTS(rs.getLong("event_gen_ts"));
					rainfallForecast.setInsertTs(rs.getLong("insert_ts"));
					rainfallForecast.setUpdateTs(rs.getLong("update_ts"));
					rainfallForecast.setDeleted(rs.getInt("deleted"));
					rainfallForecast.setDay1(rs.getDouble(day1ColName));
				}
				if (day2Col <= 7 && rainfallForecast.getDay2() == DEFAULT_NO_FORECAST_VALUE) {
					rainfallForecast.setDay2(rs.getDouble(day2ColName));
				}
				if (day3Col <= 7 && rainfallForecast.getDay3() == DEFAULT_NO_FORECAST_VALUE) {
					rainfallForecast.setDay3(rs.getDouble(day3ColName));
				}
				if (day4Col <= 7 && rainfallForecast.getDay4() == DEFAULT_NO_FORECAST_VALUE) {
					rainfallForecast.setDay4(rs.getDouble(day4ColName));
				}
				if (day5Col <= 7 && rainfallForecast.getDay5() == DEFAULT_NO_FORECAST_VALUE) {
					rainfallForecast.setDay5(rs.getDouble(day5ColName));
				}
				if (day6Col <= 7 && rainfallForecast.getDay6() == DEFAULT_NO_FORECAST_VALUE) {
					rainfallForecast.setDay6(rs.getDouble(day6ColName));
				}
				if (day7Col <= 7 && rainfallForecast.getDay7() == DEFAULT_NO_FORECAST_VALUE) {
					rainfallForecast.setDay7(rs.getDouble(day7ColName));
				}

			}
			return locForecasDayMap;
		} catch (SQLException se) {
			// TODO: Log and throw the right exception
			System.out.println("Error retrieving Rainforecast record  for forecast day : " + forecastDay + " -- " + se);
			throw new FetchingObjectException("Error retrieving Rainforecast record  for forecast day : " + forecastDay,
					se);
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
			if (dataStoreOwnerKey != null) {
				DataStoreContext.clearDataStoreContext(dataStoreOwnerKey);
			}
		}

	}
	
	@Override
	public Map<String, IRainfallForecast> getLastKnownRainfallForecast(List<String> locationUUIDs,
			long forecastDay) throws DSPException {
		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		double DEFAULT_NO_FORECAST_VALUE = -1.0;
		try {
			Map<String, IRainfallForecast> locForecasDayMap = new HashMap<String, IRainfallForecast>();
			long tempDate = forecastDay;
			int dayCount = 6;
			List<Integer> dayList = new ArrayList<Integer>();
			while (dayCount-- >= 0) {
				dayList.add(new Long(tempDate).intValue());
				long prevDayMillis = DateUtils.getModelDateInMillis(new Long(tempDate).intValue())
						- DateUtils.TWENTY_FOUR_HOURS_IN_SECONDS;
				tempDate = DateUtils.getModelDateFromTs(prevDayMillis);
			}
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);

			StringBuffer sql = new StringBuffer();
			sql.append("select *");
			sql.append(" from rainfall_forecast");
			sql.append(" where forecast_day in (");
			sql.append(StringUtils.commaSeparatedListForSQL(dayList));
			sql.append(") ");
			sql.append("and location_uuid in (");
			sql.append(StringUtils.commaSeparatedQuotedStringsForSQL(locationUUIDs));
			sql.append(") and deleted = 0 order by forecast_day desc");
			//System.out.println(sql.toString());
			ps = dataStore.createPreparedStatement(sql.toString());

			rs = ps.executeQuery();
			long dayDiff = 0;
			long previousCheckForecastDay = forecastDay;
			long day1Col = dayDiff + 1;
			long day2Col = dayDiff + 2;
			long day3Col = dayDiff + 3;
			long day4Col = dayDiff + 4;
			long day5Col = dayDiff + 5;
			long day6Col = dayDiff + 6;
			long day7Col = dayDiff + 7;
			String dayCollName = "day_";
			String day1ColName = dayCollName + day1Col;
			String day2ColName = dayCollName + day2Col;
			String day3ColName = dayCollName + day3Col;
			String day4ColName = dayCollName + day4Col;
			String day5ColName = dayCollName + day5Col;
			String day6ColName = dayCollName + day6Col;
			String day7ColName = dayCollName + day7Col;
			IRainfallForecast rainfallForecast;
			while (rs.next()) {
				String locUUID = rs.getString("location_uuid");
				long foreCastDayInDB = rs.getLong("forecast_day");

				if (previousCheckForecastDay != foreCastDayInDB) {
					dayDiff = forecastDay - foreCastDayInDB;
					day1Col = dayDiff + 1;
					day2Col = dayDiff + 2;
					day3Col = dayDiff + 3;
					day4Col = dayDiff + 4;
					day5Col = dayDiff + 5;
					day6Col = dayDiff + 6;
					day7Col = dayDiff + 7;
					dayCollName = "day_";
					day1ColName = dayCollName + day1Col;
					day2ColName = dayCollName + day2Col;
					day3ColName = dayCollName + day3Col;
					day4ColName = dayCollName + day4Col;
					day5ColName = dayCollName + day5Col;
					day6ColName = dayCollName + day6Col;
					day7ColName = dayCollName + day7Col;
					previousCheckForecastDay = foreCastDayInDB;
				}

				if (locForecasDayMap.get(locUUID) != null) {
					rainfallForecast = locForecasDayMap.get(locUUID);
				} else {
					rainfallForecast = new RainfallForecast();
					locForecasDayMap.put(locUUID, rainfallForecast);
					rainfallForecast.setForecastDay(forecastDay);
					rainfallForecast.setLocationUUID(rs.getString("location_uuid"));
					rainfallForecast.setDay1(DEFAULT_NO_FORECAST_VALUE);
					rainfallForecast.setDay2(DEFAULT_NO_FORECAST_VALUE);
					rainfallForecast.setDay3(DEFAULT_NO_FORECAST_VALUE);
					rainfallForecast.setDay4(DEFAULT_NO_FORECAST_VALUE);
					rainfallForecast.setDay5(DEFAULT_NO_FORECAST_VALUE);
					rainfallForecast.setDay6(DEFAULT_NO_FORECAST_VALUE);
					rainfallForecast.setDay7(DEFAULT_NO_FORECAST_VALUE);
				}

				if (day1Col <= 7 && rainfallForecast.getDay1() == DEFAULT_NO_FORECAST_VALUE) {
					rainfallForecast.setRainfallForecastId(rs.getLong("rainfall_forecast_id"));
					rainfallForecast.setEventGenTS(rs.getLong("event_gen_ts"));
					rainfallForecast.setInsertTs(rs.getLong("insert_ts"));
					rainfallForecast.setUpdateTs(rs.getLong("update_ts"));
					rainfallForecast.setDeleted(rs.getInt("deleted"));
					rainfallForecast.setDay1(rs.getDouble(day1ColName));
				}
				if (day2Col <= 7 && rainfallForecast.getDay2() == DEFAULT_NO_FORECAST_VALUE) {
					rainfallForecast.setDay2(rs.getDouble(day2ColName));
				}
				if (day3Col <= 7 && rainfallForecast.getDay3() == DEFAULT_NO_FORECAST_VALUE) {
					rainfallForecast.setDay3(rs.getDouble(day3ColName));
				}
				if (day4Col <= 7 && rainfallForecast.getDay4() == DEFAULT_NO_FORECAST_VALUE) {
					rainfallForecast.setDay4(rs.getDouble(day4ColName));
				}
				if (day5Col <= 7 && rainfallForecast.getDay5() == DEFAULT_NO_FORECAST_VALUE) {
					rainfallForecast.setDay5(rs.getDouble(day5ColName));
				}
				if (day6Col <= 7 && rainfallForecast.getDay6() == DEFAULT_NO_FORECAST_VALUE) {
					rainfallForecast.setDay6(rs.getDouble(day6ColName));
				}
				if (day7Col <= 7 && rainfallForecast.getDay7() == DEFAULT_NO_FORECAST_VALUE) {
					rainfallForecast.setDay7(rs.getDouble(day7ColName));
				}

			}
			return locForecasDayMap;
		} catch (SQLException se) {
			// TODO: Log and throw the right exception
			System.out.println("Error retrieving Rainforecast record  for forecast day : " + forecastDay + " -- " + se);
			throw new FetchingObjectException("Error retrieving Rainforecast record  for forecast day : " + forecastDay,
					se);
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
			if (dataStoreOwnerKey != null) {
				DataStoreContext.clearDataStoreContext(dataStoreOwnerKey);
			}
		}

	}
	
	@Override
	public int getLastForecastDateForLocation(String locationUUID) throws DSPException {
		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);

			StringBuffer sql = new StringBuffer();
			sql.append("SELECT max(forecast_day) as max_forecast_day ");
			sql.append("FROM rainfall_forecast ");
			sql.append("where location_uuid=? and deleted =0");

			ps = dataStore.createPreparedStatement(sql.toString());
			ps.setString(1, locationUUID);
			rs = ps.executeQuery();
			int maxForecastDate = 0;
			while (rs.next()) {
				maxForecastDate = rs.getInt("max_forecast_day");
			}
			return maxForecastDate;
			
		} catch (SQLException | DSPException se) {
			System.out.println("Error retrieving last known forecast date for " + locationUUID + se);
			throw new FetchingObjectException(
					"Error retrieving last known forecast date for " + locationUUID, se);
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
	public boolean isWaterQuantityExist(String location_uuid, long dateTs) throws DSPException{
		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);

			StringBuffer sql = new StringBuffer();
			sql.append("SELECT water_quantity_id ");
			sql.append("FROM water_quantity_rf ");
			sql.append("WHERE location_uuid= ? and update_ts >= ?");

			ps = dataStore.createPreparedStatement(sql.toString());
			ps.setString(1, location_uuid);
			ps.setLong(2, dateTs);
			rs = ps.executeQuery();
			if(rs.next()){
				return true;
			}
			
			return false;
			
		} catch (SQLException | DSPException se) {
			System.out.println("Error checking if water quantity exists for date " + dateTs + se);
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
		return false;
	}
	@Override
	public Map<String, IRainfallForecast> getRFData(String location_uuid, long dateTs) throws DSPException{
		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		Map<String, IRainfallForecast> locForecastDayMap = null;
		try {
			locForecastDayMap = new HashMap<String, IRainfallForecast>();
			
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);

			StringBuffer sql = new StringBuffer();
			/**
			 * which table to use water_quantity_rf OR rainfall_forecast ?
			 */
			sql.append("SELECT * ");
			sql.append("FROM water_quantity_rf ");
			sql.append("WHERE location_uuid= ? order by update_ts desc limit 1");

			ps = dataStore.createPreparedStatement(sql.toString());
			ps.setString(1, location_uuid);
			rs = ps.executeQuery();
			IRainfallForecast rainfallForecast;
			if(rs.next()){
				rainfallForecast = new RainfallForecast();
				//add required fields into rainfallForecast class
				locForecastDayMap.put(location_uuid, rainfallForecast);
			}
			return locForecastDayMap;
		} 
		catch (SQLException se) {
			// TODO: Log and throw the right exception
			System.out.println("Error retrieving Rainforecast data for forecast date : " + dateTs + " -- " + se);
		} 
		finally {
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
			if (dataStoreOwnerKey != null) {
				DataStoreContext.clearDataStoreContext(dataStoreOwnerKey);
			}
		}
		return locForecastDayMap;
	}
	
	
	
	
	
	
	
	

}
