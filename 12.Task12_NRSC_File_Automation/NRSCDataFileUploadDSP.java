package com.vassarlabs.iwm.soilmoisture.stress.dsp.impl;

import static com.vassarlabs.iwm.utils.IWMConstants.IS_DEBUG_ENABLED;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.vassarlabs.common.dsp.context.DataStoreContext;
import com.vassarlabs.common.dsp.err.DSPException;
import com.vassarlabs.common.dsp.err.FetchingObjectException;
import com.vassarlabs.common.dsp.err.ObjectCreationException;
import com.vassarlabs.common.dsp.rdbms.api.IRDBMSDataStore;
import com.vassarlabs.iwm.soilmoisture.stress.dsp.api.INRSCDataFileUploadDSP;
import com.vassarlabs.iwm.soilmoisture.stress.pojo.api.INRSCFileUpload;
import com.vassarlabs.iwm.soilmoisture.stress.pojo.api.ISoilMoistureNRSCData;
import com.vassarlabs.iwm.soilmoisture.stress.pojo.impl.NRSCFileUpload;

@Component
public class NRSCDataFileUploadDSP implements INRSCDataFileUploadDSP {

	@Autowired
	@Qualifier("business_data")
	protected IRDBMSDataStore dataStore;

	@Override
	public void insertSoilMoistureNRSCData(List<ISoilMoistureNRSCData> nrscDataList, INRSCFileUpload nrscFileUpload)
			throws DSPException {

		String dataStoreOwnerKey = null;
		String transactionId = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		boolean result = false;

		// Insert NRSC File Upload record

		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
			transactionId = dataStore.beginTransaction();

			nrscFileUpload.setNoOfEntries(nrscDataList.size());
			nrscFileUpload.setFileUploadTS(DataStoreContext.getCurrentTS());
			//nrscFileUpload = insertNRSCFileUpload(nrscFileUpload);

			StringBuffer sql = new StringBuffer();
			sql.append("insert into soil_moisture_nrsc_data");
			sql.append(" ( grid_id, nrsc_file_upload_id, model_date, evapotranspiration");
			sql.append(", runoff, soil_moisture_L1, soil_moisture_L2, soil_moisture_L3");
			sql.append(", is_forecasted_data, insert_ts, update_ts, deleted, user_session_id");
			sql.append(") values (");
			sql.append(" ?, ?, ?, ?,");
			sql.append(" ?, ?, ?, ?,");
			sql.append(" ?, ?, null, 0, 0);");

			ps = dataStore.createPreparedStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);

			for (ISoilMoistureNRSCData nrscData : nrscDataList) {

				ps.setLong(1, nrscData.getGridId());
				ps.setLong(2, nrscFileUpload.getFileUploadId());
				ps.setInt(3, nrscData.getModelDate());
				ps.setDouble(4, nrscData.getEvapotranspiration());
				ps.setDouble(5, nrscData.getRunoff());
				ps.setDouble(6, nrscData.getSoilMoistureL1());
				ps.setDouble(7, nrscData.getSoilMoistureL2());
				ps.setDouble(8, nrscData.getSoilMoistureL3());
				ps.setInt(9, nrscData.isForecastedData());
				ps.setLong(10, DataStoreContext.getCurrentTS());
				ps.addBatch();
			}

			int[] nrscDataIdList = ps.executeBatch();
			if (nrscDataIdList.length != nrscDataList.size()) {
				if (IS_DEBUG_ENABLED) {
					System.out.println("Error inserting ISoilMoistureNRSCData - list size : " + nrscDataList.size()
							+ " -- no. of records inserted is : " + nrscDataIdList.length);
				}
				throw new ObjectCreationException("Error inserting ISoilMoistureNRSCData - list size : "
						+ nrscDataList.size() + " -- no. of records inserted is : " + nrscDataIdList.length);
			}

			dataStore.commitTransaction(transactionId);
			result = true;
		} catch (SQLException e) {
			System.out.println("Error inserting NRSC record with object, " + e.getMessage());

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
	}
	
	@Override
	public void replaceSoilMoistureNRSCForecastData(List<ISoilMoistureNRSCData> nrscDataList)
			throws DSPException {

		String dataStoreOwnerKey = null;
		String transactionId = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		boolean result = false;

		// Insert NRSC File Upload record

		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
			transactionId = dataStore.beginTransaction();

			StringBuffer sql = new StringBuffer();
			
			sql.append("update soil_moisture_nrsc_data set model_date = ?, evapotranspiration = ?");
			sql.append(", runoff = ?, soil_moisture_L1 = ?, soil_moisture_L2 = ?, soil_moisture_L3 = ?");
			sql.append(", insert_ts = ? where model_date = ?");

			ps = dataStore.createPreparedStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);

			for (ISoilMoistureNRSCData nrscData : nrscDataList) {

				ps.setInt(1, nrscData.getModelDate());
				ps.setDouble(2, nrscData.getEvapotranspiration());
				ps.setDouble(3, nrscData.getRunoff());
				ps.setDouble(4, nrscData.getSoilMoistureL1());
				ps.setDouble(5, nrscData.getSoilMoistureL2());
				ps.setDouble(6, nrscData.getSoilMoistureL3());
				ps.setLong(7, DataStoreContext.getCurrentTS());
				ps.setInt(8, nrscData.getModelDate());
				
				ps.addBatch();
			}

			int[] nrscDataIdList = ps.executeBatch();
			if (nrscDataIdList.length != nrscDataList.size()) {
				if (IS_DEBUG_ENABLED) {
					System.out.println("Error inserting ISoilMoistureNRSCData - list size : " + nrscDataList.size()
							+ " -- no. of records inserted is : " + nrscDataIdList.length);
				}
				throw new ObjectCreationException("Error inserting ISoilMoistureNRSCData - list size : "
						+ nrscDataList.size() + " -- no. of records inserted is : " + nrscDataIdList.length);
			}

			dataStore.commitTransaction(transactionId);
			result = true;
		} catch (SQLException e) {
			System.out.println("Error inserting NRSC record with object, " + e.getMessage());

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
	}

	@Override
	public void replaceSoilMoistureNRSCForecastDataByForecastdata(List<ISoilMoistureNRSCData> nrscDataList)
			throws DSPException {
		String dataStoreOwnerKey = null;
		String transactionId = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		boolean result = false;

		// Insert NRSC File Upload record

		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
			transactionId = dataStore.beginTransaction();

			StringBuffer sql = new StringBuffer();
			
			sql.append("update soil_moisture_nrsc_data set model_date = ?, evapotranspiration = ?");
			sql.append(", runoff = ?, soil_moisture_L1 = ?, soil_moisture_L2 = ?, soil_moisture_L3 = ?");
			sql.append(", insert_ts = ? where model_date = ?");

			ps = dataStore.createPreparedStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);

			for (ISoilMoistureNRSCData nrscData : nrscDataList) {

				ps.setInt(1, nrscData.getModelDate());
				ps.setDouble(2, nrscData.getEvapotranspiration());
				ps.setDouble(3, nrscData.getRunoff());
				ps.setDouble(4, nrscData.getSoilMoistureL1());
				ps.setDouble(5, nrscData.getSoilMoistureL2());
				ps.setDouble(6, nrscData.getSoilMoistureL3());
				ps.setLong(7, DataStoreContext.getCurrentTS());
				ps.setInt(8, nrscData.getModelDate());
				
				//ps.addBatch();
			}

			int[] nrscDataIdList = ps.executeBatch();
			if (nrscDataIdList.length != nrscDataList.size()) {
				if (IS_DEBUG_ENABLED) {
					System.out.println("Error inserting ISoilMoistureNRSCData - list size : " + nrscDataList.size()
							+ " -- no. of records inserted is : " + nrscDataIdList.length);
				}
				throw new ObjectCreationException("Error inserting ISoilMoistureNRSCData - list size : "
						+ nrscDataList.size() + " -- no. of records inserted is : " + nrscDataIdList.length);
			}

			dataStore.commitTransaction(transactionId);
			result = true;
		} catch (SQLException e) {
			System.out.println("Error inserting NRSC record with object, " + e.getMessage());

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
		
	}
	
	@Override
	public void deleteOldWeatherForecastRecords() throws DSPException {
		/**
		 * 1. Find the event_gen_ts of all the files that are inserted 2. find
		 * the last second event_gen_ts 3. move all the records which are less
		 * than this event_gen_ts to history table 4. delete all the records
		 * which are less than this event_gen_ts from the main table
		 */
		/*
		 * long currMillis = System.currentTimeMillis(); List<Long>
		 * eventGenTsList = isroDsp.getEventGenTsOfFilesInserted(currMillis);
		 * if(eventGenTsList.size() < 3){ System.out.
		 * println("ISRODataService :: deleteOldWeatherForecastRecords :: number  of files inserted are less than or equal to 2, returning"
		 * ); return; } List<Long> referenceList = eventGenTsList.subList(2,
		 * eventGenTsList.size());
		 * 
		 * System.out.
		 * println("ISRODataService :: deleteOldWeatherForecastRecords :: copying the records whose event_gen_ts are less than "
		 * + referenceList);
		 * 
		 * boolean result =
		 * isroDsp.copyWeatherForecastDataToHistoryData(referenceList);
		 * if(!result){ System.out.
		 * println("ISRODataService :: deleteOldWeatherForecastRecords :: copying of records whose event_gen_ts are less than "
		 * +
		 * referenceList+" Not successfull, so not going forward with deleting the records.  "
		 * ); } else{ System.out.
		 * println("ISRODataService :: deleteOldWeatherForecastRecords :: copying Done of records whose event_gen_ts are less than "
		 * + referenceList); System.out.
		 * println("ISRODataService :: deleteOldWeatherForecastRecords :: deleting  records whose event_gen_ts are less than "
		 * + referenceList);
		 * 
		 * isroDsp.deleteWeatherForecastRecords(referenceList);
		 * 
		 * System.out.
		 * println("ISRODataService :: deleteOldWeatherForecastRecords :: deleting  Done of records whose event_gen_ts are less than "
		 * + referenceList); }
		 */
	}

	@Override
	public void insertNRSCFileUploadData(String fileName, File file) throws DSPException, IOException {

		String dataStoreOwnerKey = null;
		String transactionId = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		boolean result = false;

		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);

			transactionId = dataStore.beginTransaction();

			StringBuffer sql = new StringBuffer();
			sql.append(
					"INSERT into nrsc_file_upload (filename, file_upload_ts, no_of_records, insert_ts, update_ts, deleted, user_session_id, last_modified_ts, is_forecasted_data)");
			sql.append(" values (?, ?, ?, ?, null, 0, 0, ?, ?);");

			ps = dataStore.createPreparedStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);

			ps.setString(1, fileName);
			ps.setLong(2, DataStoreContext.getCurrentTS());
			int noOfRecordsInFile = 0;
			noOfRecordsInFile = getTotalRecords(file);
			ps.setInt(3, noOfRecordsInFile);
			ps.setLong(4, DataStoreContext.getCurrentTS());
			ps.setLong(5, file.lastModified());
			if(fileName.contains("forecast")){
				short flag = 1;
				ps.setShort(6, flag);
			}
			else{
				short flag = 0;
				ps.setShort(6, flag);
			}

			if (IS_DEBUG_ENABLED) {
				System.out.println(ps.toString());
			}

			ps.executeUpdate();

			long nrscFileUploadId = -1;
			rs = ps.getGeneratedKeys();
			if (rs.next()) {
				nrscFileUploadId = rs.getLong(1);
			} else {
				if (IS_DEBUG_ENABLED) {
					System.out.println("Error: Error occured while getting auto generated keys");
				}
				
			}

			dataStore.commitTransaction(transactionId);

			
		} catch (SQLException e) {
			System.out.println("Error inserting nrsc file upload : " + fileName + " -- " + e);
			throw new ObjectCreationException("Error inserting nrsc file upload : " + fileName + " -- ", e);
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
	public int getTotalRecords(File file) throws IOException{
		int count = 0;
		InputStream is = new BufferedInputStream(new FileInputStream(file));
	    try {
	        byte[] c = new byte[1024];
	        int readChars = 0;
	        boolean endsWithoutNewLine = false;
	        while ((readChars = is.read(c)) != -1) {
	            for (int i = 0; i < readChars; ++i) {
	                if (c[i] == '\n')
	                    ++count;
	            }
	            endsWithoutNewLine = (c[readChars - 1] != '\n');
	        }
	        if(endsWithoutNewLine) {
	            ++count;
	        } 
	        return count;
	    } finally {
	        is.close();
	    }
	}
	@Override
	public INRSCFileUpload insertNRSCFileUpload(INRSCFileUpload nrscFileUpload) throws DSPException {

		String dataStoreOwnerKey = null;
		String transactionId = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		boolean result = false;

		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);

			transactionId = dataStore.beginTransaction();

			StringBuffer sql = new StringBuffer();
			sql.append(
					"INSERT into nrsc_file_upload (filename, file_upload_ts, no_of_records, insert_ts, update_ts, deleted, user_session_id, last_modified_ts, is_forecasted_data)");
			sql.append(" values (?, ?, ?, ?, null, 0, 0, ?, ?);");

			ps = dataStore.createPreparedStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);

			ps.setString(1, nrscFileUpload.getFileName());
			ps.setLong(2, nrscFileUpload.getFileUploadTS());
			ps.setInt(3, nrscFileUpload.getNoOfEntries());
			ps.setLong(4, DataStoreContext.getCurrentTS());
			ps.setLong(5, nrscFileUpload.getLastModified());
			ps.setShort(6, nrscFileUpload.getIsForecasted());

			if (IS_DEBUG_ENABLED) {
				System.out.println(ps.toString());
			}

			ps.executeUpdate();

			long nrscFileUploadId = -1;
			rs = ps.getGeneratedKeys();
			if (rs.next()) {
				nrscFileUploadId = rs.getLong(1);
			} else {
				if (IS_DEBUG_ENABLED) {
					System.out.println("Error: Error occured while getting auto generated keys");
				}
				throw new ObjectCreationException("Error creating NRSC file upload record  = " + nrscFileUpload
						+ " - while getting auto generated keys");
			}

			dataStore.commitTransaction(transactionId);

			nrscFileUpload.setFileUploadId(nrscFileUploadId);
			nrscFileUpload.setInsertTs(DataStoreContext.getCurrentTS());
			result = true;
			if (IS_DEBUG_ENABLED) {
				System.out.println(nrscFileUpload);
			}
			return nrscFileUpload;
		} catch (SQLException e) {
			System.out.println("Error inserting nrsc file upload : " + nrscFileUpload + " -- " + e);
			throw new ObjectCreationException("Error inserting nrsc file upload : " + nrscFileUpload + " -- ", e);
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
	public void updateNRSCMetaData(int modelDate, File file, INRSCFileUpload inrscFileUpload) throws DSPException{
		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		String transactionId = null;
		String fileName = file.getName();

		try {
			
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);
			
			transactionId = dataStore.beginTransaction();
			StringBuffer sql = new StringBuffer();
			sql.append("update nrsc_file_upload set file_upload_ts = ?,  filename = ?, insert_ts = ?, last_modified_ts = ?, is_forecasted_data = ? ");
			sql.append("where filename like ? and deleted = 0");
			
			ps = dataStore.createPreparedStatement(sql.toString(), Statement.RETURN_GENERATED_KEYS);
			ps.setLong(1, DataStoreContext.getCurrentTS());
			ps.setString(2, fileName);
			ps.setLong(3, DataStoreContext.getCurrentTS());
			ps.setLong(4, file.lastModified());
			

			if(fileName.contains("forecast")){
				short flag = 1;
				ps.setShort(5, flag);
				System.out.println("Contains forecast");
			}
			else{
				System.out.println("Does not Contains forecast , filename="+fileName);
				short flag = 0;
				ps.setShort(5, flag);
			}
			ps.setString(6, "%" + modelDate + "%");
			if (IS_DEBUG_ENABLED) {
				System.out.println(ps.toString());
			}
			System.out.println(ps.toString());
			
			int result = ps.executeUpdate();
			dataStore.commitTransaction(transactionId);
			if(result < 0){
				System.out.println("Updation of matadata failed for file : " + fileName);
			}
			else if(result > 0){
				System.out.println("Updation of matadata done for file : " + fileName);
			}
			
		} catch (SQLException se) {
			// TODO: Log and throw the right exception
			System.out.println("Error retrieving INRSCFileUpload record  for file : " + fileName);
			se.printStackTrace();
			throw new FetchingObjectException("Error retrieving INRSCFileUpload record  for file : " + fileName, se);
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
	public INRSCFileUpload getNRSCFileUpload(int modelDate) throws DSPException {

		// String fileName =
		// fullFilePathName.substring(fullFilePathName.lastIndexOf("/"),
		// fullFilePathName.length());

		String dataStoreOwnerKey = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			dataStoreOwnerKey = DataStoreContext.initReusableDataStoreContext(dataStore);

			StringBuffer sql = new StringBuffer();
			sql.append("select *");
			sql.append(" from nrsc_file_upload");
			sql.append(" where filename like ? and deleted = 0");

			ps = dataStore.createPreparedStatement(sql.toString());
			ps.setString(1, "%" + modelDate + "%");
			
			if (IS_DEBUG_ENABLED) {
				System.out.println(ps.toString());
			}

			rs = ps.executeQuery();

			INRSCFileUpload nrscFileUpload = null;
			while (rs.next()) {
				if (nrscFileUpload == null) {
				nrscFileUpload = new NRSCFileUpload();

				nrscFileUpload.setFileUploadId(rs.getLong("nrsc_file_upload_id"));
				nrscFileUpload.setFileUploadTS(rs.getLong("file_upload_ts"));
				nrscFileUpload.setNoOfEntries(rs.getInt("no_of_records"));
				nrscFileUpload.setFileName(rs.getString("filename"));
				nrscFileUpload.setInsertTs(rs.getLong("insert_ts"));
				nrscFileUpload.setUpdateTs(rs.getLong("update_ts"));
				nrscFileUpload.setDeleted(rs.getInt("deleted"));
				nrscFileUpload.setUserSessionId(rs.getLong("user_session_id"));
				nrscFileUpload.setIsForecasted(rs.getShort("is_forecasted_data"));
				nrscFileUpload.setLastModified(rs.getLong("last_modified_ts"));
				}
			}

			return nrscFileUpload;
		} catch (SQLException se) {
			// TODO: Log and throw the right exception
			System.out.println("Error retrieving INRSCFileUpload record  for date : " + modelDate);
			throw new FetchingObjectException("Error retrieving INRSCFileUpload record  for date : " + modelDate, se);
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

	
}
