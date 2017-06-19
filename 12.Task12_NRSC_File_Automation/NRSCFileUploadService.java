package com.vassarlabs.iwm.soilmoisture.stress.service;

import static com.vassarlabs.iwm.utils.IWMConstants.IS_DEBUG_ENABLED;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.vassarlabs.common.dsp.err.DSPException;
import com.vassarlabs.common.dsp.utils.DSPConstants;
import com.vassarlabs.common.utils.err.GenericVLException;
import com.vassarlabs.iwm.soilmoisture.stress.dsp.api.INRSCDataFileUploadDSP;
import com.vassarlabs.iwm.soilmoisture.stress.pojo.api.INRSCFileUpload;
import com.vassarlabs.iwm.soilmoisture.stress.pojo.api.ISoilMoistureNRSCData;
import com.vassarlabs.iwm.soilmoisture.stress.pojo.impl.NRSCFileUpload;
import com.vassarlabs.iwm.soilmoisture.stress.pojo.impl.SoilMoistureNRSCData;
import com.vassarlabs.iwm.soilmoisture.stress.service.api.INRSCFileUploadService;
import com.vassarlabs.iwm.soilmoisture.stress.service.api.ISoilMoistureNRSCDataService;
import com.vassarlabs.iwm.utils.IWMConstants;

@Component
public class NRSCFileUploadService
	implements INRSCFileUploadService {
	
	@Autowired
	protected INRSCDataFileUploadDSP nrscDataFileUploadDSP;
	
	@Autowired
	protected ISoilMoistureNRSCDataService nrscDataService;
	
	
	@Override 
	public void insertNRSCDataFromFile(int modelDate, File file, long lastModified) throws DSPException, IOException{
		String fileName = file.getName();
		boolean isForecastFile = fileName.contains("forecast");

		/*
		 * if acutal actual ignore 
		 * if actual forecast then ignore 
		 * if forecast forecast then replace
		 */
		INRSCFileUpload inrscFileUpload = nrscDataFileUploadDSP.getNRSCFileUpload(modelDate);
		
		if (inrscFileUpload == null) {
			System.out.println("No initial data found so inserting current data...");
			nrscDataFileUploadDSP.insertNRSCFileUploadData(fileName, file);
			insertNRSCDataFromFile(modelDate, file);
			return;
		}
		
		int isForecasted = inrscFileUpload.getIsForecasted();
		long lastModifiedFileInDB = inrscFileUpload.getLastModified();

		if (!isForecastFile && (isForecasted==1)) {
			// replace existing forecast
			System.out.println("Found a new actual data file and replacing forecasting data...");
			//Update file meta data
			nrscDataFileUploadDSP.updateNRSCMetaData(modelDate, file, inrscFileUpload);
			replaceNRSCForecastDataByActualDataFromFile(file.getName(), file);
			
		} else if (isForecastFile && (isForecasted==1) && lastModified > lastModifiedFileInDB) {
			// replace old forecast with new forecast
			System.out.println("Found a new forecast data file so replacing old forecast data...");
			//Update file meta data
			nrscDataFileUploadDSP.updateNRSCMetaData(modelDate, file, inrscFileUpload);
			replaceNRSCForecastDataByForecastDataFromFile(file.getName(), file);
		}
	}
	
	@Override
	public void insertNRSCDataFromFile(int modelDate, File file) {
		
		String fileName = file.getName();
		
		List<ISoilMoistureNRSCData> smNRSCDataRecords = null;
		try {
			smNRSCDataRecords = readNCRSDataFromFile(fileName,file);
		} catch (GenericVLException | DSPException ge) {
			System.out.println("Error reading from NRSC file : " + fileName + " - " + ge.getMessage());
		}

		INRSCFileUpload nrscFileUpload = new NRSCFileUpload();
		nrscFileUpload.setFileName(fileName);
		if(fileName.contains("forecast")){
			short flag = 1;
			nrscFileUpload.setIsForecasted(flag);
		}
		else{
			short flag = 0;
			nrscFileUpload.setIsForecasted(flag);
		}
		nrscFileUpload.setLastModified(file.lastModified());
		try {
			nrscDataFileUploadDSP.insertSoilMoistureNRSCData(smNRSCDataRecords, nrscFileUpload);
		} catch (DSPException de) {
			System.out.println("Error inserting soil moisture data from NRSC file : " + fileName + " - " + de.getMessage());
			
		}
	}
	
	@Override
	public void replaceNRSCForecastDataByActualDataFromFile(String fileName, File file) throws DSPException {
		
		List<ISoilMoistureNRSCData> smNRSCDataRecords = null;
		try {
			smNRSCDataRecords = readNCRSDataFromFile(fileName,file);
		} catch (GenericVLException | DSPException ge) {
			System.out.println("Error reading from NRSC file : " + fileName + " - " + ge.getMessage());
		}

		try {
			nrscDataFileUploadDSP.replaceSoilMoistureNRSCForecastData(smNRSCDataRecords);
		} catch (DSPException de) {
			System.out.println("Error inserting soil moisture data from NRSC file : " + fileName + " - " + de.getMessage());
			
		}
	}
	
	@Override
	public void replaceNRSCForecastDataByForecastDataFromFile(String fileName, File file) throws DSPException {
		
		List<ISoilMoistureNRSCData> smNRSCDataRecords = null;
		try {
			smNRSCDataRecords = readNCRSDataFromFile(fileName,file);
		} catch (GenericVLException | DSPException ge) {
			System.out.println("Error reading from NRSC file : " + fileName + " - " + ge.getMessage());
		}
		
		try {
			nrscDataFileUploadDSP.replaceSoilMoistureNRSCForecastDataByForecastdata(smNRSCDataRecords);
		} catch (DSPException de) {
			System.out.println("Error inserting soil moisture data from NRSC file : " + fileName + " - " + de.getMessage());
			
		}
	}
	
	@Override
	public List<String> processNRSCDataFromFolder(String folderPath) {

		// Logic
		// 1. Get List of files from folderPath
		// 2. For each file do
		// 3.   Check if the file has been processed already, if yes process next file
		// 4.   Read contents of file, and validate numeric data
		// 5.   Create list of ISoilMoistureNRSCData
		// 6.   Begin Transaction
		// 7. 	  Create a record in sm_nrsc_file_upload
		// 8.     Insert all records from 5 above
		// 9.   Commit transaction
		// 10. Process next file
		
		List<String> filesNameList = listFilesForFolder(folderPath);
		List<String> errorList = new ArrayList<String>(); 
		for (String fileName : filesNameList) {
			List<String> fileUploadErrors = processNRSCDataFromFile(fileName);
			errorList.addAll(fileUploadErrors);
		}
		return errorList;
	}

	@Override
	public List<String> processNRSCDataFromFile(String fileName) {
		return processNRSCDataFromFile(getDateAsModelDate(fileName));
	}
	
	@Override
	public int getDateAsModelDate(String fileName){
		
		//Sample file name NRSC_VIC_AP_3min_WBC_20161201.txt
		String result = fileName;
		
		String[] tmp = fileName.split("_");
		result = tmp[tmp.length-1].split("\\.")[0];
		
		return Integer.parseInt(result);
	}
	
	@Override
	public List<String> processNRSCDataFromFile(int modelDate, File file) {
		String fileName = file.getName();
		//String fileName = file.getName();
		List<String> fileUploadErrors = new ArrayList<String>();
		
		long startTs = System.currentTimeMillis();
		if (IS_DEBUG_ENABLED) {
			System.out.println("Started to process NRSC File : " + fileName);
		}
		
		INRSCFileUpload inrscFileUpload = null;
		try {
			inrscFileUpload = nrscDataFileUploadDSP.getNRSCFileUpload(modelDate);
		} catch (DSPException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("Error checking if NRSC File : " + fileName + " is already uploaded -- " + e.getMessage());
			fileUploadErrors.add("Error checking if NRSC File : " + fileName + " is already uploaded -- " + e.getMessage());
			return fileUploadErrors;
		}
		
		if (inrscFileUpload != null) {
			// File has already been uploaded
			if (IS_DEBUG_ENABLED) {
				System.out.println("NRSC File : " + fileName + " is already uploaded, ignoring the file");
			}
			fileUploadErrors.add("NRSC File : " + fileName + " is already uploaded, ignoring the file");
			return fileUploadErrors;
		}
		
		List<ISoilMoistureNRSCData> smNRSCDataRecords = null;
		try {
			smNRSCDataRecords = readNCRSDataFromFile(fileName,file);
		} catch (GenericVLException | DSPException ge) {
			System.out.println("Error reading from NRSC file : " + fileName + " - " + ge.getMessage());
			fileUploadErrors.add("Error reading from NRSC file : " + fileName + " - " + ge.getMessage());
			ge.printStackTrace();
			return fileUploadErrors;
		}

		INRSCFileUpload nrscFileUpload = new NRSCFileUpload();
		nrscFileUpload.setFileName(fileName);
		if(fileName.contains("forecast")){
			short flag = 1;
			nrscFileUpload.setIsForecasted(flag);
		}
		else{
			short flag = 0;
			nrscFileUpload.setIsForecasted(flag);
		}
		nrscFileUpload.setLastModified(file.lastModified());
		try {
			nrscDataFileUploadDSP.insertSoilMoistureNRSCData(smNRSCDataRecords, nrscFileUpload);
		} catch (DSPException de) {
			System.out.println("Error inserting soil moisture data from NRSC file : " + fileName + " - " + de.getMessage());
			fileUploadErrors.add("Error inserting soil moisture data from NRSC file : " + fileName + " - " + de.getMessage());
			de.printStackTrace();
			return fileUploadErrors;
		}
		if (IS_DEBUG_ENABLED) {
			System.out.println("Time taken to upload NRSC file : " + fileName + " is " + (System.currentTimeMillis() - startTs));
		}
		return fileUploadErrors;
	}

	protected List<String> listFilesForFolder(String filepath) {
		File folder = new File(filepath);
		List<String> fileNamesList = new ArrayList<String>(); 
		for (final File fileEntry : folder.listFiles()) {
	        if (fileEntry.isFile()) {
	        	fileNamesList.add(fileEntry.getAbsolutePath());
	        }
	    }
		return fileNamesList;
	}

	protected List<ISoilMoistureNRSCData> readNCRSDataFromFile(String fileName, File file)
		throws GenericVLException, DSPException {

		int modelDate = extractModelDate(fileName);
		int isForecastedData = isForecastedData(fileName);
		
		Map<String, Long> gpsToGridIDMap = nrscDataService.getAllGPSToGridIDMap();
		
		List<ISoilMoistureNRSCData> nrscDataList = new ArrayList<ISoilMoistureNRSCData>();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file));
			
		    String line;
		    boolean validData;
		    while ((line = br.readLine()) != null) {
		    	String[] attributes = line.split(" ");
		    	
		    	validData = validateData(attributes);
		    	if (!validData) {
					if (IS_DEBUG_ENABLED) {
						System.out.println("Invalid record found (ignoring record) : " + line);
					}
					continue;
		    	}
		    	
		    	ISoilMoistureNRSCData soilMoistureNRSCData = new SoilMoistureNRSCData();
		    	
		    	/* 
		    	 * attribute[1] is latitude, attribute[2] is longitude
		    	 * preparing a string of format "LAT LONG", because we should fetch the gridID based on GPS
		    	 * instead of using it directly from file
		    	 * 
		    	 * */
		    	String coordinates = attributes[1]+" "+attributes[2];
		    	if (!gpsToGridIDMap.containsKey(coordinates)) {
		    		if (IS_DEBUG_ENABLED) {
			    		System.out.println("NRSCFileUploadService - No corresponding grid has been found "
			    				+ "in the system for coordinates = " + coordinates);
		    		}
		    		continue;
		    	}
		    	
				long gridID = gpsToGridIDMap.get(coordinates);
				
				soilMoistureNRSCData.setGridId(gridID);
				soilMoistureNRSCData.setModelDate(modelDate);
				soilMoistureNRSCData.setEvapotranspiration(((Double.parseDouble(attributes[3]))));
				soilMoistureNRSCData.setRunoff(((Double.parseDouble(attributes[4]))));
				soilMoistureNRSCData.setSoilMoistureL1((Double.parseDouble(attributes[5])));
				soilMoistureNRSCData.setSoilMoistureL2((Double.parseDouble(attributes[6])));
				soilMoistureNRSCData.setSoilMoistureL3((Double.parseDouble(attributes[7])));
				soilMoistureNRSCData.setIsForecastedData(isForecastedData);
				
				nrscDataList.add(soilMoistureNRSCData);
		    }
		    return nrscDataList;
		} catch (Exception e) {
			System.out.println("Error reading/parsing file : " + fileName + " -- " + e);
			e.printStackTrace();
			throw new GenericVLException("Error reading/parsing file : " + fileName, e);
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					// Do Nothing
				}
			}
		}
	}
	
	private boolean validateData(String[] attributes) {

		boolean validData =  true;
		if (attributes == null || attributes.length == 0 || attributes.length < 8) {
			
			if (IS_DEBUG_ENABLED) {
				System.out.println("Invalid record with attributes[] length : " + attributes);
			}
			validData = false;
			return validData;
		}
		
		Double dValue = null;
		for (int i=3; i< attributes.length; i++) {
			try {
				dValue = Double.parseDouble(attributes[i]);
				if(dValue.isNaN()) {
					if (IS_DEBUG_ENABLED) {
						System.out.println("Invalid record data (isNan) : " + dValue);
					}
					validData = false;
					return validData;
				}
				if (dValue < 0) {
					if (IS_DEBUG_ENABLED) {
						System.out.println("Invalid record data (negative value) : " + dValue);
					}
					validData = false;
					return validData;
				}
			} catch (NumberFormatException nfe) {
				System.out.println("Invalid record data :  " + attributes + " : " + nfe.getMessage());
				validData = false;
				return validData;
			}
    	}
		return validData;
	}

	protected int extractModelDate(String fileName)
		throws GenericVLException {

		if (fileName.contains("_")) {
			String modalDate =  fileName.substring(fileName.lastIndexOf('_')+1, fileName.lastIndexOf('.'));
		    return Integer.parseInt(modalDate);
		} else {
		    throw new GenericVLException("Invalid file name format - contains no '_' : " + fileName);
		}
	}

	protected int isForecastedData(String fileName) throws GenericVLException {
		if (fileName.toLowerCase().contains(IWMConstants.SM_NRSC_FORECAST_FILE_IDENTIFIER.toLowerCase())) {
			return DSPConstants.DB_TRUE;
		} 
		return DSPConstants.DB_FALSE;
	}
	
	@Override
	public List<String> processNRSCDataFromFile(File file) throws GenericVLException {
		 return processNRSCDataFromFile(extractModelDate(file.getName()),file);
	}

	@Override
	public List<String> processNRSCDataFromFile(int modelDate) {
		// TODO Auto-generated method stub
		return null;
	}

	
}
