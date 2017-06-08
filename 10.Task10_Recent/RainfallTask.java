package com.vassarlabs.test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.expression.ParseException;
import org.springframework.stereotype.Component;

import com.vassarlabs.common.dsp.err.DSPException;
import com.vassarlabs.common.init.err.AppInitializationException;
import com.vassarlabs.common.init.service.api.IApplicationInitService;
import com.vassarlabs.common.utils.err.ObjectNotFoundException;
import com.vassarlabs.config.spring.AppContext;
import com.vassarlabs.location.service.api.ILocationHierarchyService;
import com.vassarlabs.location.utils.LocationConstants;
import com.vassarlabs.spatial.ext.service.api.IGPSExtensionService;
import com.vividsolutions.jts.geom.Geometry;

@Component
public class RainfallTask {

	public static BufferedWriter bw = null;
	public static FileWriter fw = null;


	@Autowired
	ILocationHierarchyService locationHierarchyService;

	@Autowired
	@Qualifier("RESTInitServiceImpl")
	protected IApplicationInitService applicationInitService;

	@Autowired
	ILocationHierarchyService locService;

	@Autowired
	protected IGPSExtensionService gpsExtensionService;

	public static void main(String[] args)
			throws AppInitializationException, DSPException, com.vividsolutions.jts.io.ParseException, ObjectNotFoundException {

		RainfallTask task = AppContext.getApplicationContext().getBean(RainfallTask.class);
		task.start();
	}

	public void start() throws DSPException, AppInitializationException, com.vividsolutions.jts.io.ParseException, ObjectNotFoundException {
		applicationInitService.initialize();
		Map<String, List<String>> stateToRainfallStationUUID = locationHierarchyService
				.getAllLocForParentChildTypes(LocationConstants.STATE, LocationConstants.RAINFALL);
		Map<String, Geometry> locUUIDtoGeometry = getGPSMappingForLocUUIDs();

		try {
			fw = new FileWriter("/home/srikanth/Desktop/RFData.csv");
			bw = new BufferedWriter(fw);

			fw.write("location_id,name,lat,long,day1,day2,day3,day4,day5,day6,day7,day8,day9,day10,"
					+ "day11,day12,day13,day14,day15,day16,day17,day18,day19,day20,day21,day22,day23,day24,day25,day26,day27,day28,day29,day30,day31\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
	
		Connection con = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			con = DriverManager.getConnection("jdbc:mysql://localhost/business_data", "root", "root");
		}
		catch(Exception e){
			
		}
		for (String state : stateToRainfallStationUUID.keySet()) {

			List<String> stationUUIDs = stateToRainfallStationUUID.get(state);

			for (String stationUUID : stationUUIDs) {

				long stationID = locationHierarchyService.getLocIdForLocUUID(stationUUID);
				String locName = locationHierarchyService.getLocationNameFromUUID(stationUUID);
				Geometry obj = locUUIDtoGeometry.get(stationUUID);
				
				String result = getRFDataFromDB(stationUUID, con);
				
				if(result.length()>0){
					try {
						fw.write(stationID + "," + locName + "," + obj.getCoordinate().x + "," + obj.getCoordinate().y + "," +result+"\n");
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				else{
					System.out.println("Data not found for locUUID " + stationUUID);
				}

			}

			try{
				fw.flush();
				bw.close();
				fw.close();
			}
			catch(Exception e){}
			
			
			}
		try{
			con.close();
		}
		catch(Exception e){
			
		}
	}

	public String getRFDataFromDB(String locationUUID, Connection con) {
		String result = "";
		
		try {

			Statement stmt = con.createStatement();
			ResultSet rs = stmt
					.executeQuery("select daily_data_values_1 from water_quantity_aggr_rf where location_uuid='"
							+ locationUUID + "' and calendar_year_month>=201705 and calendar_year_month<=201706");
			if (rs.next()) {
				return rs.getString(1);

			}
		} catch (Exception e) {
			System.out.println("error = " + e.getMessage());
		}
		
		return result;
	}

	protected Map<String, Geometry> getGPSMappingForLocUUIDs()
			throws DSPException, com.vividsolutions.jts.io.ParseException {

		try {
			applicationInitService.initialize();
		} catch (AppInitializationException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		Map<String, List<String>> allSMLocMap = locService.getAllLocForParentChildTypes(LocationConstants.STATE,
				LocationConstants.RAINFALL);
		List<String> allSMLocUUIDList = allSMLocMap.get(
				locService.getLocUUIdForLocID(1)); /** For Andhra Pradesh **/

		Map<Integer, String> entityLocationMap = null;

		try {
			entityLocationMap = locService.getEntityIdsForLocations(allSMLocUUIDList);

		} catch (DSPException | ObjectNotFoundException e) {
			e.printStackTrace();
		}

		List<Integer> entityList = new ArrayList<>();
		entityList.addAll(entityLocationMap.keySet());
		Map<Integer, Geometry> entityGPSMap = null;

		try {
			entityGPSMap = gpsExtensionService.getGPSData(entityList);
		} catch (ObjectNotFoundException | ParseException | DSPException e) {
			e.printStackTrace();
		}

		Map<String, Geometry> locationGPSMap = new HashMap<>();

		for (Integer entityId : entityList) {
			String locationUUID = entityLocationMap.get(entityId);
			Geometry gps = entityGPSMap.get(entityId);
			if (gps == null) {
				System.out.println("ZBXXX : No GPS For entity = " + entityId);
				continue;
			}
			locationGPSMap.put(locationUUID, gps);
		}

		Map<String, Geometry> locUUIDToGPSMap = new HashMap<>();

		for (String locUUID : allSMLocUUIDList) {
			Geometry gps = locationGPSMap.get(locUUID);
			if (gps != null)
				locUUIDToGPSMap.put(locUUID, gps);
		}
		return locUUIDToGPSMap;
	}
}
