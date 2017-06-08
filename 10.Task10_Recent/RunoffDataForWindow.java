package com.vassarlabs.test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.vassarlabs.common.dsp.err.DSPException;
import com.vassarlabs.common.init.err.AppInitializationException;
import com.vassarlabs.common.init.service.api.IApplicationInitService;
import com.vassarlabs.common.utils.err.ObjectNotFoundException;
import com.vassarlabs.config.spring.AppContext;
import com.vassarlabs.iwm.soilmoisture.stress.pojo.api.ISoilMoistureNRSCData;
import com.vassarlabs.iwm.soilmoisture.stress.pojo.api.ISoilMoistureNRSCGridData;
import com.vassarlabs.iwm.soilmoisture.stress.pojo.impl.SoilMoistureNRSCData;
import com.vassarlabs.iwm.soilmoisture.stress.service.api.ISoilMoistureNRSCDataService;
import com.vassarlabs.location.service.api.ILocationHierarchyService;

@Component
public class RunoffDataForWindow {

	public static BufferedWriter bw = null;
	public static FileWriter fw = null;
	
	@Autowired
	ISoilMoistureNRSCDataService nrscDataService;

	@Autowired
    ISoilMoistureNRSCDataService  nrscGridData;
	
	@Autowired
	ILocationHierarchyService locationHierarchyService;

	@Autowired
	@Qualifier("RESTInitServiceImpl")
	protected IApplicationInitService applicationInitService;

	public static void main(String[] args) throws AppInitializationException, DSPException,
			com.vividsolutions.jts.io.ParseException, ObjectNotFoundException, IOException {

		RunoffDataForWindow task = AppContext.getApplicationContext().getBean(RunoffDataForWindow.class);
		int startDate = 20170507;
		int endDate = 20170610;
		task.start(startDate, endDate);
	}

	public void start(int startDate, int endDate) throws DSPException, IOException{
		try {
			applicationInitService.initialize();
		} catch (AppInitializationException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		List<ISoilMoistureNRSCGridData> smNRSCGridDataList = nrscGridData.getAllNRSCGridData(); 
		Map<Long, String> gridToLatLongMap = new HashMap<>();
		for(ISoilMoistureNRSCGridData smNRSCGridData : smNRSCGridDataList){
			long gridID = smNRSCGridData.getGridId();
			double lat = smNRSCGridData.getLatitude();
			double log = smNRSCGridData.getLongitude();
			
			gridToLatLongMap.put(gridID, Double.toString(lat)+","+Double.toString(log));
		}
		
		Map<Long, Double> gridToRunoffMap = new HashMap<>();
		while(startDate <= endDate){
			fw = new FileWriter("/home/srikanth/Desktop/RFData/"+startDate +".csv");
			bw = new BufferedWriter(fw);

			fw.write("LAT,LONG,VALUE\n");
			int date = startDate - 2;
			
			while(date<=startDate){
				List<ISoilMoistureNRSCData> smDataMap = nrscGridData.getNRSCData(date);
				for(int i=0; i<smDataMap.size(); i++){
					SoilMoistureNRSCData nrscData = (SoilMoistureNRSCData) smDataMap.get(i);
					
					long gridID = nrscData.getGridId();
					double runOff = nrscData.getRunoff();
					if(gridToRunoffMap.containsKey(gridID)){
						gridToRunoffMap.put(gridID, gridToRunoffMap.get(gridID)+runOff);
					}
					else{
						gridToRunoffMap.put(gridID, runOff);
					}
				}
				date++;
			}
			
			for(long gridID : gridToRunoffMap.keySet()){
				fw.write(gridToLatLongMap.get(gridID)+","+gridToRunoffMap.get(gridID)+"\n");
				System.out.println(gridToLatLongMap.get(gridID)+","+gridToRunoffMap.get(gridID));
			}
			startDate += 1;
			try{
				fw.flush();
				bw.close();
				fw.close();
			}
			catch(Exception e){}
		}
	}
}
