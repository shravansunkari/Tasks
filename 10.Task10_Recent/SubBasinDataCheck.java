package com.vassarlabs.test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.vassarlabs.common.dsp.err.DSPException;
import com.vassarlabs.common.dsp.utils.DSPConstants;
import com.vassarlabs.common.init.err.AppInitializationException;
import com.vassarlabs.common.init.service.api.IApplicationInitService;
import com.vassarlabs.common.utils.err.ObjectNotFoundException;
import com.vassarlabs.config.spring.AppContext;
import com.vassarlabs.iwm.soilmoisture.stress.service.api.ISoilMoistureNRSCDataService;
import com.vassarlabs.location.service.api.ILocationHierarchyService;
import com.vassarlabs.location.utils.LocationConstants;

@Component
public class SubBasinDataCheck {

	public static BufferedWriter bw = null;
	public static FileWriter fw = null;
	public static BufferedWriter bwLog = null;
	public static FileWriter fwLog = null;

	@Autowired
	ISoilMoistureNRSCDataService nrscDataService;

	@Autowired
	ILocationHierarchyService locationHierarchyService;

	@Autowired
	@Qualifier("RESTInitServiceImpl")
	protected IApplicationInitService applicationInitService;

	public static void main(String[] args) throws AppInitializationException, DSPException,
			com.vividsolutions.jts.io.ParseException, ObjectNotFoundException, IOException {

		SubBasinDataCheck task = AppContext.getApplicationContext().getBean(SubBasinDataCheck.class);
		task.checkSubBasin("");
	}

	public void checkSubBasin(String subBasinName) throws DSPException, ObjectNotFoundException, IOException {
		try {
			applicationInitService.initialize();
		} catch (AppInitializationException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			fw = new FileWriter("/home/srikanth/Desktop/data.csv");
			bw = new BufferedWriter(fw);

		} catch (IOException e) {
			e.printStackTrace();
		}
		int[] dates = new int[] { 20170401, 20170402, 20170403, 20170404, 20170405, 20170406, 20170407, 20170408,
				20170409, 201704010, 201704011, 201704012, 201704013, 201704014, 201704015, 201704016, 201704017,
				201704018, 201704019 };
		subBasinName = "TADAKALERU-SB";
		
		fw.write(subBasinName + ",day1,day2,day3,day4,day5,day6,day7,day8,day9,day10,"
					+ "day11,day12,day13,day14,day15,day16,day17,day18,day19\n");
		
		Map<String, List<String>> subBasinToMicroBasinMap = locationHierarchyService
				.getAllLocForParentChildTypes(LocationConstants.SUBBASIN, LocationConstants.MICROBASIN);

		for (String subBasinUUID : subBasinToMicroBasinMap.keySet()) {
			String subBasinName1 = locationHierarchyService.getLocationNameFromUUID(subBasinUUID);
			if (subBasinName.equals(subBasinName1)) {
				List<String> microBasinUnderSubBasin = subBasinToMicroBasinMap.get(subBasinUUID);
				for (int i = 0; i < dates.length; i++) {

					Map<String, Map<Integer, Double>> microBasinTorunOffDataMap = nrscDataService
							.getMicroBasinLevelRunOffData(dates[i], dates[i], null);
					double runOff = 0;
					for (String microBasin : microBasinUnderSubBasin) {
						Map<Integer, Double> microBasinRunOffData = microBasinTorunOffDataMap.get(microBasin);

						if (microBasinRunOffData.get(dates[i]) > 0
								&& microBasinRunOffData.get(dates[i]) != DSPConstants.NO_DATA) {
							runOff += microBasinRunOffData.get(dates[i]);
						}
					}
					if (runOff > 0) {

						fw.write(runOff + ",");

					} else {

						fw.write(DSPConstants.NO_DATA + ",");

					}
				}
				// microbasin,date,runoff
			}
		}
		
		fw.write("\n");
		
		try {
			fw.flush();
			bw.close();
			fw.close();
		} catch (Exception e) {
		}
	}
}
