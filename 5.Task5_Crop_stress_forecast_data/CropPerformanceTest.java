package com.vassarlabs.test;

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
import com.vassarlabs.common.utils.DateUtils;
import com.vassarlabs.common.utils.err.ObjectNotFoundException;
import com.vassarlabs.config.spring.AppContext;
import com.vassarlabs.iwm.soilmoisture.cropstress.pojo.api.ICropGrowthStageMD;
import com.vassarlabs.iwm.soilmoisture.cropstress.pojo.api.ICropStressForecastData;
import com.vassarlabs.iwm.soilmoisture.cropstress.service.api.ICropGrowthStageService;
import com.vassarlabs.iwm.soilmoisture.cropstress.service.api.ICropStressDataService;
import com.vassarlabs.iwm.soilmoisture.cropstress.service.api.ICropStressLocMapService;
import com.vassarlabs.iwm.soilmoisture.stress.service.api.ISoilMoistureStressService;

@Component
public class CropPerformanceTest {

	@Autowired
	@Qualifier("RESTInitServiceImpl")
	protected IApplicationInitService applicationInitService;

	@Autowired
	ICropStressLocMapService cropStressLocMapService;
	@Autowired
	ICropStressDataService cropStressDataService;
	@Autowired
	protected ICropGrowthStageService cropGrowthStageService;
	@Autowired
	protected ISoilMoistureStressService soilMoistureStressService;

	private void test() throws AppInitializationException, IOException {
		applicationInitService.initialize();
		long currMillis = System.currentTimeMillis();
		int modelDate = DateUtils.getModelDateFromTs(currMillis);

		try {
			cropStressDataService.computeVillageCropStressData(modelDate);
		} catch (DSPException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ObjectNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(
				"Time taken to compute Forecast Data " + (System.currentTimeMillis() - currMillis) / 1000 + " secs");

		currMillis = System.currentTimeMillis();
		modelDate = DateUtils.getModelDateFromTs(currMillis);
		List<String> villageList = null;
		System.out.println("Start " + currMillis);
		try {
			villageList = cropStressLocMapService.getAllVillagesFullName();
		} catch (DSPException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		Map<String, Map<String, List<ICropStressForecastData>>> cropStressForecastData = new HashMap<String, Map<String, List<ICropStressForecastData>>>();

		try {
			cropStressForecastData = cropStressDataService.getVillageCropStressData(modelDate, villageList, null);
			System.out.println("cropStressForecastData.size() " + cropStressForecastData.size());
		} catch (DSPException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("crop get service " + (System.currentTimeMillis() - currMillis) / 1000 + " secs");

	}

	private void test1() throws AppInitializationException, IOException {
		applicationInitService.initialize();
		
		long currMillis = System.currentTimeMillis();
		int modelDate = DateUtils.getModelDateFromTs(currMillis);
		
		try {
			Map<Integer, ICropGrowthStageMD> cropGrowthStageMDMap = cropGrowthStageService.getAllCropGrowthStageMD();
			List<ICropStressForecastData> cropData = cropStressDataService
					.getLastKnownFarmCropStressDataForVillage(modelDate, 28469, "PADDY", cropGrowthStageMDMap);

			System.out.println("cropData.size() = " + cropData.size());
		} catch (DSPException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) throws AppInitializationException, IOException {
		System.out.println("Getting crop_stress data for all the farms under a village...");
		
		CropPerformanceTest cpTest = AppContext.getApplicationContext().getBean(CropPerformanceTest.class);
		// cpTest.test();
		double t1 = System.currentTimeMillis();
		cpTest.test1();
		double t2 = System.currentTimeMillis();
		System.out.println("\nDone fetching data...\nTime took " + (t2-t1)/1000.0);
	}

}
