package com.vassarlabs.iwm.soilmoisture.cropstress.handler.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.vassarlabs.common.dsp.err.DSPException;
import com.vassarlabs.common.dsp.err.ObjectCreationException;
import com.vassarlabs.common.utils.DateUtils;
import com.vassarlabs.common.utils.err.ObjectNotFoundException;
import com.vassarlabs.eventmapper.err.EventHandlingException;
import com.vassarlabs.iwm.cleanup.dsp.api.ICleanUpDSP;
import com.vassarlabs.iwm.cleanup.handlers.impl.ACleanUpEventHandler;
import com.vassarlabs.iwm.cleanup.pojo.api.ICleanUpEventObject;
import com.vassarlabs.iwm.cleanup.utils.api.CleanUpConstants;
import com.vassarlabs.iwm.soilmoisture.cropstress.service.api.ICropStressDataService;

/**
 * CSForecastCleanUpEventHandler is called from {@link CSCleanUpEventHandler} class. 
 * This class handles Database table clean up logic for business_data.crop_stress_forecast_data table. 
 * Retains records having model_date >= (Last known Crop Stress Propagated model date - 1). 
 * And moves remaining data to business_data.crop_stress_forecast_data_backup table using 
 * last known model date of propagated table, because crop stress computation ends with propagated table.
 * 
 * @author arunima
 */
@Component
public class CSForecastCleanUpEventHandler extends ACleanUpEventHandler {

	@Autowired
	ICleanUpDSP cleanUpDSPImpl;

	@Autowired
	ICropStressDataService cropStressDataService;
	@Override
	public Object handleEvent(Object event)
			throws EventHandlingException, ObjectCreationException, ObjectNotFoundException {

		long startTs = System.currentTimeMillis();
		System.out.println("CSForecastCleanUpEventHandler - handleEvent : started cleanup at " + startTs);

		try {
			ICleanUpEventObject cleanUpEvent = (ICleanUpEventObject) event;
			int modelDate = cleanUpEvent.getModelDate();
			int CS_HISTORY_WINDOW_SIZE = 2;
			
			if (cleanUpEvent.getModelDate() <= 0) {
				int lastKnownModelDate = cropStressDataService.getLastKnownDateForCSPropData(DateUtils.getModelDateFromTs(System.currentTimeMillis()));
				String cleanUpModelDate = DateUtils.getNDayInFormat(CleanUpConstants.MODEL_DATE_FORMAT, String.valueOf(lastKnownModelDate), -CS_HISTORY_WINDOW_SIZE);
				modelDate = Integer.parseInt(cleanUpModelDate);
			}
			
			cleanUpDSPImpl.cleanUpTableData(CleanUpConstants.CROP_STRESS_FORECAST_DATA_TABLE,
					CleanUpConstants.CROP_STRESS_FORECAST_DATA_BACKUP_TABLE, modelDate, CS_HISTORY_WINDOW_SIZE);
			System.out.println("CSForecastCleanUpEventHandler - total time taken = "
					+ (System.currentTimeMillis() - startTs) / 1000);

		} catch (DSPException e) {
			throw new EventHandlingException("Error while moving data to backup table, inside CSForecastCleanUpEventHandler ", e);
		}
		return null;
	}
}
