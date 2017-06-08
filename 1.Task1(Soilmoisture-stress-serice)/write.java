@Override
	public Map<String, IExtendedRainfallForecastData> getMultiSourcedRainfallForecast(int modelDate,
			List<String> rfLocUUID) {

		/*
		 * 1. Gets all weatherForecastData at rainfall staion level from ISRO.
		 * 2. Gets {@link IRainfallForecast} data from APSDPS 
		 * 3. Prepare {@link ICSRFForecastData} for crop stress computation Use rf forecast value
		 *    from {@link IWeatherForecast} if available for a rfLocUUID and
		 *    rfForecastDate else use {@link IRainfallForecast} else return NO_DATA
		 */

		Map<String, IExtendedRainfallForecastData> rainfallForecastDataMap = new HashMap<String, IExtendedRainfallForecastData>();
		List<Integer> modelDatesList = DateUtils.addNDaysFromModelDate(modelDate,
				IWMConstants.SM_RF_DEFAULT_ISRO_FORECAST_DAYS);

		/*
		 * 1. Get rainfall forecast data from Actual Rainfall Data
		 * 2. Get rainfall forecast data from ISRO/WeatherForecast service
		 * 3. Get rainfall forecast data from APSDPS
		 */
		Map<String, Map<Integer, IWeatherForecast>> rfStationWeatherForecastData = null;
		Map<String, IRainfallForecast> apsdpsRainfallForecastData = null;

		try {

			apsdpsRainfallForecastData = rfForecastService.getForecastDataForStress(modelDate);
			rfStationWeatherForecastData = weatherForecastService.getNDaysRFStationLevelForecast(
					DateUtils.getModelDateInMillis(modelDate), IWMConstants.SM_RF_DEFAULT_ISRO_FORECAST_DAYS);

		} catch (DSPException e) {
			// e.printStackTrace();
			System.out.println("SMStressService : getMultiSourcedRainfallForecast - Exception "
					+ "encountered while fetching rainfall forecast data from APSDPS or ISRO, " + e.getMessage());
		} catch (ParseException e) {

		} catch (ObjectNotFoundException ex) {

		}

		/*
		 * If data is not present from both the sources, then return empty map
		 */
		if (rfStationWeatherForecastData == null && apsdpsRainfallForecastData == null) {
			if (IS_DEBUG_ENABLED) {
				System.out.println("SMStressService : getMultiSourcedRainfallForecast - "
						+ " no forecast is available for date = " + modelDate);
			}
			return rainfallForecastDataMap;
		}

		/*
		 * Else if there is data from any one of the sources, make the other as
		 * empty instead of null to avoid null pointer exceptions during
		 * computation in the following for-loop.
		 */
		if (rfStationWeatherForecastData == null) {
			rfStationWeatherForecastData = new HashMap<>();
		}
		if (apsdpsRainfallForecastData == null) {
			apsdpsRainfallForecastData = new HashMap<>();
		}

		for (String locUUID : rfLocUUID) {
			Map<Integer, IWeatherForecast> weatherForecastMap = rfStationWeatherForecastData.get(locUUID);
			IRainfallForecast rfForecast = apsdpsRainfallForecastData.get(locUUID);
			boolean isRFForecastAvailable = true;
			boolean isWForecastAvailable = true;

			if (weatherForecastMap == null || weatherForecastMap.isEmpty()) {
				isWForecastAvailable = false;
			}

			if (rfForecast == null) {
				isRFForecastAvailable = false;
				/*
				 * If both the sources doesn't has the forecast data, then
				 * continue
				 */
				if (!isWForecastAvailable) {
					if (IS_DEBUG_ENABLED) {
						System.out.println("SMStressService : getMultiSourcedRainfallForecast - "
								+ "Forecast not available for - " + locUUID);
					}
					continue;
				}
			}

			IExtendedRainfallForecastData commonForecastData = new ExtendedRainfallForecastData();
			commonForecastData.setLocationUUID(locUUID);
			commonForecastData.setForecastDay(modelDate);

			/*
			 * 1. APSDPS has rainfall forecast starting from today 2. ISRO
			 * Weather Forecast has data starting from tomorrow 3. Adjust
			 * accordingly
			 * 
			 * Use Current rainfall explicitly for Day-1 for today's forecast,
			 * if APSDPS is not needed.
			 */

			if (isRFForecastAvailable && rfForecast.getDay1() >= 0) {
				commonForecastData.setDay1(rfForecast.getDay1());
				commonForecastData.setDataSourceDay1(DataSourceConstants.APSDPS);
			} else {
				commonForecastData.setDay1(DSPConstants.NO_DATA);
				commonForecastData.setDataSourceDay1(DataSourceConstants.NO_SOURCE);
			}

			/*
			 * Day 2nd to Day 7th
			 */
			if (isWForecastAvailable && modelDatesList.size() > 0
					&& weatherForecastMap.containsKey(modelDatesList.get(0))
					&& weatherForecastMap.get(modelDatesList.get(0)).getRf() >= 0) {
				commonForecastData.setDay2(weatherForecastMap.get(modelDatesList.get(0)).getRf());
				commonForecastData.setDataSourceDay2(DataSourceConstants.ISRO);
			} else {
				if (isRFForecastAvailable && rfForecast.getDay2() >= 0) {
					commonForecastData.setDay2(rfForecast.getDay2());
					commonForecastData.setDataSourceDay2(DataSourceConstants.APSDPS);
				} else {
					commonForecastData.setDay2(DSPConstants.NO_DATA);
					commonForecastData.setDataSourceDay2(DataSourceConstants.NO_SOURCE);
				}
			}

			if (isWForecastAvailable && modelDatesList.size() > 1
					&& weatherForecastMap.containsKey(modelDatesList.get(1))
					&& weatherForecastMap.get(modelDatesList.get(1)).getRf() >= 0) {
				commonForecastData.setDay3(weatherForecastMap.get(modelDatesList.get(1)).getRf());
				commonForecastData.setDataSourceDay3(DataSourceConstants.ISRO);
			} else {
				if (isRFForecastAvailable && rfForecast.getDay3() >= 0) {
					commonForecastData.setDay3(rfForecast.getDay3());
					commonForecastData.setDataSourceDay3(DataSourceConstants.APSDPS);
				} else {
					commonForecastData.setDay3(DSPConstants.NO_DATA);
					commonForecastData.setDataSourceDay3(DataSourceConstants.NO_SOURCE);
				}
			}

			if (isWForecastAvailable && modelDatesList.size() > 2
					&& weatherForecastMap.containsKey(modelDatesList.get(2))
					&& weatherForecastMap.get(modelDatesList.get(2)).getRf() >= 0) {
				commonForecastData.setDay4(weatherForecastMap.get(modelDatesList.get(2)).getRf());
				commonForecastData.setDataSourceDay4(DataSourceConstants.ISRO);
			} else {
				if (isRFForecastAvailable && rfForecast.getDay4() >= 0) {
					commonForecastData.setDay4(rfForecast.getDay4());
					commonForecastData.setDataSourceDay4(DataSourceConstants.APSDPS);
				} else {
					commonForecastData.setDay4(DSPConstants.NO_DATA);
					commonForecastData.setDataSourceDay4(DataSourceConstants.NO_SOURCE);
				}
			}

			if (isWForecastAvailable && modelDatesList.size() > 3
					&& weatherForecastMap.containsKey(modelDatesList.get(3))
					&& weatherForecastMap.get(modelDatesList.get(3)).getRf() >= 0) {
				commonForecastData.setDay5(weatherForecastMap.get(modelDatesList.get(3)).getRf());
				commonForecastData.setDataSourceDay5(DataSourceConstants.ISRO);
			} else {
				if (isRFForecastAvailable && rfForecast.getDay5() >= 0) {
					commonForecastData.setDay5(rfForecast.getDay5());
					commonForecastData.setDataSourceDay5(DataSourceConstants.APSDPS);
				} else {
					commonForecastData.setDay5(DSPConstants.NO_DATA);
					commonForecastData.setDataSourceDay5(DataSourceConstants.NO_SOURCE);
				}
			}

			if (isWForecastAvailable && modelDatesList.size() > 4
					&& weatherForecastMap.containsKey(modelDatesList.get(4))
					&& weatherForecastMap.get(modelDatesList.get(4)).getRf() >= 0) {
				commonForecastData.setDay6(weatherForecastMap.get(modelDatesList.get(4)).getRf());
				commonForecastData.setDataSourceDay6(DataSourceConstants.ISRO);
			} else {
				if (isRFForecastAvailable && rfForecast.getDay6() >= 0) {
					commonForecastData.setDay6(rfForecast.getDay6());
					commonForecastData.setDataSourceDay6(DataSourceConstants.APSDPS);
				} else {
					commonForecastData.setDay6(DSPConstants.NO_DATA);
					commonForecastData.setDataSourceDay6(DataSourceConstants.NO_SOURCE);
				}
			}

			if (isWForecastAvailable && modelDatesList.size() > 5
					&& weatherForecastMap.containsKey(modelDatesList.get(5))
					&& weatherForecastMap.get(modelDatesList.get(5)).getRf() >= 0) {
				commonForecastData.setDay7(weatherForecastMap.get(modelDatesList.get(5)).getRf());
				commonForecastData.setDataSourceDay7(DataSourceConstants.ISRO);
			} else {
				if (isRFForecastAvailable && rfForecast.getDay7() >= 0) {
					commonForecastData.setDay7(rfForecast.getDay7());
					commonForecastData.setDataSourceDay7(DataSourceConstants.APSDPS);
				} else {
					commonForecastData.setDay7(DSPConstants.NO_DATA);
					commonForecastData.setDataSourceDay7(DataSourceConstants.NO_SOURCE);
				}
			}

			rainfallForecastDataMap.put(locUUID, commonForecastData);
		}

		return rainfallForecastDataMap;
	}