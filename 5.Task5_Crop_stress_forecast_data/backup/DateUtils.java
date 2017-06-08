package com.vassarlabs.common.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author zubair
 *
 */
public class DateUtils {

	public static final int WEEK_IN_SECONDS = 7*24*60*60*1000;
	public static final int TWENTY_FOUR_HOURS_IN_SECONDS = 24*60*60*1000;
	public static final int ONE_MINUTE = 1 * 60 * 1000;
	public static final int ONE_HOUR_IN_MS = 1 * 60 * 60 * 1000;


	public static Map<String, TimePeriodMonth> getMainDashboardRFTimes3(long referenceTimeMillis){
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
		Map<String, TimePeriodMonth> times = new LinkedHashMap<String, TimePeriodMonth>();
		

		TimePeriodMonth currentMonth =  getCurrentTimePeriodMonth(referenceTimeMillis);
		currentMonth.setMonthStart(getMonthStartTs(referenceTimeMillis));
		currentMonth.setMonthEnd(referenceTimeMillis);
		currentMonth.setNameOfMonthYear("This Month Upto "+dateFormat.format(referenceTimeMillis)	);
	
		referenceTimeMillis = currentMonth.getMonthStart()-2;
		TimePeriodMonth previousMonth =  getCurrentTimePeriodMonth(referenceTimeMillis);
		
		referenceTimeMillis = previousMonth.getMonthStart()-2;
		TimePeriodMonth previousToPreviousMonth =  getCurrentTimePeriodMonth(referenceTimeMillis);
		
		times.put("month0", previousToPreviousMonth);
		times.put("month1", previousMonth);
		times.put("month2", currentMonth);

		TimePeriodMonth accumulatedFromJune = DateUtils.getLastSeasonMonthTimes2(System.currentTimeMillis(), 5);
		accumulatedFromJune.setMonthEnd(System.currentTimeMillis());
		System.out.println("accumulatedFromJune.getMonthStart():"+accumulatedFromJune.getMonthStart());
		accumulatedFromJune.setNameOfMonthYear("accumulated");
		times.put("accumulated", accumulatedFromJune);
		
	
		System.out.println("times in getMainDashboardRFTimes2:"+times); 
		return times;
	}
	
	public static String getTimeStringFromTimeInMillis(long referenceTimeMillis){
		//TODO: remove hard coding for date format
		if(referenceTimeMillis < 1)return "NA";
		
		//TODO: adding 5:30 to time remove hard coding
		//referenceTimeMillis += 19800000;
		String dateFormat = "dd/MM/yyyy HH:mm:ss";
		Date referenceDate = new Date(referenceTimeMillis);
		SimpleDateFormat referenceTimeFormat = new SimpleDateFormat(dateFormat);
		String referenceTime = referenceTimeFormat.format(referenceDate);
		return referenceTime;
	}
	
	public static long getStartOfDay(long referenceTimeMillis){
		
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(referenceTimeMillis);
		//cal.set(Calendar.DATE,-1);  //previous day
		 cal.set(Calendar.HOUR_OF_DAY, 0);            // set hour to midnight
		  cal.set(Calendar.MINUTE, 0);                 // set minute in hour
		  cal.set(Calendar.SECOND, 0);                 // set second in minute
		  cal.set(Calendar.MILLISECOND, 0);   
		return cal.getTimeInMillis();
	}

	/**
	 * Get end of the day for input reference ts
	 * 
	 * @param referenceTs
	 * @return
	 */
	public static long getEndOfDay(long referenceTs) {
		
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(getStartOfDay(referenceTs));
		cal.add(Calendar.HOUR, 24);
		cal.add(Calendar.MILLISECOND, -1);
		return cal.getTimeInMillis();
	}
	public static long getIntervalOfDay(long referenceTimeMillis, int hours, int minutes, int seconds){
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(referenceTimeMillis);
		 cal.set(Calendar.HOUR_OF_DAY, hours);            // set hour to midnight
		  cal.set(Calendar.MINUTE, minutes);                 // set minute in hour
		  cal.set(Calendar.SECOND, seconds);                 // set second in minute
		  cal.set(Calendar.MILLISECOND, 0); 
		return cal.getTimeInMillis();
	}
	
	public static long getIntervalOfNDay(long referenceTimeMillis,int n,int hours,int minutes,int seconds){
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(referenceTimeMillis);
		 cal.set(Calendar.HOUR_OF_DAY, hours);            // set hour to midnight
		  cal.set(Calendar.MINUTE, minutes);                 // set minute in hour
		  cal.set(Calendar.SECOND, seconds);                 // set second in minute
		  cal.set(Calendar.MILLISECOND, 0); 
		return cal.getTimeInMillis();
	}
	
	
	public static long getStartOfPreviousDay(long referenceTimeMillis){
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(referenceTimeMillis);
		
		cal.set(Calendar.HOUR_OF_DAY, -24);             // set hour to midnight
		  cal.set(Calendar.MINUTE, 0);                 // set minute in hour
		  cal.set(Calendar.SECOND, 0);                 // set second in minute
		  cal.set(Calendar.MILLISECOND, 0);   
		return cal.getTimeInMillis();
	}
	
	public static long getStartOfTomorrow(long referenceTimeMillis){
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(referenceTimeMillis);
		
		cal.set(Calendar.HOUR_OF_DAY, 24);             // set hour to midnight
		  cal.set(Calendar.MINUTE, 0);                 // set minute in hour
		  cal.set(Calendar.SECOND, 0);                 // set second in minute
		  cal.set(Calendar.MILLISECOND, 0);   
		return cal.getTimeInMillis();
	}
	
	public static long getLastIntervalOfGivenTime(long referenceTimeMillis,int hours,int minutes,int seconds){
		long startOfday = DateUtils.getStartOfDay(referenceTimeMillis);
		long updateInterval = DateUtils.getIntervalOfDay(startOfday, hours, minutes, seconds);

		//this will happen when time is between 12:00 to 8:00 am 
		if(updateInterval > referenceTimeMillis){
			startOfday = DateUtils.getStartOfPreviousDay(referenceTimeMillis);
			updateInterval = DateUtils.getIntervalOfDay(startOfday, hours, minutes, seconds);
		}
		return updateInterval;
	}
	

	public static long getStartOfYear(int year){
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.YEAR, year); 
		cal.set(Calendar.DAY_OF_YEAR, 1);
		cal.set(Calendar.HOUR_OF_DAY, 1);
		cal.set(Calendar.HOUR, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		return cal.getTimeInMillis();
	}
	
	public static long getEndOfYear(int year){
		Calendar cal = Calendar.getInstance();
		cal.set(Calendar.YEAR, year); 
		cal.set(Calendar.MONTH, 11);
		cal.set(Calendar.DATE, 31);
		cal.set(Calendar.HOUR_OF_DAY, 24);
		cal.set(Calendar.HOUR, 11);
		cal.set(Calendar.MINUTE, 59);
		cal.set(Calendar.SECOND, 59);
		//cal.add(Calendar.SECOND, -1);
		return cal.getTimeInMillis();
	}

	public static long getPreviousYearStartTs(long referenceTimeMillis) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(referenceTimeMillis);
		cal.set(Calendar.MONTH, 0);
		cal.set(Calendar.DATE, 1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		cal.add(Calendar.YEAR, -1);
		
		return cal.getTimeInMillis();
	}
	
	public static long getPreviousYearEndTs(long referenceTimeMillis) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(referenceTimeMillis);
		cal.set(Calendar.MONTH, 0);
		cal.set(Calendar.DATE, 1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		cal.add(Calendar.MILLISECOND, -1);
		
		return cal.getTimeInMillis();
	}
	
	public static long getLastYearSameTimeTs(long referenceTimeMillis) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(referenceTimeMillis);
		cal.add(Calendar.YEAR, -1);
		
		return cal.getTimeInMillis();
	}
	
	public static long getYesterdayStartTs(long referenceTimeMillis) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(referenceTimeMillis);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		cal.add(Calendar.DATE, -1);
		
		return cal.getTimeInMillis();
	}
	
	public static long getYesterdayEndTs(long referenceTimeMillis) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(referenceTimeMillis);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		cal.add(Calendar.MILLISECOND, -1);
		
		return cal.getTimeInMillis();
	}
	
	public static long getYesterdaySameTimeTs(long referenceTimeMillis) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(referenceTimeMillis);
		cal.add(Calendar.DATE, -1);
		
		return cal.getTimeInMillis();
	}
	
	public static long getPreviousMonthStartTs(long referenceTimeMillis) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(referenceTimeMillis);
		cal.set(Calendar.DATE, 1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		cal.add(Calendar.MONTH, -1);
		
		return cal.getTimeInMillis();
	}
	
	
	//Given noOfMonths method gives time stamp of given months back
	public static long getPreviousMonthsStartTs(long referenceTimeMillis, int noOfMonths){	
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(referenceTimeMillis);
		cal.set(Calendar.DATE, 1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		cal.add(Calendar.MONTH, -noOfMonths);
		
		return cal.getTimeInMillis();		
	}
	
	
	public static long getPreviousMonthEndTs(long referenceTimeMillis) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(referenceTimeMillis);
		cal.set(Calendar.DATE, 1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		cal.add(Calendar.MILLISECOND, -1);
		
		return cal.getTimeInMillis();
	}
	
	public static long getMonthStartTs(long referenceTimeMillis) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(referenceTimeMillis);
		cal.set(Calendar.DATE, 1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		//cal.add(Calendar.MONTH, -1);
		
		return cal.getTimeInMillis();
	}
	
	public static long getMonthEndTs(long referenceTimeMillis) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(referenceTimeMillis);
		cal.set(Calendar.DATE, 1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		cal.add(Calendar.MONTH, +1);
		cal.add(Calendar.MILLISECOND, -1);
		
		return cal.getTimeInMillis();
	}
	
	public static TimePeriodMonth	getTimePeriodForMonth(int month){
		TimePeriodMonth timePeriodMonth = new TimePeriodMonth();
		
		Calendar cal = Calendar.getInstance();
		
		
		cal.set(Calendar.MONTH, month);
		cal.set(Calendar.DAY_OF_MONTH, 0); 
		cal.set(Calendar.HOUR_OF_DAY, 0); 
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.add(Calendar.MILLISECOND, 1);
		timePeriodMonth = getCurrentTimePeriodMonth(cal.getTimeInMillis());
		
		return timePeriodMonth;
		
	}
	
	public static long getPreviousYearSameTimeTs(long referenceTimeMillis) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(referenceTimeMillis);
		cal.add(Calendar.YEAR, -1);
		
		return cal.getTimeInMillis();
	}
	
	public static long getGivenYearSameTimeTs(long referenceTimeMillis ,int year){
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(referenceTimeMillis);
		cal.set(Calendar.YEAR, year);
		
		return cal.getTimeInMillis();
	}
	
	
	public static Map<String, Long> getLastYearSameMonthTimes(long referenceTimeMillis){
		Map<String, Long> times = new HashMap<String, Long>();
		long PreviouYearSameTime = getPreviousYearSameTimeTs(referenceTimeMillis);
		times.put("end", getMonthEndTs(PreviouYearSameTime));// ;
		times.put("start", getMonthStartTs(PreviouYearSameTime));//;
		
		return times;
	}
	
	public static TimePeriodMonth getLastYearSamDayTimes(long referenceTimeMillis){
		long previouYearSameTime = getPreviousYearSameTimeTs(referenceTimeMillis);
		
		TimePeriodMonth lastYearSamDayTimePeriodMonth = new TimePeriodMonth();
		lastYearSamDayTimePeriodMonth.setMonthStart(previouYearSameTime-TWENTY_FOUR_HOURS_IN_SECONDS);
		lastYearSamDayTimePeriodMonth.setMonthEnd(previouYearSameTime);
		
		return lastYearSamDayTimePeriodMonth;
	}
	
	public static TimePeriodMonth getGivenYearSamDayTimes(long referenceTimeMillis, int year){
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
		//long previouYearSameTime = getPreviousYearSameTimeTs(referenceTimeMillis);
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(referenceTimeMillis); 
		calendar.set(Calendar.YEAR, year);
		long givenYearTimeInMillis = calendar.getTimeInMillis();
		TimePeriodMonth lastYearSamDayTimePeriodMonth = new TimePeriodMonth();
		lastYearSamDayTimePeriodMonth.setMonthStart(givenYearTimeInMillis-TWENTY_FOUR_HOURS_IN_SECONDS);
		lastYearSamDayTimePeriodMonth.setMonthEnd(givenYearTimeInMillis);
		lastYearSamDayTimePeriodMonth.setNameOfMonthYear(	dateFormat.format(lastYearSamDayTimePeriodMonth.getMonthEnd()	));

		
		return lastYearSamDayTimePeriodMonth;
	}
	
	
	
	public static TimePeriodMonth getLastYearSameMonthTimes2(long referenceTimeMillis){
		//Calendar cal = Calendar.getInstance();
		//cal.add(Calendar.YEAR, -1);
		TimePeriodMonth times = new TimePeriodMonth();
		long previouYearSameTime = getPreviousYearSameTimeTs(referenceTimeMillis);
		times.setMonthEnd(getMonthEndTs(previouYearSameTime));//("end", );// ;
		times.setMonthStart(getMonthStartTs(previouYearSameTime));//("start", );//;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MMM-yy");
		
		times.setNameOfMonthYear(dateFormat.format(previouYearSameTime));
		return times;
	}
	
	public static long getLastMonthSameTimeTs(long referenceTimeMillis) {
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(referenceTimeMillis);
		cal.add(Calendar.MONTH, -1);
		
		return cal.getTimeInMillis();
	}
	

	public static List<TimePeriodMonth> getPriorTimePeriods(long referenceMilliSeconds, int nMonths){
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(referenceMilliSeconds);
		
		List<TimePeriodMonth> timePeriodMonths = new ArrayList<TimePeriodMonth>();
		TimePeriodMonth timePeriodMonth = null;
		for (int i = 0; i < nMonths; i++) {
			timePeriodMonth = new TimePeriodMonth();
			cal.add(Calendar.MONTH, -1); 
			timePeriodMonth = getLastYearSameMonthTimes2(cal.getTimeInMillis());
			timePeriodMonths.add(timePeriodMonth);
			
		}
		
		return timePeriodMonths;
	}
	
	/**
	 * 
	 * @param timePeriodMonth Does have start and end times. 
	 * @return List<TimePeriodMonth> Months seprated if input timePeriodMonth spans more than one month. Last TimePeriodMonth
	 * in this list does have MonthEnd as current time. 
	 */
	public static List<TimePeriodMonth> splitTimePeriodMonth(TimePeriodMonth timePeriodMonth){
		
		List<TimePeriodMonth> timePeriodMonths = new ArrayList<TimePeriodMonth>();
		
		long timeStart = timePeriodMonth.getMonthStart();
		long timeEnd = timePeriodMonth.getMonthEnd();
		
		while(timeStart <=timeEnd){
			TimePeriodMonth tm = getCurrentTimePeriodMonth(timeStart);
			
			if(	getMonthEndTs(tm.getMonthStart()) >	timeEnd){
				tm.setMonthEnd(timeEnd);
			}else{
				tm.setMonthEnd(getMonthEndTs(tm.getMonthStart()));
			}
			timePeriodMonths.add(tm);

			Calendar cal = Calendar.getInstance();
			cal.setTimeInMillis(tm.getMonthStart()); 
			cal.add(Calendar.MONTH, 1);
			timeStart = cal.getTimeInMillis();
		}
		
		return timePeriodMonths;
	}
	
	public static double fractionOfMonth(TimePeriodMonth timePeriodMonth){
		TimePeriodMonth fullTimePeriodMonth = DateUtils.getCurrentTimePeriodMonth(timePeriodMonth.getMonthStart());
		double givenTimeDiff = timePeriodMonth.getMonthEnd()-timePeriodMonth.getMonthStart();
		double monthTimeDiff = fullTimePeriodMonth.getMonthEnd()-fullTimePeriodMonth.getMonthStart();
		
		return 		 (double )Math.round(	(givenTimeDiff/monthTimeDiff)*100	)/100;//Math.round((givenTimeDiff*100/monthTimeDiff)/100);
		
	}
	
	public static Map<String, Long> getLastSeasonMonthTimes(long referenceTimeMillis, int seasonMonth){
		
		Map<String, Long> times = new HashMap<String, Long>();
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(referenceTimeMillis);
		
		if(cal.get(Calendar.MONTH) >seasonMonth ){
			cal.set(Calendar.MONTH, seasonMonth); // Set to May
		}else if(cal.get(Calendar.MONTH) <= seasonMonth ){
			cal.set(Calendar.MONTH, seasonMonth); // Set to May
			cal.add(Calendar.YEAR, -1); // Set to last year
		}
		times.put("end", getMonthEndTs(cal.getTimeInMillis()));
		times.put("start", getMonthStartTs(cal.getTimeInMillis()));
		
		return times;
	}
	
	public static TimePeriodMonth getLastSeasonMonthTimes2(long referenceTimeMillis, int seasonMonth){
		
		TimePeriodMonth timePeriodMonth = new TimePeriodMonth();
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(referenceTimeMillis);
		
		if(cal.get(Calendar.MONTH) >= seasonMonth ){
			cal.set(Calendar.MONTH, seasonMonth);
			cal.set(Calendar.DAY_OF_MONTH, 1); 
			
		}else if(cal.get(Calendar.MONTH) < seasonMonth ){
			cal.set(Calendar.MONTH, seasonMonth);
			cal.add(Calendar.YEAR, -1); // Set to last year
			cal.set(Calendar.DAY_OF_MONTH, 1); 
		}
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("MMM-yy");
		
		timePeriodMonth.setNameOfMonthYear(dateFormat.format(cal.getTime()));
		timePeriodMonth.setMonthStart(getMonthStartTs(cal.getTimeInMillis()));
		timePeriodMonth.setMonthEnd(getMonthEndTs(cal.getTimeInMillis()));
		
		return timePeriodMonth;
	}
	
	public static TimePeriodMonth getCurrentTimePeriodMonth(long referenceTimeMillis){
		
		TimePeriodMonth timePeriodMonth = new TimePeriodMonth();
		SimpleDateFormat dateFormat = new SimpleDateFormat("MMM-yy");
		timePeriodMonth.setNameOfMonthYear(dateFormat.format(referenceTimeMillis));
		timePeriodMonth.setMonthStart(getMonthStartTs(referenceTimeMillis)); 
		timePeriodMonth.setMonthEnd(getMonthEndTs(referenceTimeMillis)); 
		//System.out.println("timePeriodMonth :"+timePeriodMonth);
		return timePeriodMonth;
	}

	public static Map<String, Map<String, Long>> getTrendMonthsTimes(long referenceTimeMillis){
		
		Map<String, Map<String, Long>> times = new HashMap<String, Map<String, Long>>();
		
		Map<String, Long> troughSeasonMonthTimes = getLastSeasonMonthTimes( referenceTimeMillis, 4);// May
		Map<String, Long> peakSeasonMonthTimes = getLastSeasonMonthTimes( referenceTimeMillis, 10);// Nov
	
		Map<String, Long> sameMonthLastYearMonthTimes = getLastYearSameMonthTimes(referenceTimeMillis);
		times.put("trough", troughSeasonMonthTimes);
		times.put("peak", peakSeasonMonthTimes);
		times.put("lastyear", sameMonthLastYearMonthTimes);
		//times.put("current", sameMonthLastYearMonthTimes);
		
		return times;
	}	
	
public static Map<String, TimePeriodMonth> getTrendMonthsTimes2(long referenceTimeMillis){
		
		Map<String, TimePeriodMonth> times = new LinkedHashMap<String, TimePeriodMonth>();
		
		TimePeriodMonth troughSeasonMonthTimes = getLastSeasonMonthTimes2( referenceTimeMillis, 4);// May
		TimePeriodMonth peakSeasonMonthTimes = getLastSeasonMonthTimes2( referenceTimeMillis, 10);// Nov
		TimePeriodMonth currentMonthTimes = getCurrentTimePeriodMonth(referenceTimeMillis);
		TimePeriodMonth sameMonthLastYearMonthTimes = getLastYearSameMonthTimes2(referenceTimeMillis);

		times.put("lastyear", sameMonthLastYearMonthTimes);
		if(peakSeasonMonthTimes.getMonthStart() > troughSeasonMonthTimes.getMonthStart()){
			times.put("trough", troughSeasonMonthTimes);
			times.put("peak", peakSeasonMonthTimes);
		}else{
			times.put("peak", peakSeasonMonthTimes);
			times.put("trough", troughSeasonMonthTimes);
		}
		times.put("current", currentMonthTimes);
		//times.put("current", sameMonthLastYearMonthTimes);
		
		return times;
	}	
	
	public static Map<String, TimePeriodMonth> getMainDashboardRFTimes(long referenceTimeMillis){
		
		Map<String, TimePeriodMonth> times = new LinkedHashMap<String, TimePeriodMonth>();
		SimpleDateFormat dateFormat = new SimpleDateFormat("MMM-yy");
		Calendar cal2 = Calendar.getInstance();
		cal2.set(Calendar.MONTH, 4);// May
		cal2.set(Calendar.HOUR_OF_DAY, 0); 
		cal2.set(Calendar.MINUTE, 0);
		cal2.set(Calendar.SECOND, 0);
		cal2.set(Calendar.MILLISECOND, 0);
		TimePeriodMonth firstTimePeriod = getCurrentTimePeriodMonth(cal2.getTimeInMillis());
		firstTimePeriod.setNameOfMonthYear(dateFormat.format(firstTimePeriod.getMonthStart())); 
		times.put(firstTimePeriod.getNameOfMonthYear(), firstTimePeriod);
		 
		long startForAccumulatedTimePeriod = -1;
		while(true){
			cal2.add(Calendar.MONTH, 1);
			TimePeriodMonth tpm = new TimePeriodMonth();
			if(cal2.getTimeInMillis() <= referenceTimeMillis){
				
				tpm = getCurrentTimePeriodMonth(cal2.getTimeInMillis());
				times.put(tpm.getNameOfMonthYear(), tpm);
				if(startForAccumulatedTimePeriod == -1){
					startForAccumulatedTimePeriod = tpm.getMonthStart();
				}
			}else{
				break;
			}
		}
		
		TimePeriodMonth accumuatedTimeMonth = new TimePeriodMonth();
		accumuatedTimeMonth.setMonthStart(startForAccumulatedTimePeriod);
		accumuatedTimeMonth.setMonthEnd(referenceTimeMillis);
		accumuatedTimeMonth.setNameOfMonthYear("Accumulated"); 
		times.put(accumuatedTimeMonth.getNameOfMonthYear(), accumuatedTimeMonth);
		

		System.out.println("times in getMainDashboardRFTimes:"+times); 
		return times;
	}	
	
	public static int[] getPreviousYearsForACurrentMonth(long currentMilliSeconds, int currentMonth){
		TimePeriodMonth periodMonth = getLastSeasonMonthTimes2(currentMilliSeconds,currentMonth );
		
		long ts = periodMonth.getMonthStart();
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(ts);
		int year = calendar.get(Calendar.YEAR);
		
		int[] years = new int[36];
		for (int i = 0; i < years.length; i++) {
			years[i]=year-(i+1);
			
		}
		return years;
	}
	
	public static Map<String, TimePeriodMonth> getMainDashboardRFTimes2(long referenceTimeMillis){
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
		Map<String, TimePeriodMonth> times = new LinkedHashMap<String, TimePeriodMonth>();
		

		TimePeriodMonth currentMonth =  getCurrentTimePeriodMonth(referenceTimeMillis);
		currentMonth.setMonthEnd(referenceTimeMillis);
		currentMonth.setNameOfMonthYear("This Month Upto "+dateFormat.format(referenceTimeMillis)	);
	
		referenceTimeMillis = currentMonth.getMonthStart()-2;
		TimePeriodMonth previousMonth =  getCurrentTimePeriodMonth(referenceTimeMillis);
		
		referenceTimeMillis = previousMonth.getMonthStart()-2;
		TimePeriodMonth previousToPreviousMonth =  getCurrentTimePeriodMonth(referenceTimeMillis);
		
		times.put("month0", previousToPreviousMonth);
		times.put("month1", previousMonth);
		times.put("month2", currentMonth);

		TimePeriodMonth accumulatedFromJune = DateUtils.getLastSeasonMonthTimes2(System.currentTimeMillis(), 5);
		accumulatedFromJune.setMonthEnd(System.currentTimeMillis());
		System.out.println("accumulatedFromJune.getMonthStart():"+accumulatedFromJune.getMonthStart());
		accumulatedFromJune.setNameOfMonthYear(	dateFormat.format(accumulatedFromJune.getMonthStart())	+" to "	+dateFormat.format(System.currentTimeMillis()));
		times.put("accumulated", accumulatedFromJune);
		
	
		System.out.println("times in getMainDashboardRFTimes2:"+times); 
		return times;
	}	
	
	
	public static Map<String, TimePeriodMonth> getRFAnalysisTableTimes(long referenceTimeMillis, int year){
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
		Map<String, TimePeriodMonth> times = new LinkedHashMap<String, TimePeriodMonth>();
		

		TimePeriodMonth accumulatedFromJune = DateUtils.getLastSeasonMonthTimes2(System.currentTimeMillis(), 5);
		accumulatedFromJune.setMonthEnd(System.currentTimeMillis());
		
		TimePeriodMonth previousAccumulatedFromJune = new TimePeriodMonth();
		
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(accumulatedFromJune.getMonthStart()); 
		calendar.set(Calendar.YEAR, year); 
		previousAccumulatedFromJune.setMonthStart(calendar.getTimeInMillis()); 
		
		calendar = Calendar.getInstance(); 
		calendar.setTimeInMillis(accumulatedFromJune.getMonthEnd());
		
		int month = DateUtils.getMonthFromTs(System.currentTimeMillis());
		
		/**
		 * Consider when year is 2016 month is jan
		 * then startsTs : 31 May 2015
		 * endTs should be : currentDay - 2016
		 */
		if(month < 5){
			calendar.set(Calendar.YEAR, year+1); 
		}
		else calendar.set(Calendar.YEAR, year); 
		previousAccumulatedFromJune.setMonthEnd(calendar.getTimeInMillis());
		
		
		
		accumulatedFromJune.setNameOfMonthYear(	dateFormat.format(accumulatedFromJune.getMonthStart())	+" to "	+dateFormat.format(System.currentTimeMillis()));
		previousAccumulatedFromJune.setNameOfMonthYear(	dateFormat.format(previousAccumulatedFromJune.getMonthStart())	+" to "	+dateFormat.format(previousAccumulatedFromJune.getMonthEnd()));
		times.put("accumulated", accumulatedFromJune);
		times.put("previousAccumulated", previousAccumulatedFromJune);
		
	
		System.out.println("times in getRFAnalysisTableTimes:"+times); 
		return times;
	}	
	
	
	public static Map<String, TimePeriodMonth> getTimePeriodsForReservoirCandleStick(long referenceTimeMillis, int year){
		TimePeriodMonth chosenYearCurrentDayTimePeriodMonth = getGivenYearSamDayTimes( referenceTimeMillis, year);
		chosenYearCurrentDayTimePeriodMonth.setNameOfMonthYear("sameDayChosenYear");
		
		TimePeriodMonth currentDayPeriodMonth = getGivenYearSamDayTimes( referenceTimeMillis, Calendar.getInstance().get(Calendar.YEAR));
		currentDayPeriodMonth.setNameOfMonthYear("toDay");
		
		/*TimePeriodMonth currentYear = new TimePeriodMonth();//		getStartOfYear(year);
		currentYear.setMonthStart(getStartOfYear(Calendar.getInstance().get(Calendar.YEAR)));
		currentYear.setMonthEnd(getEndOfYear(Calendar.getInstance().get(Calendar.YEAR)));
		//currentYear.setNameOfMonthYear(""+Calendar.getInstance().get(Calendar.YEAR)); 
		currentYear.setNameOfMonthYear("currentYear");*/
		

		TimePeriodMonth chosenYear = new TimePeriodMonth();//		
		chosenYear.setMonthStart(getStartOfYear(year));
		chosenYear.setMonthEnd(System.currentTimeMillis());
		chosenYear.setNameOfMonthYear("chosenYear");//""+year); 

		
		
		Map<String, TimePeriodMonth> times = new LinkedHashMap<String, TimePeriodMonth>();
		times.put("previousToDay", chosenYearCurrentDayTimePeriodMonth);
		times.put("currentToDay", currentDayPeriodMonth);
		//times.put("currentYear", currentYear);
		times.put("chosenYear", chosenYear);
		
		return times;
	}

	
	
	public static Map<String, TimePeriodMonth> getReservoirDashboardTimes(long referenceTimeMillis, int year){ 
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
		Map<String, TimePeriodMonth> times = new LinkedHashMap<String, TimePeriodMonth>();
		
		TimePeriodMonth current24hrsTimePeriodMonth = new TimePeriodMonth();
		current24hrsTimePeriodMonth.setMonthStart(getLastMonthSameTimeTs(referenceTimeMillis));
		current24hrsTimePeriodMonth.setMonthEnd(referenceTimeMillis);
		current24hrsTimePeriodMonth.setNameOfMonthYear("Current");
		TimePeriodMonth currentMonsoonTimePeriodMonth =  getLastSeasonMonthTimes2(referenceTimeMillis, 5);
		currentMonsoonTimePeriodMonth.setMonthEnd(referenceTimeMillis);
		currentMonsoonTimePeriodMonth.setNameOfMonthYear(dateFormat.format(currentMonsoonTimePeriodMonth.getMonthStart())	+" to "	+dateFormat.format(currentMonsoonTimePeriodMonth.getMonthEnd()));
		
		TimePeriodMonth currentKharifTimePeriod = getLastSeasonMonthTimes2(referenceTimeMillis, 6);
		long currentKharifTimePeriodStartInMillis = currentKharifTimePeriod.getMonthStart();
		Calendar kharifCalendar = Calendar.getInstance();
		kharifCalendar.setTimeInMillis(currentKharifTimePeriodStartInMillis); 
		kharifCalendar.add(Calendar.MONTH, 3);
		currentKharifTimePeriod.setMonthEnd(getMonthEndTs(kharifCalendar.getTimeInMillis()));	
		currentKharifTimePeriod.setNameOfMonthYear(dateFormat.format(currentKharifTimePeriod.getMonthStart())	+" to "	+dateFormat.format(currentKharifTimePeriod.getMonthEnd())); 
		
		
		TimePeriodMonth currentRabiTimePeriod = getLastSeasonMonthTimes2(referenceTimeMillis, 9);
		long currentRabiTimePeriodStartInMillis = currentRabiTimePeriod.getMonthStart();
		Calendar rabiCalendar = Calendar.getInstance();
		rabiCalendar.setTimeInMillis(currentRabiTimePeriodStartInMillis); 
		rabiCalendar.add(Calendar.MONTH, 5);
		currentRabiTimePeriod.setMonthEnd(getMonthEndTs(rabiCalendar.getTimeInMillis()));
		currentRabiTimePeriod.setNameOfMonthYear(dateFormat.format(currentRabiTimePeriod.getMonthStart())	+" to "	+dateFormat.format(currentRabiTimePeriod.getMonthEnd()));
		
		times.put("current", current24hrsTimePeriodMonth);
		times.put("monsoon", currentMonsoonTimePeriodMonth);
		times.put("kharif", currentKharifTimePeriod);
		times.put("rabi", currentRabiTimePeriod);
		
		return times;
		
	}
	
	/**
	 * Jira: VAS-215
	 * return today and yesteraday time period 
	 * today: from todays 12 AM to current time
	 * yesterday: yesterdays 12 AM to todays 12 AM
	 */
    public static Map<String,TimePeriodMonth> getYesterdayTodayTimePeriods(){
        HashMap<String,TimePeriodMonth> timePeriods=new HashMap<>();
        long referenceTs=System.currentTimeMillis();
        TimePeriodMonth current=new TimePeriodMonth();
        Calendar cal=Calendar.getInstance();
        cal.setTimeInMillis(referenceTs);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        current.setMonthStart(cal.getTimeInMillis());
        current.setMonthEnd(referenceTs);
        
        TimePeriodMonth yesterday=new TimePeriodMonth();
        yesterday.setMonthEnd(current.getMonthStart()-1);
        yesterday.setMonthStart(current.getMonthStart()-TWENTY_FOUR_HOURS_IN_SECONDS);
        //System.out.print(yesterday.getMonth);
        timePeriods.put("today",current);
        timePeriods.put("yesterday", yesterday);
        
        return timePeriods;
    }

	
	public static TimePeriodMonth getLastWeekTimePeriodMonth() {
		TimePeriodMonth timePeriodMonth = new TimePeriodMonth();
		timePeriodMonth.setMonthStart(System.currentTimeMillis()-WEEK_IN_SECONDS);
		timePeriodMonth.setMonthEnd(System.currentTimeMillis());
		return timePeriodMonth;
	}
	
	public static TimePeriodMonth getYesterdayTimePeriodMonth() {
		TimePeriodMonth timePeriodMonth = new TimePeriodMonth();
		timePeriodMonth.setMonthStart(System.currentTimeMillis()-(TWENTY_FOUR_HOURS_IN_SECONDS*2));
		timePeriodMonth.setMonthEnd(System.currentTimeMillis()-TWENTY_FOUR_HOURS_IN_SECONDS);
		return timePeriodMonth;
	}
	
	public static String getDateInFormat(String format, long referenceTimeMillis){
		SimpleDateFormat dateFormat = new SimpleDateFormat(format);
		return dateFormat.format(referenceTimeMillis);
	}
	
	public static int getYear(long referenceTimeMillis) {
		Calendar c = Calendar.getInstance();
		c.setTimeInMillis(referenceTimeMillis);
		return c.get(Calendar.YEAR);
	}

	public static Date getNextHourStart(long referenceTimeMillis) {
		Calendar cal = Calendar.getInstance();
		
		cal.add(Calendar.HOUR_OF_DAY, 1);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		Date nextHourStart = cal.getTime();
		
		return nextHourStart;
	}

	public static Date getNextDayStart(long referenceTimeMillis) {
		Calendar cal = Calendar.getInstance();
		
		cal.add(Calendar.DATE, 1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		Date nextDayStart = cal.getTime();
		
		return nextDayStart;
	}

	public static Date getNextWeekStart(long referenceTimeMillis) {
		Calendar cal = Calendar.getInstance();

		cal.set(Calendar.DAY_OF_WEEK, Calendar.SATURDAY);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		cal.add(Calendar.DATE, 1);
		Date nextWeekStart = cal.getTime();
		
		return nextWeekStart;
	}


	/**
	 * Returns the last N Days time stamp
	 * 
	 * @param referenceTimeMillis
	 * @param nDays
	 * @return
	 */
	public static long getLastNDaysTs(long referenceTimeMillis, int nDays) {
		referenceTimeMillis = referenceTimeMillis - (TWENTY_FOUR_HOURS_IN_SECONDS * nDays);

		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(referenceTimeMillis);
		
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTimeInMillis();
	}

	public static Date getNextMonthStart(long referenceTimeMillis) {
		Calendar cal = Calendar.getInstance();

		cal.add(Calendar.MONTH, 1);
		cal.set(Calendar.DATE, 1);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		Date nextMonthStart = cal.getTime();
		
		return nextMonthStart;
	}
	
	public static int getCurrentYearAndMonth(){

		return getYearAndMonth(Calendar.getInstance().getTimeInMillis());
		
	}
	
	public static List<Integer> getYearAndMonthList(long starts, long endts) {
		
		List<Integer> yearMonthList = null;
		
		Calendar cal1 = Calendar.getInstance();
		cal1.setTimeInMillis(starts);
		cal1.set(Calendar.DATE, 1);
		cal1.set(Calendar.HOUR_OF_DAY, 0);
		cal1.set(Calendar.MINUTE, 0);
		cal1.set(Calendar.SECOND, 0);
		cal1.set(Calendar.MILLISECOND, 0);
		
		Calendar cal2 = Calendar.getInstance();
		cal2.setTimeInMillis(endts);
		cal2.set(Calendar.DATE, 1);
		cal2.set(Calendar.HOUR_OF_DAY, 0);
		cal2.set(Calendar.MINUTE, 0);
		cal2.set(Calendar.SECOND, 0);
		cal2.set(Calendar.MILLISECOND, 0);
		
		long t1 = cal1.getTimeInMillis();
		long t2 = cal2.getTimeInMillis();
		
		Calendar cal3 = Calendar.getInstance();
		cal3.setTimeInMillis(t1);
		
		while(t1<=t2){

			if(yearMonthList == null){
				yearMonthList = new ArrayList<Integer>();
			}

			yearMonthList.add(getYearAndMonth(t1));
			
			cal3.add(Calendar.MONTH, 1);
			t1 = cal3.getTimeInMillis();
		}
		
		
		return yearMonthList;
	}
	
	public static List<String> getYearAndMonthStringsList(long starts, long endts) {
		
		List<String> yearMonthList = null;
		
		Calendar cal1 = Calendar.getInstance();
		cal1.setTimeInMillis(starts);
		cal1.set(Calendar.DATE, 1);
		cal1.set(Calendar.HOUR_OF_DAY, 0);
		cal1.set(Calendar.MINUTE, 0);
		cal1.set(Calendar.SECOND, 0);
		cal1.set(Calendar.MILLISECOND, 0);
		
		Calendar cal2 = Calendar.getInstance();
		cal2.setTimeInMillis(endts);
		cal2.set(Calendar.DATE, 1);
		cal2.set(Calendar.HOUR_OF_DAY, 0);
		cal2.set(Calendar.MINUTE, 0);
		cal2.set(Calendar.SECOND, 0);
		cal2.set(Calendar.MILLISECOND, 0);
		
		long t1 = cal1.getTimeInMillis();
		long t2 = cal2.getTimeInMillis();
		
		Calendar cal3 = Calendar.getInstance();
		cal3.setTimeInMillis(t1);
		
		while(t1<=t2){

			if(yearMonthList == null){
				yearMonthList = new ArrayList<String>();
			}

			yearMonthList.add(getYearAndMonthString(t1));
			
			cal3.add(Calendar.MONTH, 1);
			t1 = cal3.getTimeInMillis();
		}
		
		
		return yearMonthList;
	}
	
	
	
	public static int getYearAndMonth(long timeStamp){
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMM");
		String label = dateFormat.format(timeStamp);
		return Integer.parseInt(label);
	
	}
	
public static String getYearAndMonthString(long timeStamp){
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMM");
		String label = dateFormat.format(timeStamp);
		return label;
	
	}
	
	public static int getRFYearAndMonth(long timeStamp){
		// 30600000+1 ms = 30600001 ( = 8 hrs 30 min and 1 ms
		timeStamp-=30600001;		
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMM");
		String label = dateFormat.format(timeStamp);
		return Integer.parseInt(label);
	
	}
	
	public static int getYearFromTs(long referencetimeInMillis){
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(referencetimeInMillis);
		return cal.get(Calendar.YEAR);
	}
	
	public static int getMonthFromTs(long referencetimeInMillis){
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(referencetimeInMillis);
		return cal.get(Calendar.MONTH);
	}
	
	//Returns Month and year in the format of MMM-yy i.e Aug-16 for August-2016
		public static String getMonthYearFromTs(long referencetimeInMillis){
			
			SimpleDateFormat simpleDateFormat=new SimpleDateFormat("MMM-yy");
			Calendar cal=Calendar.getInstance();
			cal.setTimeInMillis(referencetimeInMillis);
			return simpleDateFormat.format(cal.getTime());
		}
		
		public static int getDayFromTs(long referencetimeInMillis){
			Calendar cal = Calendar.getInstance();
			cal.setTimeInMillis(referencetimeInMillis);
			return cal.get(Calendar.DAY_OF_MONTH);
		}
		
		public static int getRFDayFromTs(long referencetimeInMillis){
			Calendar cal = Calendar.getInstance();
			// 30600000+1 ms = 30600001 ( = 8 hrs 30 min and 1 ms
			referencetimeInMillis-=30600001;		
			cal.setTimeInMillis(referencetimeInMillis);
			return cal.get(Calendar.DAY_OF_MONTH);
		}
		
		public static String getMonthStringFromMonthNo(int month_no){
			String month = month_no+"";
			String monthString = "";
			SimpleDateFormat monthParse = new SimpleDateFormat("MM");
		    SimpleDateFormat monthDisplay = new SimpleDateFormat("MMM");
		    try {
		    	monthString = monthDisplay.format(monthParse.parse(month));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    
		    return monthString;
		}
	
		/**
		 * Get the no. of days in the month represented by referenceInMillis
		 * 
		 * @param referencetimeInMillis
		 * @return
		 */
		public static int getNoOfDaysInMonth(long referencetimeInMillis) {
			Calendar cal = Calendar.getInstance();
			cal.setTimeInMillis(referencetimeInMillis);
			return cal.getActualMaximum(Calendar.DAY_OF_MONTH);
		}
		
		/**
		 * For input yearMonth (YYYYMM) returns the no. of days
		 * in the corresponding month
		 * 
		 * @param yearMonth
		 * @return
		 */
		public static int getNoOfDaysInMonth(int yearMonth) {
			
			int year = yearMonth / 100;
			int month = yearMonth % 100;
			
			Calendar cal = Calendar.getInstance();
			cal.set(year, month - 1, 1, 0, 0);
			return cal.getActualMaximum(Calendar.DAY_OF_MONTH);
		}

		/**
		 * Returns the monsoon year for input yearMonth which is in the format
		 * YYYYMM
		 * 
		 * @param eventGenYear
		 * @return
		 */
		public static int getMonsoonYear(int yearMonth) {
			
			int year = yearMonth/100;
			int month = yearMonth%100;
			if (month > 5) {
				return year;
			}
			return (year - 1);
		}
		
		public static String changeDateFormat(String fromFormat, String toFormat, String fromFormatValue){
			SimpleDateFormat sdfFrom = new SimpleDateFormat(fromFormat);
			SimpleDateFormat sdfTo = new SimpleDateFormat(toFormat);
			String toFormatValue = fromFormatValue; // Worst case?
			try {
				java.util.Date dateFrom =  sdfFrom.parse(fromFormatValue);
				toFormatValue = sdfTo.format(dateFrom);
			} catch (ParseException e) {
				// TODO: what if fails?
				System.out.println(e.getMessage()+" Could not convert the date to "+sdfTo+" format");
			}
			
			return toFormatValue;
		}
	
		/*
		 * Returns last monsoon start day 
		 * e.g. for today - 06Apr16, returns 01Jun15
		 */
		public static long getStartOfMonsoon(long referenceTs) {
			
			Calendar cal = Calendar.getInstance();
			cal.setTimeInMillis(referenceTs);
			int month = getMonthFromTs(referenceTs);
			if(month >= 5){
				cal.set(Calendar.MONDAY, Calendar.JUNE);
				cal.set(Calendar.DAY_OF_MONTH, 1);
			}else {
				cal.add(Calendar.YEAR, -1);
				cal.set(Calendar.MONDAY, Calendar.JUNE);
				cal.set(Calendar.DAY_OF_MONTH, 1);
			}
			
			
			return getStartOfDay(cal.getTimeInMillis());
		}

		/*
		 * Returns start of month ts - 1st day of month at 12:00:00.0000
		 */
		public static long getStartOfMonth(long referenceTs) {
			
			Calendar cal = Calendar.getInstance();
			cal.setTimeInMillis(referenceTs);
			cal.set(Calendar.DAY_OF_MONTH, 1);
			
			return getStartOfDay(cal.getTimeInMillis());
		}

		
		/**
		 * Returns event gen year for input referenceTs
		 * @param monsoonStartTs
		 * @return
		 */
		public static int getEventGenYear(long referenceTs) {
			Calendar cal = Calendar.getInstance();
			cal.setTimeInMillis(referenceTs);
			
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMM");
			String label = dateFormat.format(cal.getTimeInMillis());
			return Integer.parseInt(label);
		}

		public static long getPastYearSameTimeTs(long referenceTs, int year) {

			Calendar cal = Calendar.getInstance();
			cal.setTimeInMillis(referenceTs);
			int currYear = cal.get(Calendar.YEAR);
			cal.set(Calendar.YEAR, (year - currYear));

			return cal.getTimeInMillis();
		}
		
		public static Long getYYYYMMdd(long referenceTs) {
			Calendar cal = Calendar.getInstance();
			cal.setTimeInMillis(referenceTs);
			
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
			String label = dateFormat.format(cal.getTimeInMillis());
			return Long.parseLong(label);
		}
		
		public static String getNextDayInFormat(String format, String dayInFormat) {
			SimpleDateFormat sdf = new SimpleDateFormat(format);
			Date lastUpdateTime = null;
			try {
				lastUpdateTime = sdf.parse(dayInFormat);
			} catch (ParseException e) {
				System.out.println("Error while parsing the date" + dayInFormat);
				e.printStackTrace();
			}
			long nextDayTs = lastUpdateTime.getTime() + (24*60*60*1000) + 1;
			return sdf.format(nextDayTs);
		}
		
		/**
		 * Given a date in 'yyyyMMdd' format, it will return the start of the Monsoon [in 'yyyyMMdd' format]
		 * @param modelDate
		 * @return
		 */
		public static int getStartOfMonsoon(int modelDate) {
			long referenceTs = getModelDateInMillis(modelDate);
			long startOfMonsoon = DateUtils.getStartOfMonsoon(referenceTs);

			Date dtStartOfMonsoon = new Date(startOfMonsoon);
			SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
			String strDate =  format.format(dtStartOfMonsoon);
			
			return Integer.parseInt(strDate);
		}
		
		/**
		 * Given a date in 'yyyyMMdd' format, it will be converted to milliseconds since epoch.
		 * Implicitly, the hour will be set to 00:00 (start of the day)
		 * 
		 * @param modelDate
		 * @return
		 */
		public static long getModelDateInMillis(int modelDate) {
			int year = modelDate/10000;
			int month = (modelDate% 10000) / 100;
			int day = modelDate % 100;

			Calendar cal = Calendar.getInstance();
			cal.set(year, month-1, day,0,0,0);
			
			return cal.getTimeInMillis();
		}		
		
		
		public static List<String> getLast7DayTime(long referenceTs , String dateformat){

			SimpleDateFormat sdf = new SimpleDateFormat(dateformat);
			List<String> last7Days = new ArrayList<String>();
			
			Calendar cal = Calendar.getInstance();
			cal.setTimeInMillis(referenceTs);
			
			int day = cal.get(Calendar.DAY_OF_MONTH);
		    int month = cal.get(Calendar.MONTH);
		    int year = cal.get(Calendar.YEAR);

			
		    for(int i=day-1; i > (day-7); i--){
	            cal.set(year, month, i);
	            last7Days.add(sdf.format(cal.getTime()));
	        }
			
			
			return last7Days;
			
		}
		
		public static Integer getModelDateFromTs(long referenceTs) {
			Calendar cal = Calendar.getInstance();
			cal.setTimeInMillis(referenceTs);
			
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
			String label = dateFormat.format(cal.getTimeInMillis());
			return Integer.parseInt(label);
		}
		
		/**
		 * day of the year ( example: For January 1,2016 J=1 and Decemeber 31,2016 J=366 and Today = 298)
		 * @param date in millis
		 * @return day of the year
		 * */
		
		public static int getDayOfYear(long referenceTimeMillis){

			Calendar cal = Calendar.getInstance();
			cal.setTimeInMillis(referenceTimeMillis);
			   
			return cal.get(Calendar.DAY_OF_YEAR);
		}
		
		/**
		 * Converts input date and date format into equivalent time stamp
		 * Returns 0 for invalid format
		 * 
		 * @param date
		 * @param format
		 * @return time stamp
		 */
		public static long getTimestamp(String date, String format) {
			
			SimpleDateFormat sd = new SimpleDateFormat(format);
			Date value;
			try {
				value = sd.parse(date);
				return value.getTime();
			} catch (ParseException e) {
				e.printStackTrace();
				System.out.println("Error parsing date (format) : " + date + "(" + format + ")");
				return 0;
			}
			
		}
		
		
		/**
		 * substracts X days from the given reference date
		 * and returns the new date.
		 * 
		 * The input and return dates are modelDates, in the format of YYYYMMDD
		 * 
		 * @param modelDate
		 * @param noOfDays
		 * @return
		 */
		public static int substractDaysFromModelDate(int modelDate, int noOfDays) {
			long timeStamp = getModelDateInMillis(modelDate);
			timeStamp = timeStamp - (noOfDays * TWENTY_FOUR_HOURS_IN_SECONDS);
			return getModelDateFromTs(timeStamp);
		}
		
		/**
		 * substracts X days from the given reference date = startData
		 * and returns the list of dates from startData till Reference date.
		 * 
		 * The input and return dates are modelDates, in the format of YYYYMMDD
		 * 
		 * @param modelDate
		 * @param noOfDays
		 * @return list of dates in ascending order
		 */
		public static List <Integer> substractNDaysFromModelDate(int modelDate, int noOfDays) {
			List <Integer> dates = new ArrayList<Integer>();
			
			long timeStamp = getModelDateInMillis(modelDate);
			for(int i=noOfDays;i>=0;i--){
				
				dates.add(getModelDateFromTs( timeStamp - (i * TWENTY_FOUR_HOURS_IN_SECONDS)));
			}
		
			return dates;
		}

		/**
		 * Returns the crop Year based on the Model Date passed
		 * TODO:: create mapping between crop Year and IWM monsoon year
		 * Currently reuturns year in model date only, but need to be changed
		 * 
		 * @param modelDate - YYYYmmdd
		 * @return
		 */
		public static int getCropYear(int modelDate) {
			int year = modelDate/10000;
			return year;
		}
		/**
		 * This method computes date in a given format based on noOfday with respect to modelDate
		 * 
		 * @param format
		 * @param modelDate
		 * @param noOfday
		 * @return : return date in format
		 */
		public static String getNDayInFormat(String format, String modelDate,int noOfday) {
			SimpleDateFormat sdf = new SimpleDateFormat(format);
			Date lastUpdateTime = null;
			try {
				lastUpdateTime = sdf.parse(modelDate);
			} catch (ParseException e) {
				System.out.println("Error while parsing the date" + modelDate);
				e.printStackTrace();
			}
			long nextDayTs = lastUpdateTime.getTime() + (24*60*60*1000) * noOfday + 1;
			return sdf.format(nextDayTs);
		}
		/**
		 * 
		 * @param format
		 * @return Today Date in a given format
		 */
		public static Integer  getTodayDateInformat(String format){
			DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
			Date date = new Date();
			return Integer.parseInt(dateFormat.format(date));
		}
		/**
		 * return month from model date 
		 * @param modelDate
		 * @param format
		 * @return : 
		 * @throws ParseException 
		 */
		public static int getMonthFromDate(int modelDate,String format) throws ParseException{
			
			DateFormat inputDF  = new SimpleDateFormat(format);
			Date date = inputDF.parse(String.valueOf(modelDate));
		
		//	Date date = new Date(modelDate);
			Calendar cal = Calendar.getInstance();
		    cal.setTime(date);
		 
		    int month = cal.get(Calendar.MONTH);
		
			return (month+1);
		}
		/**
		 * return  day from model date
		 * @param modelDate
		 * @param format
		 * @return : Day
		 * @throws ParseException 
		 */
		public static int getDayFromDate(int modelDate,String format) throws ParseException{
			
			DateFormat inputDF  = new SimpleDateFormat(format);
			Date date = inputDF.parse(String.valueOf(modelDate));
		
		//	Date date = new Date(modelDate);
			Calendar cal = Calendar.getInstance();
		    cal.setTime(date);
		 
		   
		    int day = cal.get(Calendar.DAY_OF_MONTH);
		  
			return day;
		}
		/**
		 * return  year from model date
		 * @param modelDate
		 * @param format
		 * @return : Day
		 * @throws ParseException 
		 */
		public static int getYearFromDate(int modelDate,String format) throws ParseException{
			
			DateFormat inputDF  = new SimpleDateFormat(format);
			Date date = inputDF.parse(String.valueOf(modelDate));
		
		//	Date date = new Date(modelDate);
			Calendar cal = Calendar.getInstance();
		    cal.setTime(date);
		  
			return cal.get(Calendar.YEAR);
		}
		
		/**
		 * This method will check wheather model date is in a given time period or not. without considering year. 
		 * @param modelDate1
		 * @param startmonth
		 * @param startday
		 * @param endmonth
		 * @param endday
		 * @param fromat
		 * @return
		 * @throws ParseException
		 */
		public static int  isModelDateInPeriod(int modelDate1,int startmonth, int startday , int endmonth, int endday ,String fromat) throws ParseException{
			SimpleDateFormat sdf = new SimpleDateFormat(fromat);
	        Date date1 = sdf.parse(String.valueOf(modelDate1));
	        Calendar modelCal = Calendar.getInstance();	    
	        modelCal.setTime(date1);
	        int modelMonth = modelCal.get(Calendar.MONTH)+1;
	        int modelDay = modelCal.get(Calendar.DAY_OF_MONTH);
	      
	        if(startmonth <= endmonth){
	        	if((modelMonth == startmonth && modelDay >= startday) || (modelMonth == endmonth  && modelDay <= endday  )  ){
	        		return 0;
	        	}else 
	        	if(modelMonth > startmonth && modelMonth < endmonth ){
	        		return 0;
	        	}else {
	        		return -1;
	        	}
	        }else {
	        	if((modelMonth == startmonth && modelDay >= startday) || (modelMonth == endmonth  && modelDay <= endday)){
	        		return 0;
	        	}else
	        	if(modelMonth > startmonth ){
	        		return 0;
	        	}
	        	else
	        	if(modelMonth < endmonth ){
	        		return 0;
	        	}
	        	else 
	        	{
	        		return -1;
	        		
	        	}
	        }
	        
		}
		/**
		 * add X days from the given reference date = startData
		 * and returns the list of dates from day after startData till no. of days.
		 * 
		 * The input and return dates are modelDates, in the format of YYYYMMDD
		 * 
		 * @param modelDate
		 * @param noOfDays
		 * @return list of dates in ascending order
		 */
		public static List <Integer> addNDaysFromModelDate(int modelDate, int noOfDays) {
			List <Integer> dates = new ArrayList<Integer>();
			
			long timeStamp = getModelDateInMillis(modelDate);
			for(int i = 1 ;i <= noOfDays; i++){
				long dateTs = timeStamp + ((long)i * TWENTY_FOUR_HOURS_IN_SECONDS);
				dates.add(getModelDateFromTs(dateTs));
			}
			return dates;
		}
		
		/**
		 * Returns timeStamp in milliseconds
		 * Eg.  hourSince is 1026456, hourSinceDate is 1900-01-01 and hourSinceDateFormat is yyyy-MM-dd
		 * 
		 * @param hoursSince  
		 * @param hourSinceDate
		 * @param hourSinceDateFormat
		 * @return
		 */
		public static long getTimeStamp(int hoursSince, String hourSinceDate, String hourSinceDateFormat) {
			long timeStamp = getTimestamp(hourSinceDate, hourSinceDateFormat);
			return  (timeStamp + ((long)hoursSince * ONE_HOUR_IN_MS));
			
		}

		/**
		 * Returns date in modelDateFormat
		 * 
		 * @param hoursSince
		 * @param hourSincedate
		 * @param hourSinceDateformat
		 * @param modelDateFormat
		 * @return
		 */
		public static String getModelDateFromHourSince(int hoursSince, String hourSincedate, String hourSinceDateformat, String modelDateFormat) {
			return getDateInFormat(modelDateFormat, getTimeStamp(hoursSince, hourSincedate, hourSinceDateformat));
		}
		/**
		 * add X days from the given reference date = startData
		 * and returns modelDate + noOfDays
		 * 
		 * The input and return dates are modelDates, in the format of yyyymmdd
		 * 
		 * @param modelDate
		 * @param noOfDays
		 * @return date after adding noOfDays to modelDate
		 */
		public static int addNDaysToModelDate(int modelDate, int noOfDays) {
			
			long timeStamp = getModelDateInMillis(modelDate);
			return getModelDateFromTs( timeStamp + (noOfDays * TWENTY_FOUR_HOURS_IN_SECONDS));
		}
		
		/**
		 * Substract two model dates (DDMMYYYY) and return the no of days.
		 * It substracts smaller date from bigger date, so the result is always positive
		 * 
		 * @param modelDate1
		 * @param modelDate2
		 * @return date after adding noOfDays to modelDate
		 */
		public static int substractModelDates(int modelDate1, int modelDate2) {
			if (modelDate1 > modelDate2) {
				return substractModelDates(modelDate2, modelDate1);
			}
			long start = getModelDateInMillis(modelDate1);
			long end = getModelDateInMillis(modelDate2);
			return (int) ((end-start) / TWENTY_FOUR_HOURS_IN_SECONDS);
		}
		
		public static long getMillisForDate(int year, int month, int day, int hour, int minute, int second) {
			Calendar cal = Calendar.getInstance();
			cal.set(year, month, day, hour, minute, second);
			return cal.getTimeInMillis();
		}
		
		public static int getSeasonStartDateFromModelDate(int modelDate){
			
			Calendar cal=Calendar.getInstance();
			DateFormat format=new SimpleDateFormat("yyyymmdd");
			format.format(modelDate);
			cal=format.getCalendar();
			
			if(cal.MONTH >= Calendar.MARCH && cal.DATE >=1 && cal.MONTH <= Calendar.MAY && cal.DATE<=31){
				return 20170301;
			}
			else if(cal.MONTH >= Calendar.JUNE && cal.DATE >=1 && cal.MONTH <= Calendar.AUGUST && cal.DATE<=31){
				return 20170601;
			}
			else if(cal.MONTH >= Calendar.SEPTEMBER && cal.DATE >=1 && cal.MONTH <= Calendar.NOVEMBER && cal.DATE<=30){
				return 20170901;
			}
			else{
				/*
				 * this is for the period from december of the last year to the march of the current year.
				 * It's Winter
				 * 
				 */
				return 20161201;
			}
			
			/*
			Calendar cal = Calendar.getInstance();
			int year = modelDate/10000;
			int month = (modelDate/100)%100-1;//Month is zero based index
			int day = modelDate%100;
			cal.set(year, month, day);
			*/
			
			
		}
}
