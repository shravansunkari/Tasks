package controllers.com.vassarlabs.play.controllers;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.JsonNode;
import com.vassarlabs.common.dsp.err.DSPException;
import com.vassarlabs.common.utils.DateUtils;
import com.vassarlabs.iwm.soilmoisture.stress.dsp.api.INRSCDataFileUploadDSP;
import com.vassarlabs.iwm.soilmoisture.stress.service.api.INRSCFileUploadService;
import com.vassarlabs.offlineprocess.parsers.api.IInputParams;
import com.vassarlabs.offlineprocess.parsers.impl.InputParams;

import controllers.com.vassarlabs.play.controllers.cache.CacheConstants;
import play.libs.Akka;
import play.libs.Json;
import play.mvc.Http.MultipartFormData;
import play.mvc.Http.MultipartFormData.FilePart;
import scala.concurrent.duration.Duration;
import play.mvc.Result;

@org.springframework.stereotype.Controller
public class SoilMoistureFileUploadController extends BasicController implements InitializingBean{

	@Autowired
	INRSCFileUploadService fileUploadService;
	
	@Autowired
	INRSCDataFileUploadDSP nrscService;
	
	private static final String FILEUPLOAD_PAGE_OTHER_PARAMS = "params";
	private static final String LOCAL_FILE_DIRECTORY_URL = "/home/nrscftp/";
	private static String LAST_UPLOADED_FILE;

	private void deleteOldWFRecords(){
		/*
		 * TODO
		 */
		//nrscService.deleteOldWeatherForecastRecords();
		/*try {
			
		} catch (DSPException e) {
			e.printStackTrace();
		}*/
	}
	
	private void insertRecords(String fileURL, String fileName){
		System.out.println("Automatic data uploader service. File path : " + fileURL + " fileName=" + fileName);
		List<String> errors = fileUploadService.processNRSCDataFromFile(getDateAsModelDate(fileName), new File(fileURL));
		if (!errors.isEmpty()) {
			System.out.println("Error in uploading File path : " + fileURL + " fileName=" + fileName);
		}
	}

	private void pickLatestFileForNRSCData(){
		File latestLocalFile;
		
        latestLocalFile = getLatestFile();
        
        if(latestLocalFile != null){
            System.out.println("NRSCFILEUPLOAD Controller : Automatic scheduler. Latest file found is : " + latestLocalFile.getName());
            File localMachineFileURL = new File(LOCAL_FILE_DIRECTORY_URL + latestLocalFile.getName());
            insertRecords(localMachineFileURL.getPath(), latestLocalFile.getName());
        }
	}
	
	private void insertLatestFiles() throws DSPException, IOException{
		File[] filesInLocalFolder = new File(LOCAL_FILE_DIRECTORY_URL).listFiles();
		for(File file : filesInLocalFolder){
			int modelDate = getDateAsModelDate(file.getName());
			fileUploadService.insertNRSCDataFromFile(modelDate, file,file.lastModified());
		}
	}
	
	private int getDateAsModelDate(String fileName){
		
		//Sample file name NRSC_VIC_AP_3min_WBC_20161201.txt
		String result = fileName;
		
		String[] tmp = fileName.split("_");
		result = tmp[tmp.length-1].split("\\.")[0];
		
		return Integer.parseInt(result);
	}
	
	private File getLatestFile(){
		File localFolder;
		File[] localFileList;
		localFolder = new File(LOCAL_FILE_DIRECTORY_URL);
        localFileList = localFolder.listFiles();
		
        File latestFile = null;
		long maxTS = -1;
		
		if(localFileList == null || localFileList.length == 0){
			System.out.println("ISRO File uploadController : no files present in local directory");
			return null;
		}
		
		/**
		 * get the file with latest lastModified value.
		 */
		for(File file : localFileList){
			if(file.lastModified() > maxTS){
				maxTS = file.lastModified();
				latestFile = file;
			}
		}
		
		return latestFile;
	}
	
	private void init() {
		
		// Scheduler to delete old forecastRecords
		long currMillis = System.currentTimeMillis();
		long refTime = DateUtils.getIntervalOfDay(currMillis, CacheConstants.NRSC_WEATHER_FORECAST_DELETE_OLD_HOUR, CacheConstants.NRSC_WEATHER_FORECAST_DELETE_OLD_MINUTE, 0);
		if(refTime < currMillis){
			refTime += DateUtils.TWENTY_FOUR_HOURS_IN_SECONDS;
		}
		long waitMillis = refTime - currMillis;
		Akka.system().scheduler().schedule(
                Duration.create(waitMillis, TimeUnit.MILLISECONDS),
                Duration.create(CacheConstants.NRSC_WEATHER_FORECAST_DELETE_OLD_TIME_INTERVAL, TimeUnit.HOURS),
                new Runnable() {
                    @Override
                    public void run() {
                    	/*
                    	 * TODO
                    	 */
                    	deleteOldWFRecords();
                    }
                }
          , Akka.system().dispatcher()
        );
		
		Akka.system().scheduler().schedule(
				Duration.create(CacheConstants.WAIT_TIME, TimeUnit.SECONDS),
				Duration.create(CacheConstants.NRSC_FILE_AUTOMATIC_UPLOAD_INTERVAL, TimeUnit.MINUTES), 
				new Runnable() {

					@Override
					public void run() {
						System.out.println("SoilMoistureFileUploadController auto updater : Starting automatic upload of data file.");
						try{
							System.out.println("SoilMoistureFileUploadController auto updater : Starting processing file at Time : " + System.currentTimeMillis());
							insertLatestFiles();
						}catch(Throwable t){
							System.out.println("SoilMoistureFileUploadController auto updater : Some exception happened while automatic file upload. Exception occured at : " + System.currentTimeMillis());
							t.printStackTrace();
						}
						
						System.out.println("SoilMoisture File uploadController auto updater : finished processing file from ftp.");
					}
				}, Akka.system().dispatcher());
		
		System.out.println("SoilMoisture File uploadController :: cache and scheduler initialized");
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		init();
	}

	/*public Result uploadFile() {

		JsonNode jsonData = request().body().asJson();
		System.out.println("jsonData in uploadFile():" + jsonData);

		MultipartFormData multipartFormData = request().body().asMultipartFormData();

		IInputParams inputParams = new InputParams();

		// String otherParameters = null;
		Map<String, String[]> formParams = multipartFormData.asFormUrlEncoded();
		// String processorName ="", locationType="";
		if (formParams != null) {
			String[] paramsValues = formParams.get(FILEUPLOAD_PAGE_OTHER_PARAMS);
			if (paramsValues != null && paramsValues.length == 1) {
				// otherParameters = paramsValues[0];
				System.out.println("paramsValue in uploadFile():" + paramsValues[0]);

				inputParams.setInputParamsData(paramsValues[0]);
				System.out.println("inputParams=" + inputParams);

			}
		}

		List<FilePart> files = multipartFormData.getFiles();
		for (Iterator iterator = files.iterator(); iterator.hasNext();) {
			FilePart filePart = (FilePart) iterator.next();
			filePart.getFilename();
			File file = filePart.getFile();
			System.out.println("filePart.getFilename():" + filePart.getFilename() + ";file:" + file);
			System.out.println("File Size " + file.getTotalSpace());

			List<String> errors = fileUploadService.processNRSCDataFromFile(filePart.getFilename(), file);
			if (!errors.isEmpty())
				return internalServerError(Json.toJson(errors));
			return ok();

		}
		return ok();
	}
	public Result uploadFTPFile(String filepath) {
		System.out.println("File Path : " + filepath);
		File file = new File(filepath);
		
		System.out.println("filePart.getFilename():" + file.getName() + ";file:" + file);
		System.out.println("File Size " + file.getTotalSpace());
		
		List<String> errors = fileUploadService.processNRSCDataFromFile(file.getName(), file);
		
		if (!errors.isEmpty())
		{
			System.out.println("Error in uploading");
			return internalServerError(Json.toJson(errors));
		}
		return ok();
		
	}


	public void getFromFTPServer(String serverUrl){
		int port = 21;
		String user = "ftptest";
		String pass = "ftptest";

		FTPClient ftpClient = new FTPClient();

		try {

			ftpClient.connect(serverUrl, port);
			showServerReply(ftpClient);

			int replyCode = ftpClient.getReplyCode();
			if (!FTPReply.isPositiveCompletion(replyCode)) {
				System.out.println("Connect failed");
				return;
			}

			boolean success = ftpClient.login(user, pass);
			showServerReply(ftpClient);

			if (!success) {
				System.out.println("Could not login to the server");
				return;
			}

			// Lists files and directories
			FTPFile[] files1 = ftpClient.listFiles("/ftp");
			printFileDetails(files1);
			// uses simpler methods
			String[] files2 = ftpClient.listNames();
			printNames(files2);

			
			 * For each file call upload method service
			 
			for (FTPFile ftpFile : files1) {
				InputStream iStream = ftpClient.retrieveFileStream(ftpFile.getName());
				File file = File.createTempFile("tmp", null);
				FileUtils.copyInputStreamToFile(iStream, file);
				//Calling file upload service
				System.out.println("File : " + file.getName());
				uploadFTPFile(file);
			}
			
			String[] files2 = ftpClient.listNames();
			
			for (String file : files2) {
				File f = new File("/home/ftptest/ftp/" + file);
				System.out.println("File : " + f.getAbsolutePath());
				uploadFTPFile(f.getAbsolutePath());
			}
			
		} catch (IOException ex) {
			System.out.println("Oops! Something wrong happened");
			ex.printStackTrace();
		} finally {
			// logs out and disconnects from server
			try {
				if (ftpClient.isConnected()) {
					ftpClient.logout();
					ftpClient.disconnect();
				}
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}

	private static void printFileDetails(FTPFile[] files) {
		DateFormat dateFormater = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		for (FTPFile file : files) {
			String details = file.getName();
			if (file.isDirectory()) {
				details = "[" + details + "]";
			}
			details += "\t\t" + file.getSize();
			details += "\t\t" + dateFormater.format(file.getTimestamp().getTime());

			System.out.println(details);
		}
	}

	private static void showServerReply(FTPClient ftpClient) {
		String[] replies = ftpClient.getReplyStrings();
		if (replies != null && replies.length > 0) {
			for (String aReply : replies) {
				System.out.println("SERVER: " + aReply);
			}
		}
	}*/
	public Result uploadFile() { 
		/*JsonNode jsonData = request().body().asJson();
		System.out.println("jsonData in uploadFile():" + jsonData);

		MultipartFormData multipartFormData = request().body().asMultipartFormData();

		IInputParams inputParams = new InputParams();


		Map<String, String[]> formParams = multipartFormData.asFormUrlEncoded();
		
		if (formParams != null) {
			String[] paramsValues = formParams.get(FILEUPLOAD_PAGE_OTHER_PARAMS);
			if (paramsValues != null && paramsValues.length == 1) {
				
				System.out.println("paramsValue in uploadFile():" + paramsValues[0]);
				inputParams.setInputParamsData(paramsValues[0]);
				System.out.println("inputParams=" + inputParams);
			}
		}

		List<FilePart> files = multipartFormData.getFiles();
		for (Iterator iterator = files.iterator(); iterator.hasNext();) {
			FilePart filePart = (FilePart) iterator.next();
			filePart.getFilename();
			File file = filePart.getFile();
			
			System.out.println("SoilMoistureFileUploadController : Filename = " + filePart.getFilename() +", File Size = " + file.getTotalSpace());
			System.out.println("SoilMoistureFileUploadController : File =" + file);
			
			List<String> info = new ArrayList<>();
			try {
				System.out.println("File path : "+file.getPath());
				info = fileUploadService.parseFile(file.getPath(), filePart.getFilename());
			} catch (DSPException | IOException e) {
				e.printStackTrace();
				return internalServerError(Json.toJson(info));
			}
			for(String i:info){
				System.out.println(" Info string "+i);
			}
			return ok(Json.toJson(info));
		}
		*/
		return ok();
	
	}
}
