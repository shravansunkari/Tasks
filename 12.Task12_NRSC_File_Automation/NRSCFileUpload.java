package com.vassarlabs.iwm.soilmoisture.stress.pojo.impl;

import com.vassarlabs.common.pojo.api.AVLObject;
import com.vassarlabs.iwm.soilmoisture.stress.pojo.api.INRSCFileUpload;

public class NRSCFileUpload
	extends AVLObject
	implements INRSCFileUpload {
	
	protected long fileUploadId;
	protected String fileName;
	protected long fileUploadTS;
	protected int noOfEntries;
	protected short isForecasted;
	protected long lastModified;
	
	public NRSCFileUpload() {
		super();
	}

	public NRSCFileUpload(long fileUploadId, String fileName, long fileUploadTS, int noOfEntries, short isForecasted,
			long lastModified) {
		super();
		this.fileUploadId = fileUploadId;
		this.fileName = fileName;
		this.fileUploadTS = fileUploadTS;
		this.noOfEntries = noOfEntries;
		this.isForecasted = isForecasted;
		this.lastModified = lastModified;
	}



	@Override
	public long getFileUploadId() {
		return fileUploadId;
	}

	@Override
	public void setFileUploadId(long fileUploadId) {
		this.fileUploadId = fileUploadId;
	}

	@Override
	public String getFileName() {
		return fileName;
	}

	@Override
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	@Override
	public long getFileUploadTS() {
		return fileUploadTS;
	}

	@Override
	public void setFileUploadTS(long fileUploadTS) {
		this.fileUploadTS = fileUploadTS;
	}

	@Override
	public int getNoOfEntries() {
		return noOfEntries;
	}

	@Override
	public void setNoOfEntries(int noOfEntries) {
		this.noOfEntries = noOfEntries;
	}

	@Override
	public short getIsForecasted() {
		return isForecasted;
	}

	@Override
	public void setIsForecasted(short isForecasted) {
		this.isForecasted = isForecasted;
		
	}

	@Override
	public long getLastModified() {
		return lastModified;
	}

	@Override
	public void setLastModified(long lastModified) {
		this.lastModified = lastModified;
		
	}

	@Override
	public String toString() {
		return "NRSCFileUpload [fileUploadId=" + fileUploadId + ", fileName=" + fileName + ", fileUploadTS="
				+ fileUploadTS + ", noOfEntries=" + noOfEntries + ", isForecasted=" + isForecasted + ", lastModified="
				+ lastModified + "]";
	}
	
}
