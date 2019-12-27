package com.java.sparkGround.SparkTutorial.LogParsing;

import java.io.Serializable;



public class LogDataPojo implements Serializable,Comparable<LogDataPojo>{
    private static final long serialVersionUID = -2685444218382696366L;
	private String host;
	private String timeStamp;
	private String url;
	private int httpCode;
	
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public String getTimeStamp() {
		return timeStamp;
	}
	public void setTimeStamp(String timeStamp) {
		this.timeStamp = timeStamp;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public int getHttpCode() {
		return httpCode;
	}
	public void setHttpCode(int httpCode) {
		this.httpCode = httpCode;
	}
	public int compareTo(LogDataPojo arg0) {
		return Double.compare(this.httpCode,arg0.getHttpCode());
		
	}

}
