package com.java.sparkGround.SparkTutorial.LogParsing;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class LogFiles implements Serializable{
	/**
	 * @throws AnalysisException 
	 * 
	 */
	
	public static void main(String args[]) throws AnalysisException{
		System.out.println("Configuring Spark Configuartion");
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf objSparkConf = new SparkConf().setAppName("TrendAnalyticsMapper").setMaster("local[1]");
		objSparkConf.set("spark.sql.crossJoin.enabled", "true");
		JavaSparkContext objJavaSparkContext = new JavaSparkContext(objSparkConf);
		SparkSession objSparkSession = SparkSession.builder().appName("Java Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate();
		LogFiles objLogFiles=new LogFiles();
		objLogFiles.createRDDStrorage(objSparkConf, objJavaSparkContext, objSparkSession);
	}
	private static final long serialVersionUID = 1L;
	Pattern p1 = Pattern.compile("^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+)(.*)\" (\\d{3}) (\\S+)");
	public  void createRDDStrorage(SparkConf objSparkConf,
			JavaSparkContext objJavaSparkContext, SparkSession objSparkSession) throws AnalysisException {
		System.out.println("Parsing Log Files");
		JavaRDD<String> objFileJavaRDD = objSparkSession.sparkContext()
				.textFile("NasaData/NASA_access_log_Aug95.gz", 1).toJavaRDD();
           
          //  System.out.println("Total Data  :- " +objFileJavaRDD.count());
            
            JavaRDD<LogDataPojo> newData = objFileJavaRDD.map(new Function<String, LogDataPojo>() {
            	public LogDataPojo call(String line) {
            		String s="";
            		Matcher m = p1.matcher(line);
            		LogDataPojo objLogDataPojo=new LogDataPojo();
            		while (m.find()) {
            			
            	       /* System.out.println(m.group(1));
            	        System.out.println(m.group(4));
            	        System.out.println(m.group(6));
            	        System.out.println(m.group(8));*/
            	     // s="{"+m.group(1)+"{"+m.group(4)+"{"+m.group(6)+"{"+m.group(8);
            			objLogDataPojo.setHost(m.group(1));
            			objLogDataPojo.setTimeStamp(m.group(4));
            			objLogDataPojo.setUrl(m.group(6));
            			objLogDataPojo.setHttpCode(Integer.valueOf(m.group(8)));
            			
            	     }            	
            		return objLogDataPojo;
            	}
            	});
          /* List<LogDataPojo> abc=newData.top(5);
           for(LogDataPojo obj:abc){
        	   System.out.println(obj.getHost());
        	   System.out.println(obj.getHttpCode());
           }*/
          //Create DataFrame .
            List<StructField> lstSchemaField = new ArrayList<StructField>();
            
            StructField structHost = DataTypes.createStructField("host",DataTypes.StringType, true);
			lstSchemaField.add(structHost);
			StructField structTimeStamp = DataTypes.createStructField("timeStamp",DataTypes.StringType, true);
			lstSchemaField.add(structTimeStamp);
			StructField structUrl = DataTypes.createStructField("url",DataTypes.StringType, true);
			lstSchemaField.add(structUrl);
			StructField structHttpCode = DataTypes.createStructField("httpCode",DataTypes.StringType, true);
			lstSchemaField.add(structHttpCode);
			StructType structSchema = DataTypes.createStructType(lstSchemaField);
			
			// Convert records of the RDD (RecordData) to Rows
			JavaRDD<Row> objRecordJavaRDD = newData.map(new Function<LogDataPojo, Row>() {
				/**
				 * 
				 */
				private static final long serialVersionUID = 5900148082341514523L;

				public Row call(LogDataPojo sRecord) throws Exception {

					String[] sAttributes = null;
					ArrayList<String> sArr = new ArrayList<String>();
					sArr.add(sRecord.getHost());
					sArr.add(sRecord.getTimeStamp());
					sArr.add(sRecord.getUrl());
					sArr.add(String.valueOf(sRecord.getHttpCode()));
							
						
					sAttributes = new String[sArr.size()];
					sAttributes = sArr.toArray(sAttributes);
					return RowFactory.create(sAttributes);
				}
			});
			Dataset<Row> objDatasetToDataFrame = objSparkSession.createDataFrame(objRecordJavaRDD, structSchema);
			
			objDatasetToDataFrame.createOrReplaceTempView("nasa_log");
			Dataset<Row> nasalogTable=objSparkSession.sql("SELECT * FROM nasa_log");
			nasalogTable.cache();
			nasalogTable.show();
			System.out.println("Write spark code to find out top 10 requested URLs along with a count of the number of times they have been requested (This information will help the company to find out most popular pages and how frequently they are accessed)");
			objSparkSession.sql("select url,count(*) as req_cnt from nasa_log where upper(url) like '%HTML%' group by url order by req_cnt desc LIMIT 10").show();
			
			System.out.println("Write spark code to find out the top five-time frame for high traffic (which day of the week or hour of the day receives peak traffic, this information will help the company to manage resources for handling peak traffic load)");
			objSparkSession.sql("select substr(timeStamp,1,14) as timeFrame,count(*) as req_cnt from nasa_log group by substr(timeStamp,1,14) order by req_cnt  LIMIT 5").show();
			//objSparkSession.sql("select host,count(*) as req_cnt from nasa_log group by host order by req_cnt desc LIMIT 5").show();
			
			System.out.println("Write spark code to find out top 5 time frames of least traffic (which day of the week or hour of the day receives least traffic, this information will help the company to do production deployment in that time frame so that less number of users will be affected if something goes wrong during deployment)");
			objSparkSession.sql("select substr(timeStamp,1,14) as timeFrame,count(*) as req_cnt from nasa_log group by substr(timeStamp,1,14) order by req_cnt desc LIMIT 5").show();
			
			
			
			System.out.println("Write Spark code to find out unique HTTP codes returned by the server along with count (this information is helpful for DevOps team to find out how many requests are failing so that appropriate action can be taken to fix the issue)");
			objSparkSession.sql("select httpCode,count(*) as req_cnt from nasa_log group by httpCode ").show();
			
           
	}    
}
