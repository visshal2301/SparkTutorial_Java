package com.java.sparkGround.SparkTutorial.jsonParsing;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class TwitterSentimentAnalysis {
	
	public static void main(String args[]) throws AnalysisException{
		System.out.println("Configuring Spark Configuartion");
		Logger.getLogger("org").setLevel(Level.ERROR);
		SparkConf objSparkConf = new SparkConf().setAppName("TrendAnalyticsMapper").setMaster("local[1]");
		objSparkConf.set("spark.sql.crossJoin.enabled", "true");
		JavaSparkContext objJavaSparkContext = new JavaSparkContext(objSparkConf);
		SparkSession objSparkSession = SparkSession.builder().appName("Java Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate();
		TwitterSentimentAnalysis objLogFiles=new TwitterSentimentAnalysis();
		objLogFiles.createRDDStrorage(objSparkConf, objJavaSparkContext, objSparkSession);
	}
		public  void createRDDStrorage(SparkConf objSparkConf,
			JavaSparkContext objJavaSparkContext, SparkSession objSparkSession) throws AnalysisException {

		Dataset<Row> tweets_df = objSparkSession.read().json("Twitter/");
		tweets_df.createOrReplaceTempView("tweets_raw");
		tweets_df.show();
		 Dataset<Row> dict_df = objSparkSession.read()
				 .format("com.databricks.spark.csv")
                 .option("header", "true")
                 .option("delimiter","\t")
                 .csv("Twitter/dictionary/dictionary.tsv");
		dict_df.createOrReplaceTempView("dictionary");
		dict_df.show();
		
		 Dataset<Row> timezone_df = objSparkSession.read()
				 .format("com.databricks.spark.csv")
                 .option("header", "true")
                 .option("delimiter","\t")
                 .csv("Twitter/TimeZone/time_zone_map.tsv");
		 timezone_df.createOrReplaceTempView("time_zone_map");
		 timezone_df.show();
		
		
		System.out.println("How many records are in _tweets_raw_ table?");
		objSparkSession.sql("SELECT count(id) FROM tweets_raw").show();
		System.out.println("How many rows are there in _dictionary_ table?");
		objSparkSession.sql("SELECT count(*) FROM dictionary").show();
		System.out.println("what is the polarity of word ARCHIDNESS in _dictionary_ table?Polarity means if the word has positive, negative, or neutral sentiment");
		objSparkSession.sql("select polarity from dictionary where word='acridness'").show();
		System.out.println("What is the time zone of country FINLAND?");
		objSparkSession.sql("select time_zone from time_zone_map where country='FINLAND'").show();
		
		 Dataset<Row> tweets_simple_df =objSparkSession.sql("SELECT id, cast ( from_unixtime( unix_timestamp(concat( '2013 ', substring(created_at,5,15)), 'yyyy MMM dd hh:mm:ss')) as timestamp) ts,text,user.time_zone FROM tweets_raw");
		 tweets_simple_df.show();
		 tweets_simple_df.createOrReplaceTempView("tweets_simple");
		 Dataset<Row> tweets_clean_df =objSparkSession.sql("SELECT id,ts,text,m.country as country FROM tweets_simple t LEFT OUTER JOIN time_zone_map m ON t.time_zone = m.time_zone");
		 tweets_clean_df.show();
		 tweets_clean_df.createOrReplaceTempView("tweets_clean");
		 
		 System.out.println("From which country tweet with id 330044004693598208 was tweeted?");
		 objSparkSession.sql("select country from tweets_clean where id='330044004693598208'").show();
		 
		 Dataset<Row> l1_df= objSparkSession.sql("select id, words from tweets_raw lateral view explode(sentences(lower(text))) dummy as words");
		 l1_df.show();
		 l1_df.createOrReplaceTempView("l1");
		 
		 Dataset<Row> l2_df= objSparkSession.sql("select id, word from l1 lateral view explode( words ) dummy as word");
		 l2_df.show();
		 l2_df.createOrReplaceTempView("l2");
		 
		 Dataset<Row> l3_df= objSparkSession.sql("select id, l2.word,case d.polarity when  'negative' then -1 when 'positive' then 1 else 0 end as polarity from l2 left outer join dictionary d on l2.word = d.word");
		 l3_df.show();
		 l3_df.createOrReplaceTempView("l3");
		 
		 System.out.println("What is the polarity of word CRUSHES");
		 
		 objSparkSession.sql("select distinct(polarity) as polarity from l3 where word='crushes'").show();
		 
		 Dataset<Row> tweets_sentiment_df= objSparkSession.sql("select id,case when sum( polarity ) > 0 then 'positive'  when sum( polarity ) < 0 then 'negative'  else 'neutral' end as sentiment from l3 group by id");
		 tweets_sentiment_df.show();
		 tweets_sentiment_df.createOrReplaceTempView("tweets_sentiment");
		 
		 System.out.println("What is the sentiment of tweet with id as 330043911940751360?");
		 objSparkSession.sql("select sentiment from tweets_sentiment where id='330043911940751360'").show();
		 
		 Dataset<Row> tweetsbi_sentiment_df= objSparkSession.sql("SELECT t.*,s.sentiment FROM tweets_clean t LEFT OUTER JOIN tweets_sentiment s on t.id = s.id");
		 tweetsbi_sentiment_df.show();
		 tweetsbi_sentiment_df.createOrReplaceTempView("tweetsbi");
		 
		 objSparkSession.sql("select country,sentiment from tweetsbi where id='330043924896968707'").show();
	}

}
