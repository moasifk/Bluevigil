package com.bluecast.bluevigil.streaming;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.google.protobuf.ServiceException;
import com.bluecast.bluevigil.model.FieldMapping;
import com.bluecast.bluevigil.model.Mapping;

/**
 ** 
 * This program can be executed by following command from your local build jar
 * location 
 * spark-submit --master yarn --deploy-mode cluster --class com.bluecast.bluevigil.streaming.BluevigilStreamingProcessor  
 * spark-realtime-core-jar-with-dependencies.jar 
 * Yassar060Blue 
 * Yassar060BlueOutput 
 * nn01.itversity.com:6667,nn02.itversity.com:6667,rm01.itversity.com:6667 
 * nn01.itversity.com:2181,nn02.itversity.com:2181,rm01.itversity.com:2181
 * 
 */
public class BluevigilStreamingProcessor {//implements Runnable  {
	static Logger LOGGER = Logger.getLogger(BluevigilStreamingProcessor.class);
	private static Properties props=new Properties();
	private static String SOURCE_TOPIC;
	private static String DEST_TOPIC;
	private static String BOOTSTRAP_SERVERS;
	private static String ZOOKEEPER_SERVER;
	private static String NWLOG_FILE_CONFIG_PATH;
	
	public static void main(String args[]){
		SOURCE_TOPIC = args[0]; // Source topic - Flume ingest logs to this topic
		DEST_TOPIC = args[1]; // The spark processed data kept in this topic
		BOOTSTRAP_SERVERS = args[2]; // Bootstrap server details, comma seperated
		ZOOKEEPER_SERVER = args[3]; // Zookeeper server details, comma seperated
		NWLOG_FILE_CONFIG_PATH = args[4];
		//parsing filled mapping JSON file
		BluevigilConsumer consumer = new BluevigilConsumer();
		// SparkConf conf = new SparkConf().setAppName("BluevigilStreamingProcessor").setMaster("local[*]");
		SparkConf conf = new SparkConf().setAppName("BluevigilStreamingProcessor");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));
		Gson gson = new Gson();
//		final File folder = new File("./properties");
		JsonReader reader;
		try {
			reader = new JsonReader(new FileReader(NWLOG_FILE_CONFIG_PATH));
		
		FieldMapping mappingData = gson.fromJson(reader,FieldMapping.class); 
		LOGGER.info("Going to call h-base consumeDataFromSource method");
		consumer.consumeDataFromSource(SOURCE_TOPIC, DEST_TOPIC, BOOTSTRAP_SERVERS, ZOOKEEPER_SERVER, jssc,mappingData);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			LOGGER.error(e.getMessage());
		}
		//List<Mapping> fieldMappingList=mappingData.getMapping();
		//Iterator<Mapping> it=fieldMappingList.iterator();	
	}
	

}

