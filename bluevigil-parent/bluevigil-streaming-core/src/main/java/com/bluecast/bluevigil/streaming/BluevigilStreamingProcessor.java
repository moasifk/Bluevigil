package com.bluecast.bluevigil.streaming;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

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

	private static String SOURCE_TOPIC;
	private static String DEST_TOPIC;
	private static String BOOTSTRAP_SERVERS;
	private static String ZOOKEEPER_SERVER;

	public static void main(String args[]) throws IOException, ServiceException {
		SOURCE_TOPIC ="Yassar060Blue"; //args[0]; // Source topic - Flume ingest logs to this topic
		DEST_TOPIC = "Yassar060BlueOutput";//args[1]; // The spark processed data kept in this topic
		BOOTSTRAP_SERVERS = "localhost:9092";//args[2]; // Bootstrap server details, comma seperated
		ZOOKEEPER_SERVER ="localhost:2181";//args[3]; // Zookeeper server details, comma seperated
		//parsing filled mapping JSON file
		BluevigilConsumer consumer = new BluevigilConsumer();
		SparkConf conf = new SparkConf().setAppName("BluevigilStreamingProcessor").setMaster("local[*]");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));
		Gson gson = new Gson();
		final File folder = new File("./properties");
		/*for (final File fileEntry : folder.listFiles()) {
			if(fileEntry.getName().endsWith(".json"))
			{
				JsonReader reader = new JsonReader(new FileReader("./properties/"+fileEntry.getName()));
				FieldMapping mappingData = gson.fromJson(reader,FieldMapping.class); 
				System.out.println("Delimitter="+mappingData.getDelimitter());
				consumer.consumeDataFromSource(SOURCE_TOPIC, DEST_TOPIC, BOOTSTRAP_SERVERS, ZOOKEEPER_SERVER, jssc,mappingData);
				
			}
		}*/
		JsonReader reader = new JsonReader(new FileReader("./properties/httpFields.json"));
		FieldMapping mappingData = gson.fromJson(reader,FieldMapping.class); 
		System.out.println("going to call h-base scan method");
		consumer.consumeDataFromSource(SOURCE_TOPIC, DEST_TOPIC, BOOTSTRAP_SERVERS, ZOOKEEPER_SERVER, jssc,mappingData);
		try {
			consumer.getHbaseData();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//List<Mapping> fieldMappingList=mappingData.getMapping();
		//Iterator<Mapping> it=fieldMappingList.iterator();		
		
		
		
	}
	/*public void run() 
	{
		BluevigilConsumer consumer = new BluevigilConsumer();
		SparkConf conf = new SparkConf().setAppName("BluevigilStreamingProcessor").setMaster("local[*]");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));
		Gson gson = new Gson();
		final File folder = new File("./properties");
		/*for (final File fileEntry : folder.listFiles()) {
			if(fileEntry.getName().endsWith(".json"))
			{
				JsonReader reader = new JsonReader(new FileReader("./properties/"+fileEntry.getName()));
				FieldMapping mappingData = gson.fromJson(reader,FieldMapping.class); 
				System.out.println("Delimitter="+mappingData.getDelimitter());
				consumer.consumeDataFromSource(SOURCE_TOPIC, DEST_TOPIC, BOOTSTRAP_SERVERS, ZOOKEEPER_SERVER, jssc,mappingData);
				
			}
		}*/
		/*JsonReader reader;
		try {
			reader = new JsonReader(new FileReader("./properties/httpFields.json"));
		
		FieldMapping mappingData = gson.fromJson(reader,FieldMapping.class); 
		System.out.println("Delimitter="+mappingData.getDelimitter());
		consumer.consumeDataFromSource(SOURCE_TOPIC, DEST_TOPIC, BOOTSTRAP_SERVERS, ZOOKEEPER_SERVER, jssc,mappingData);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}*/

}

