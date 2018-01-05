package com.bluecast.bluevigil.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

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
public class BluevigilStreamingProcessor {

	private static String SOURCE_TOPIC;
	private static String DEST_TOPIC;
	private static String BOOTSTRAP_SERVERS;
	private static String ZOOKEEPER_SERVER;

	public static void main(String args[]) {
		SOURCE_TOPIC = args[0]; // Source topic - Flume ingest logs to this topic
		DEST_TOPIC = args[1]; // The spark processed data kept in this topic
		BOOTSTRAP_SERVERS = args[2]; // Bootstrap server details, comma seperated
		ZOOKEEPER_SERVER = args[3]; // Zookeeper server details, comma seperated

		BluevigilConsumer consumer = new BluevigilConsumer();
		SparkConf conf = new SparkConf().setAppName("BluevigilStreamingProcessor"); //.setMaster("local[*]");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));
		consumer.consumeDataFromSource(SOURCE_TOPIC, DEST_TOPIC, BOOTSTRAP_SERVERS, ZOOKEEPER_SERVER, jssc);
	}

}
