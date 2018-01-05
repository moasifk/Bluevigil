package com.bluecast.bluevigil.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 ** 
 * This program can be executed by following command from your local build jar
 * location spark-submit --master yarn --deploy-mode cluster --class
 * com.bluecast.bluevigil.streaming.BluevigilStreamingProcessor
 * spark-realtime-core-jar-with-dependencies.jar localhost:9092 localhost:2181
 * SparkTest
 * 
 */
public class BluevigilStreamingProcessor {

	private static String SOURCE_TOPIC;
	private static String DEST_TOPIC;
	private static String BOOTSTRAP_SERVERS;
	private static String ZOOKEEPER_SERVER;

	public static void main(String args[]) {
		SOURCE_TOPIC = args[0];
		DEST_TOPIC = args[1];
		BOOTSTRAP_SERVERS = args[2];
		ZOOKEEPER_SERVER = args[3];

		BluevigilConsumer consumer = new BluevigilConsumer();
		SparkConf conf = new SparkConf().setAppName("BluevigilStreamingProcessor"); //.setMaster("local[*]");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));
		consumer.consumeDataFromSource(SOURCE_TOPIC, DEST_TOPIC, BOOTSTRAP_SERVERS, ZOOKEEPER_SERVER, jssc);
	}

}
