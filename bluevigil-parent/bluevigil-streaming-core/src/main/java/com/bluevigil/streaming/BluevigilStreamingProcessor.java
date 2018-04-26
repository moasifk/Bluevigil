package com.bluevigil.streaming;

import java.io.FileNotFoundException;
import java.io.FileReader;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.bluevigil.model.LogFileConfig;
import com.bluevigil.utils.BluevigilConstant;
import com.bluevigil.utils.BluevigilProperties;
import com.bluevigil.utils.Utils;
import com.google.gson.Gson;

/**
 ** 
 * This program can be executed by following command from your local build jar
 * location spark-submit --master yarn --deploy-mode cluster --class
 * com.bluecast.bluevigil.streaming.BluevigilStreamingProcessor
 * spark-realtime-core-jar-with-dependencies.jar Yassar060Blue
 * Yassar060BlueOutput
 * nn01.itversity.com:6667,nn02.itversity.com:6667,rm01.itversity.com:6667
 * nn01.itversity.com:2181,nn02.itversity.com:2181,rm01.itversity.com:2181
 * 
 */
public class BluevigilStreamingProcessor {// implements Runnable {
	static Logger LOGGER = Logger.getLogger(BluevigilStreamingProcessor.class);
	private static BluevigilProperties props = BluevigilProperties.getInstance();
	private static String SOURCE_TOPIC;
	private static String DEST_TOPIC;
	private static String BOOTSTRAP_SERVERS;
	private static String ZOOKEEPER_SERVER;
	private static String NWLOG_FILE_CONFIG_PATH;

	public static void main(String args[]) {
		// Below argument values are populated based on the input logs we have
		// to push to hadoop
		// Source topic - Flume ingest logs to this topic eg: dns-input-topic,
		// http-input-topic
		SOURCE_TOPIC = args[0];
		// The spark processed data kept in this topic eg: eg: dns-output-topic,
		// http-output-topicargs
		DEST_TOPIC = args[1];
		// Network log configuration(json) file path eg:
		// /user/bluvigil/configs/Http_file_config.json
		NWLOG_FILE_CONFIG_PATH = args[2];
		// parsing filled mapping JSON file
		BluevigilConsumer consumer = new BluevigilConsumer();
		// SparkConf conf = new
		// SparkConf().setAppName("BluevigilStreamingProcessor").setMaster("local[*]");
		SparkConf conf = new SparkConf().setAppName(
				props.getProperty("bluevigil.application.name") + BluevigilConstant.UNDERSCORE + SOURCE_TOPIC).setMaster("local[*]");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(30));
		Gson gson = new Gson();
		FileReader reader;
		try {
			reader = new FileReader(NWLOG_FILE_CONFIG_PATH);
			LogFileConfig mappingData = gson.fromJson(reader, LogFileConfig.class);
			Utils.createHbaseTable(mappingData.getHbaseTable(), props.getProperty("bluevigil.hbase.column.family.name.primary"));
			LOGGER.info("Going to call h-base consumeDataFromSource method");
			consumer.consumeDataFromSource(SOURCE_TOPIC, DEST_TOPIC, jssc, mappingData);
		} catch (FileNotFoundException e) {
			LOGGER.error(e.getMessage());
		}
	}

}
