package com.bluevigil.streaming;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.bluevigil.utils.BluevigilConstant;
import com.bluevigil.utils.BluevigilProperties;

/**
 ** 
 * This program can be executed by following command from your local build jar
 * location spark-submit --master yarn --deploy-mode cluster --class
 * com.bluevigil.streaming.BluevigilProcessor bluevigil-streaming-core.jar 
 * /user/<userName>/http_config.json
 * 
 */

public class BluevigilProcessor {
	static Logger LOGGER = Logger.getLogger(BluevigilProcessor.class);
	private static BluevigilProperties props;
	private static String SOURCE_TOPIC;
	private static String DEST_TOPIC;
	private static String NWLOG_FILE_CONFIG_PATH;

	public static void main(String args[]) {
		LOGGER.info("BluevigilProcessor - main method execution started");
		props = BluevigilProperties.getInstance();
		// Config file path
		NWLOG_FILE_CONFIG_PATH = args[0];
		// parsing filled mapping JSON file
		BluevigilConsumer bluevigilConsumer = new BluevigilConsumer();
		SparkConf conf;
		conf = new SparkConf().setAppName(
				props.getProperty(BluevigilConstant.APPLICATION_NAME) + BluevigilConstant.UNDERSCORE + SOURCE_TOPIC);
		JavaSparkContext jsc = new JavaSparkContext(conf);
		// Get input data config details
		JavaRDD<String> lines = jsc.textFile(NWLOG_FILE_CONFIG_PATH);
		StringBuilder inputJson = new StringBuilder();
		LOGGER.info("Reading input config json file : ");
		for (String line : lines.collect()) {
			inputJson.append(line);
		}
		
		LOGGER.info("Config file details: " + inputJson.toString());
		bluevigilConsumer.consumeDataFromSource(jsc, inputJson.toString());
		jsc.close();
		LOGGER.info("BluevigilProcessor - main method execution over");
	}

}
