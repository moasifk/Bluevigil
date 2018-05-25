package com.bluevigil.streaming;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

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
	private static String NWLOG_FILE_TYPE;

	public static void main(String args[]) {
		LOGGER.info("BluevigilProcessor - main method execution started");
		props = BluevigilProperties.getInstance();
		// Config file path
		NWLOG_FILE_TYPE ="http";// args[0];
		// parsing filled mapping JSON file
		BluevigilConsumer bluevigilConsumer = new BluevigilConsumer();
		SparkConf conf;
		conf = new SparkConf().setAppName(
				props.getProperty(BluevigilConstant.APPLICATION_NAME) + BluevigilConstant.UNDERSCORE + SOURCE_TOPIC);
		JavaSparkContext jsc = new JavaSparkContext(conf);
		// Get input data config details
		//JavaRDD<String> lines = jsc.textFile(NWLOG_FILE_CONFIG_PATH);
		//StringBuilder inputJson = new StringBuilder();
		//LOGGER.info("Reading input config json file : ");
		//for (String line : lines.collect()) {
		//	inputJson.append(line);
		//}
		
		LOGGER.info("Config file details: " + getJsonConfig(NWLOG_FILE_TYPE));
		bluevigilConsumer.consumeDataFromSource(jsc, getJsonConfig(NWLOG_FILE_TYPE));
		jsc.close();
		LOGGER.info("BluevigilProcessor - main method execution over");
	}
	public static String getJsonConfig(String fileType) {
		String output;
		try {
				URL url = new URL(props.getProperty("bluevigil.postgresql.rest.api.getConfigJson"+fileType));
				HttpURLConnection conn = (HttpURLConnection) url.openConnection();
				conn.setRequestMethod("GET");
				conn.setRequestProperty("Accept", "application/json");
				if (conn.getResponseCode() != 200) {
					throw new RuntimeException("Failed : HTTP error code : "
							+ conn.getResponseCode());					
				}
				BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));				
				LOGGER.info("Output from Server .... \n");
				output = br.readLine();
				/*while (br.readLine() != null) {
					System.out.println(output);
				}*/
				conn.disconnect();
			  } catch (MalformedURLException e) {
				  LOGGER.error(e.getMessage());
				return null;

			  } catch (IOException e) {
				  LOGGER.error(e.getMessage());
				return null;
			  }
		return output;
	}

}
