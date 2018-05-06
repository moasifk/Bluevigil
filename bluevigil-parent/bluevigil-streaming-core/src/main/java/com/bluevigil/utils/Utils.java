package com.bluevigil.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;

public class Utils implements Serializable {
	static transient Logger LOGGER = Logger.getLogger(Utils.class);
	// private static Properties props=new Properties();
	private static BluevigilProperties props;

	/*
	 * To check Hbase table exists or not
	 */
	public static boolean isHbaseTableExists(String hbaseTableName) {
		LOGGER.info("Util - isHbaseTableExists started");
		props = BluevigilProperties.getInstance();
		boolean isExists = true;
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", props.getProperty(BluevigilConstant.ZOOKEEPER_QUORUM));
		conf.set("hbase.zookeeper.property.clientPort", props.getProperty(BluevigilConstant.ZOOKEEPER_PORT));
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");
		try {
			HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
			isExists = hbaseAdmin.tableExists(hbaseTableName);
		} catch (IOException e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage());
		}
		LOGGER.info("Util - isHbaseTableExists end");
		return isExists;
	}

	public static Properties getProperties() {
		Properties prop = new Properties();
		InputStream input = null;
		try {

			input = new FileInputStream("./properties/config.properties");

			// load a properties file
			prop.load(input);

			return prop;

		} catch (IOException ex) {
			ex.printStackTrace();
			LOGGER.error(ex.getMessage());
			return null;
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
					LOGGER.error(e.getMessage());
				}
			}
		}
	}

	public static String getTime(long ts) {
		Date dateTime = new Date(ts * 1000L);
		String date, time;
		SimpleDateFormat jdf = new SimpleDateFormat("hh:mm");
		time = jdf.format(dateTime);
		// System.out.println("Time="+time);
		return time;
	}

	public static java.sql.Date getDate(long ts) {

		java.sql.Date sqlDate = new java.sql.Date(ts * 1000L);
		// System.out.println("SQL Date="+sqlDate);

		return sqlDate;
	}

	public static long getUnixTime() {
		long unixTime = 0;
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		dateFormat.setTimeZone(TimeZone.getTimeZone("GMT+5:30")); // Specify
																	// your
																	// timezone
		unixTime = date.getTime();
		// unixTime = unixTime / 1000;
		System.out.println("Unix time=" + unixTime);
		return unixTime;
	}

	/**
	 * Method for creating a standard column filed name from the json key
	 * 
	 * @param fieldName
	 *            json key name
	 * @param length
	 *            Maximum length to split the field
	 * @return Formatted hbase field name
	 */
	public static String createBluevigilFieldName(String fieldName, int length) {
		StringBuffer bluevigilField = new StringBuffer();
		props = BluevigilProperties.getInstance();
		String connector = props.getProperty("bluevigil.hbase.column.qualifier.connector");
		for (String fieldPart : fieldName.trim().replaceAll("[^a-zA-Z0-9]+", connector).split(connector)) {
			if (!fieldPart.isEmpty() && !fieldPart.equals(connector)) {
				bluevigilField.append(fieldPart.substring(0, Math.min(fieldPart.length(), length))).append(connector);
			}
		}
		return bluevigilField.substring(0, bluevigilField.length() - 1).toString();
	}

	/**
	 * Method for generating a full line with all column fields separated by a
	 * comma
	 * 
	 * @param separator
	 * @param parsedJson
	 * @param backenFieldMap
	 * @return
	 */
	public static String createLineFromParsedJson(String separator, Map<String, String> parsedJson,
			Map<Integer, String> backenFieldMap) {
		StringBuilder outputLine = new StringBuilder();
		String value;
		for (int key : backenFieldMap.keySet()) {
			value = parsedJson.get(backenFieldMap.get(key));
			outputLine.append((value != null ? value : BluevigilConstant.EMPTY_STRING) + separator);
		}
		// Remove the last comma separator
		return outputLine.substring(0, outputLine.length() - 1).toString();
	}
}
