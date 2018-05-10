package com.bluevigil.utils;

import java.io.IOException;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;


public class BluevigilUtils implements Serializable {
	static transient Logger LOGGER = Logger.getLogger(BluevigilUtils.class);
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

	public static String getTime(long ts) {
		Date dateTime = new Date(ts * 1000L);
		String time;
		SimpleDateFormat jdf = new SimpleDateFormat("hh:mm");
		time = jdf.format(dateTime);
		return time;
	}
	
	public static String getDate(long ts) {
		java.sql.Date sqlDate = new java.sql.Date(ts*1000L);
		return sqlDate.toString();
	}
	
	public static  long getUnixTime() {
        long unixTime = 0;
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT+5:30")); //Specify your timezone
        unixTime = date.getTime();
        return unixTime;
    }
	
	public static  String getCurrentTime() {
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    	Calendar cal = Calendar.getInstance();
        return dateFormat.format(cal);
    }
	
	public static String getIpResolveCountry(String  ipAddress) throws IOException {
		props = BluevigilProperties.getInstance();
		IpResolveCountry ipc=new IpResolveCountry(props.getProperty("mmdb.geoLocation.Country"));
		Tuple tuple= TupleFactory.getInstance().newTuple();//("162.168.1.1");
		tuple.append(ipAddress);
		String country= ipc.exec(tuple);
		return country;
	}
	
	public static String getIpResolveCity(String  ipAddress) throws IOException {
		props = BluevigilProperties.getInstance();
		IpResolveCity ipCity=new IpResolveCity(props.getProperty("mmdb.geoLocation.City"));
		Tuple cityTuple= TupleFactory.getInstance().newTuple();//("162.168.1.1");
		cityTuple.append(ipAddress);
		return ipCity.exec(cityTuple);
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
	
	/**
	 * Method to convert String ipAddress to long value
	 * @param ipAddress
	 * @return
	 */
	public static long ipToLong(String ipAddress) {
		String[] ipAddressInArray = ipAddress.split("\\.");
		long result = 0;
		for (int i = 0; i < ipAddressInArray.length; i++) {
			int power = 3 - i;
			int ip = Integer.parseInt(ipAddressInArray[i]);
			result += ip * Math.pow(256, power);
		}
		return result;
	}
	
	public static String formatJsonValue(String jsonValue) {
		while (jsonValue.startsWith("\"") || jsonValue.startsWith("[") || jsonValue.endsWith("\"") || jsonValue.endsWith("]")) {
			if (jsonValue.startsWith("\"") || jsonValue.startsWith("[")) {
				jsonValue = jsonValue.substring(1,jsonValue.length()); 
			}
			if (jsonValue.endsWith("\"") || jsonValue.endsWith("]")){
				jsonValue = jsonValue.substring(0,jsonValue.length()-1); 
			}
		}
		return jsonValue;
	}
}
