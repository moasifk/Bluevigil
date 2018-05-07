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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.TimeZone;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.joda.time.DateTime;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class Utils implements Serializable{
	static transient Logger LOGGER = Logger.getLogger(Utils.class);
	private static Properties props=new Properties();
	private static BluevigilProperties propss = BluevigilProperties.getInstance();
	public static Producer<String, String> createProducer(String bootstrapServers) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "BluevigilDev");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return new KafkaProducer<String, String>(props);
	}
	
	
	public static Connection getHbaseConnection() 
	{
		props=getProperties();
		try 
		{
			Class.forName(props.getProperty("phoenix.jdbc.driver"));
		} 
		catch (ClassNotFoundException e1) 
		{
			System.out.println("Exception Loading Driver");
			e1.printStackTrace();
			LOGGER.error(e1.getMessage());
			return null;
		}
		try
		{
			Connection con = DriverManager.getConnection(props.getProperty("phoenix.jdbc.url"));  //172.31.124.43 is the adress of VM, not needed if ur running the program from vm itself
			System.out.println("Connection Established");
			return con;
		}
		catch(SQLException e)
		{
			System.out.println(e.getMessage());
			LOGGER.error(e.getMessage());
			return null;
		}
	}
	public static  void createHbaseTable(String hbaseTableName,String hbaseTableColumnFamily) {
		Configuration conf=HBaseConfiguration.create();
		/*conf.set("hbase.zookeeper.quorum", "localhost");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent","/hbase-unsecure");	*/
		try {
		HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
			if(!hbaseAdmin.tableExists(hbaseTableName)){
				LOGGER.info("Hbase table "+hbaseTableName+" not available");
				HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName));
			    HColumnDescriptor columnDescriptor = new HColumnDescriptor(hbaseTableColumnFamily);
			    tableDescriptor.addFamily(columnDescriptor);
			    hbaseAdmin.createTable(tableDescriptor);
			    LOGGER.info("Hbase table "+hbaseTableName+" has been created");
			}
		} catch (IOException e) {
			e.printStackTrace();
			LOGGER.error(e.getMessage());
		}
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
		Date dateTime = new Date(ts*1000L); 
		String date,time;
		SimpleDateFormat jdf = new SimpleDateFormat("hh:mm");
		time = jdf.format(dateTime);
		//System.out.println("Time="+time);
		return time;
	}
	public static String getDate(long ts) {
		
		java.sql.Date sqlDate = new java.sql.Date(ts*1000L);
		//System.out.println("SQL Date="+sqlDate);   
		
		return sqlDate.toString();
	}
	public static  long getUnixTime() {
        long unixTime = 0;
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT+5:30")); //Specify your timezone
        unixTime = date.getTime();
		//unixTime = unixTime / 1000;
		System.out.println("Unix time="+unixTime);
        return unixTime;
    }
	public static  String getCurrentTime() {
        long unixTime = 0;
        //DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        DateTime date = new DateTime();
       // dateFormat.setTimeZone(TimeZone.getTimeZone("GMT+5:30")); //Specify your timezone
        
		//System.out.println("Unix time="+unixTime);
        return date.toString();
    }
	public static String getIpResolveCountry(String  ipAddress) throws IOException {
		props=getProperties();
		IpResolveCountry ipc=new IpResolveCountry(props.getProperty("mmdb.geoLocation.Country"));
		Tuple tuple= TupleFactory.getInstance().newTuple();//("162.168.1.1");
		tuple.append(ipAddress);
		String country= ipc.exec(tuple);
		//System.out.println("Country ="+country);
		return country;
	}
	public static String getIpResolveCity(String  ipAddress) throws IOException {
		props=getProperties();
		IpResolveCity ipCity=new IpResolveCity(props.getProperty("mmdb.geoLocation.City"));
		Tuple cityTuple= TupleFactory.getInstance().newTuple();//("162.168.1.1");
		cityTuple.append(ipAddress);
		//System.out.println("city ="+ipCity.exec(cityTuple));
		return ipCity.exec(cityTuple);
		//System.out.println("Country ="+country);
	}
	
	/**
	 * Method for creating a standard column filed name from the json key
	 * @param fieldName json key name
	 * @param length Maximum length to split the field
	 * @return Formatted hbase field name
	 */
	public static String createBluevigilFieldName(String fieldName, int length) {
		StringBuffer bluevigilField = new StringBuffer();
		String connector = propss.getProperty("bluevigil.hbase.column.qualifier.connector");
		for (String fieldPart: fieldName.trim().replaceAll("[^a-zA-Z0-9]+", connector).split(connector)) {
			if (!fieldPart.isEmpty() && !fieldPart.equals(connector)) {
				bluevigilField.append(fieldPart.substring(0, Math.min(fieldPart.length(), length))).append(connector);
			}
		}
		return bluevigilField.substring(0, bluevigilField.length()-1).toString();
	}
	
	/**
	 * Method for generating a full line with all column fields separated by a comma 
	 * @param separator
	 * @param parsedJson
	 * @param backenFieldMap
	 * @return
	 */
	public static String createLineFromParsedJson(String separator, Map<String, String> parsedJson, Map<Integer, String> backenFieldMap) {
		StringBuilder outputLine = new StringBuilder();
		for(int key:backenFieldMap.keySet()) {
			outputLine.append(parsedJson.get(backenFieldMap.get(key)) + separator);
		}
		// Remove the last comma separator
		return outputLine.substring(0, outputLine.length()-1).toString();
	}
}
