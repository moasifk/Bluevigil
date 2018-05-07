package com.bluevigil.analytics.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.bluevigil.analytics.utils.BluevigilProperties;

public class BluevigilUtility {
	static Logger LOGGER = Logger.getLogger(BluevigilUtility.class);
	private static BluevigilProperties props = BluevigilProperties.getInstance();
	
	//private static BluevigilProperties propss = BluevigilProperties.getInstance();
	public static Connection getHbaseConnection() 
	{		
		try 
		{
			Class.forName(props.getProperty("phoenix.jdbc.driver"));
		} 
		catch (ClassNotFoundException e1) 
		{
			LOGGER.info("Exception Loading Driver");
			LOGGER.error(e1.getMessage());
			return null;
		}
		try
		{
			Connection con = DriverManager.getConnection(props.getProperty("phoenix.jdbc.url"));  //172.31.124.43 is the adress of VM, not needed if ur running the program from vm itself
			LOGGER.info("Hbase Connection Established");
			return con;
		}
		catch(SQLException e)
		{
			LOGGER.info(e.getMessage());
			LOGGER.error(e.getMessage());
			return null;
		}
	}
	
	
	public static Connection getPostgresqlConnection(){		
		System.out.println(""+props.getProperty("bluevigil.postgresql.jdbc.driver"));
		LOGGER.info("-------- PostgreSQL JDBC Connection Testing ------------");
		try {
			//System.out.println(props.getProperty("bluevigil.postgresql.jdbc.driver"));
			Class.forName(props.getProperty("bluevigil.postgresql.jdbc.driver"));
		} catch (ClassNotFoundException e) {
			LOGGER.error("Where is your PostgreSQL JDBC Driver?,Include in your library path!");
			e.printStackTrace();
			return null;
		}
		LOGGER.info("PostgreSQL JDBC Driver Registered!");
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(props.getProperty("bluevigil.postgresql.jdbc.connection"));
		} catch (SQLException e) {
			LOGGER.error("Connection Failed! Check output console");
			e.printStackTrace();
			return null;
		}
		if (conn != null) {
			LOGGER.info("You made it, take control your database now!");
		} else {
			LOGGER.error("Failed to make connection!");
		}
		return conn;
	}
	
	public static Properties getProperties() {
		Properties prop = new Properties();
		InputStream input = null;
		try {
			input = new FileInputStream("./src/main/resources/dev/config.properties");
			// load a properties file
			prop.load(input);
			return prop;
		} catch (IOException ex) {
			LOGGER.info(ex.getMessage());
			LOGGER.error(ex.getMessage());
			return null;
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					LOGGER.info(e.getMessage());
					LOGGER.error(e.getMessage());
				}
			}
		}
	}
	/**
	 * Method for creating a standard column filed name from the json key
	 * @param fieldName json key name
	 * @param length Maximum length to split the field
	 * @return Formatted hbase field name
	 */
	public static String createBluevigilFieldName(String fieldName, int length) {
		StringBuffer bluevigilField = new StringBuffer();
		BluevigilProperties propss = BluevigilProperties.getInstance();
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
