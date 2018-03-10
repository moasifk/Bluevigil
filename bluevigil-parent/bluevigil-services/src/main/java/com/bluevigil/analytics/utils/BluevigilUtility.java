package com.bluevigil.analytics.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Logger;

public class BluevigilUtility {
	static Logger LOGGER = Logger.getLogger(BluevigilUtility.class);
	private static Properties props=new Properties();
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
			System.out.println("Hbase Connection Established");
			return con;
		}
		catch(SQLException e)
		{
			System.out.println(e.getMessage());
			LOGGER.error(e.getMessage());
			return null;
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
}
