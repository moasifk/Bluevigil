package com.bluevigil.analytics.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;

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
	

}
