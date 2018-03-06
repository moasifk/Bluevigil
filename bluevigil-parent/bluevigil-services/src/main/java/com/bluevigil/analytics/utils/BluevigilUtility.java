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
	
	public static Connection getHbaseConnection() throws SQLException 
	{
		@SuppressWarnings("unused")
		Statement stmt = null;
		try 
		{
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		} 
		catch (ClassNotFoundException e1) 
		{
			System.out.println("Exception Loading Driver");
			e1.printStackTrace();
			return null;
		}
		try
		{
			Connection con = DriverManager.getConnection("jdbc:phoenix:localhost:2181");  //172.31.124.43 is the adress of VM, not needed if ur running the program from vm itself
			
			return con;
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
			return null;
		}
	}
	

}
