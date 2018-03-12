package com.bluecast.bluevigil.utils;
import java.util.*;
import java.io.File;
import java.io.IOException;
import java.text.*;
import java.math.*;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;



public class TimeStampConversion {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	public static void main(String[] args) throws ClassNotFoundException, SQLException
	{
		try {
			String country=Utils.getIpResolveCountry("93.184.220.29");
			System.out.println("Country="+country);
			String city=Utils.getIpResolveCity("93.184.220.29");
			System.out.println("City="+city);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//hiveConnection();
		//getTime((long)1510665254.454436);
		//Double a=Double.parseDouble("1.510665254454436E9");
		//DecimalFormat formatter = new DecimalFormat("0.000000");
		//String date=formatter .format(a);
		
	}
	public static String getTime(long ts) {
		Date dateTime = new Date(ts*1000L); 
		String date,time;
		SimpleDateFormat jdf = new SimpleDateFormat("hh:mm");
		time = jdf.format(dateTime);
		System.out.println("Time="+time);
		return time;
	}
	public static java.sql.Date getDate(long ts) {
		
		java.sql.Date sqlDate = new java.sql.Date(ts*1000L);
		System.out.println("SQL Date="+sqlDate);   
		
		return sqlDate;
	}
	public static void hiveConnection() throws ClassNotFoundException, SQLException {
		
	      // get connection
	      System.out.println("In HiveConnectiion");
	      Class.forName(driverName);
	      Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "yassar", "");
	      Statement stmt = con.createStatement();
	      
	      stmt.executeQuery("CREATE DATABASE userTestDb");
	      System.out.println("Database userdb created successfully.");
	      
	      con.close();
	}
	
	
}
