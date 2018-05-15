package com.bluevigil.analytics.config;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.bluevigil.analytics.utils.BluevigilUtility;

public class ProcessJsonConfig {
	static Logger LOGGER = Logger.getLogger(BluevigilUtility.class);
	private static Properties props=new Properties();
	public static Connection conn=BluevigilUtility.getPostgresqlConnection();

	public static String getJsonConfig(String fileType){
		
		String data = null;
		
		try { 			
			PreparedStatement stmt=conn.prepareStatement("select config from configJson where file_name=?");
			stmt.setString(1, fileType);
			ResultSet rs=stmt.executeQuery();
			data=rs.getString("config");				
			} catch (SQLException sqle) {
				LOGGER.error( sqle.getMessage());
			}
		return data;		
	}
	public static boolean saveJsonConfig(String fileType,String jsonData) {	

			try { 
				PreparedStatement stmt = conn.prepareStatement("insert into configJson (file_name, config) values (?, to_json(?::json))");
				stmt.setString(1, fileType);
				stmt.setString(2, jsonData); 
				stmt.executeUpdate(); 
				stmt.close();
				return true;
			} catch (SQLException sqle) {
				LOGGER.error( sqle.getMessage());
				return false;
			}

		}
	public static boolean updateJsonConfig(String fileType,String jsonData) {
		
		try { 
			PreparedStatement stmt = conn.prepareStatement("update configJson (file_name, config) values (?, to_json(?::json)) where file_name=?");
			stmt.setString(1, fileType);
			stmt.setString(2, jsonData); 
			stmt.executeUpdate(); 
			stmt.close();	
			return true;
		} catch (SQLException sqle) {
			LOGGER.error(sqle.getMessage());
			return false;
		}

	}
}

