package com.bluevigil.analytics.config;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.Properties;

import org.apache.log4j.Logger;

import com.bluevigil.analytics.utils.BluevigilUtility;

public class ProcessJsonConfig {
	static Logger LOGGER = Logger.getLogger(BluevigilUtility.class);
	private static BluevigilProperties props = BluevigilProperties.getInstance();
	public static Connection conn=BluevigilUtility.getPostgresqlConnection();

	public static List<String> getJsonConfigTypes(){
		
		List<String> fileTypeList = new ArrayList<String>();
		String json=null;
		try { 			
			PreparedStatement stmt=conn.prepareStatement("select filename from configjson");			
			ResultSet rs=stmt.executeQuery();
			
			while(rs.next())
				{				
				fileTypeList.add(rs.getString("filename"));
				}
			} catch (SQLException sqle) {
				LOGGER.error( sqle.getMessage());
			}
		return fileTypeList;		
	}
public static String getFileConfigJson(String fileType){
		
		
		String json=null;
		try { 			
			PreparedStatement stmt=conn.prepareStatement("select config from configjson where filename=?");	
			stmt.setString(1, fileType);
			ResultSet rs=stmt.executeQuery();
			if(rs.isBeforeFirst())
			{
				while(rs.next())
					{				
					json=rs.getString("config");
					}
				return json;
			}else
				return "";
			
		}catch (SQLException sqle) {
			LOGGER.error( sqle.getMessage());
			return "";
		}
	}
public static int deleteConfigJson(String fileType){
	
	int  res=0;
	try { 			
		PreparedStatement stmt=conn.prepareStatement("delete  from configjson where filename=?");	
		stmt.setString(1, fileType);
		res=stmt.executeUpdate();		
		
		} catch (SQLException sqle) {
			LOGGER.error( sqle.getMessage());
		}
	return res;		
}
	public static boolean saveJsonConfig(String fileType,String jsonData) {	

			try { 
				if(conn!=null)
					System.out.println("Connection is not null");
				else
					System.out.println("Connection obje is null");
				PreparedStatement stmt = conn.prepareStatement("insert into configjson (filename, config) values (?, to_json(?::json))");
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
			PreparedStatement stmt = conn.prepareStatement("update configjson set config=(to_json(?::json)) where filename=?");			
			stmt.setString(1, jsonData);
			stmt.setString(2, fileType); 
			stmt.executeUpdate(); 
			stmt.close();	
			return true;
		} catch (SQLException sqle) {
			LOGGER.error(sqle.getMessage());
			return false;
		}

	}
}
