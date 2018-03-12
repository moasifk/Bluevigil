package com.bluevigil.analytics.hbase;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.bluevigil.analytics.controller.BluevigilController;
import com.bluevigil.analytics.utils.BluevigilUtility;

public class BluevigilAnalyticsQueryProcessor {
	Logger LOGGER = Logger.getLogger(BluevigilAnalyticsQueryProcessor.class);
	
	public List<Map<String,Object>> getHbaseData(String sqlQuery) {
		try {
			Connection con=BluevigilUtility.getHbaseConnection();
			LOGGER.info("In BlueVigilAnalyticsProcessor query="+sqlQuery);
			PreparedStatement stmt = con.prepareStatement(sqlQuery);
			ResultSet rs = stmt.executeQuery();
			List<Map<String,Object>> json = new ArrayList<Map<String,Object>>();
			ResultSetMetaData rsmd = rs.getMetaData();
					
		 //while(rs.next())
			while(rs.next())
			{	
				
				 int numColumns = rsmd.getColumnCount();
				  JSONObject obj = new JSONObject();
				  for (int i=1; i<=numColumns; i++) {
				    String column_name = rsmd.getColumnName(i);
				    obj.put(column_name, rs.getObject(column_name));
				  }
				  json.add(obj.toMap());
			}
			
			
			return json;
			
		} catch (SQLException e) {
			LOGGER.info(e.getMessage());
			LOGGER.error(e.getMessage());
			return null;
		}
		
	}
	

}
