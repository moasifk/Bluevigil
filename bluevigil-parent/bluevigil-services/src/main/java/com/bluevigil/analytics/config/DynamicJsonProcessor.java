package com.bluevigil.analytics.config;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.bluevigil.analytics.model.JsonParsedfields;
import com.bluevigil.analytics.utils.BluevigilConstant;
import com.bluevigil.analytics.utils.BluevigilProperties;
import com.bluevigil.analytics.utils.BluevigilUtility;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class DynamicJsonProcessor {
	static Logger LOGGER = Logger.getLogger(DynamicJsonProcessor.class);
	private static BluevigilProperties props = BluevigilProperties.getInstance();
	private static JsonParser parser = new JsonParser();

	public static List<JsonParsedfields> parseJsonInputLine(String line,String columnQual,String origFields, List<JsonParsedfields> parsedJsonList) {
		JsonObject jsonObject = parser.parse(line).getAsJsonObject();
//		Iterator<String> jsonObjectItr = jsonObject.keySet().iterator();
		Set<Map.Entry<String, JsonElement>> jsonObjectEntries = jsonObject.entrySet();		
		// Getting the max length of the column qualifier
		int qualMaxLength = Integer.parseInt(props.getProperty("bluevigil.hbase.column.qualifier.maxlength"));
		// Get the connector string for connecting different hbase field name..
		String qualConnector = props.getProperty("bluevigil.hbase.column.qualifier.connector");
		final String EMPTY_STRING = BluevigilConstant.EMPTY_STRING;
		try {
//			while (jsonObjectItr.hasNext()) {
			for (Map.Entry<String, JsonElement> jsonObjectEntry: jsonObjectEntries) {
				String jsonObjectKey = jsonObjectEntry.getKey();
//				String jsonObjectKey = jsonObjectItr.next();				
				//System.out.println("Json Key="+jsonObjectKey);
				JsonElement jsonElementValue = jsonObject.get(jsonObjectKey);
				String modifiedJsonObjectKey = BluevigilUtility.createBluevigilFieldName(jsonObjectKey, qualMaxLength);
				if (jsonElementValue.isJsonObject()) {
					// Create the field name as 3char_3char_3char
					origFields=getOrigStringConcat(origFields,jsonObjectKey,EMPTY_STRING);
					columnQual = getColumnQualifierName(columnQual, qualMaxLength, qualConnector, EMPTY_STRING,
							modifiedJsonObjectKey); 
					//System.out.println("nested columnQual="+columnQual);
					parseJsonInputLine(jsonElementValue.toString(), columnQual,origFields, parsedJsonList);
					//System.out.println("Original field="+origFields);
					if(origFields.contains(",")) {
						origFields=origFields.substring(0, origFields.lastIndexOf(","));
					}else {
						origFields= "";
					}				
					
				} else {
						//origFields	= EMPTY_STRING;			
						columnQual = getColumnQualifierName(columnQual, qualMaxLength, qualConnector, EMPTY_STRING,
								modifiedJsonObjectKey);
						//System.out.println("columnQual="+columnQual);
						JsonParsedfields jpFields=new JsonParsedfields();
						jpFields.setFieldName(getOrigStringConcat(origFields,jsonObjectKey,EMPTY_STRING));
						jpFields.setBackEndField(columnQual);
						parsedJsonList.add(jpFields);
						columnQual = EMPTY_STRING;
						
				}					
			}
			
		}catch(Exception ex) {
			LOGGER.error(ex);
			return null;
		}
		return parsedJsonList;
		
	}

	private static String getColumnQualifierName(String columnQual, int qualMaxLength, String qualConnector,
			final String EMPTY_STRING, String jsonObjectKey) {
		if (columnQual != EMPTY_STRING) {
			columnQual = columnQual + qualConnector
					+ jsonObjectKey.substring(0, Math.min(jsonObjectKey.length(), qualMaxLength));
		} else {
			columnQual = jsonObjectKey;
		}
		return columnQual;
	}
	private static String getOrigStringConcat(String orig,String keyField,final String EMPTY_STRING) {
		if(orig!=EMPTY_STRING) {
			return orig+","+keyField;
		}else {
			return keyField;
		}
	}
	private static String removeOrigString(String orig,final String EMPTY_STRING) {
		if(orig.contains(",")) {
			return orig.substring(0, orig.lastIndexOf(",")-1);
		}else {
			return "";
		}
	}	
	
}
