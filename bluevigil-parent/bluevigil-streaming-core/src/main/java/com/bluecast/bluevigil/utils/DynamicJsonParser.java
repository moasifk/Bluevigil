package com.bluecast.bluevigil.utils;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.bluecast.bluevigil.model.FieldMapping;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class DynamicJsonParser {
	static Logger LOGGER = Logger.getLogger(DynamicJsonParser.class);
	private static BluevigilProperties props = BluevigilProperties.getInstance();
	private static JsonParser parser;
	static Put put ;
	public static Put parseDynamicJson(String line, String NWLOG_FILE_CONFIG_PATH) {
		LOGGER.info("Dynamic json parse " + line);
		parser = new JsonParser();
		String rowKey = "SomeRowKey";
		Gson gson = new Gson();
		FileReader reader = null;
		try {
			reader = new FileReader(NWLOG_FILE_CONFIG_PATH);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		FieldMapping mappingData = gson.fromJson(reader, FieldMapping.class);
		put = new Put(Bytes.toBytes(rowKey));
		put = parseJsonObject(line, mappingData);
		return put;
	}

	private static Put parseJsonObject(String line, String columnHead, FieldMapping map) {
		byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("CF");
		String columnHead = "";
		JsonObject jsonObject = parser.parse(line).getAsJsonObject();
		Iterator<String> jsonObjectItr = jsonObject.keySet().iterator();
		while (jsonObjectItr.hasNext()) {
			String jsonObjectKey = jsonObjectItr.next();
			JsonElement jsonElement = jsonObject.get(jsonObjectKey);
			if (jsonElement.isJsonObject()) {
				columnHead = jsonObjectKey;
				parseJsonObject(jsonElement.toString(), columnHead, map);
			} else {
				// Get mapping data as key value pair
				if (map.get(key))
				byte[] COLUMN_NAME = Bytes.toBytes(columnHead + "_"+jsonObjectKey);
				put.addColumn(COLUMN_FAMILY_NAME, COLUMN_NAME, Bytes.toBytes(jsonElement.toString()));
			}
		}
		return put;
	}

}
