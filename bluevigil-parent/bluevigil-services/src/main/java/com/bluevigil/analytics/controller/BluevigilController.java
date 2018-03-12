package com.bluevigil.analytics.controller;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.bluevigil.analytics.hbase.BluevigilAnalyticsQueryProcessor;

@RestController

@RequestMapping("/search")
public class BluevigilController {
	
	Logger LOGGER = Logger.getLogger(BluevigilController.class);

	@CrossOrigin
	@RequestMapping(value = "/getAdhocResult", method = RequestMethod.POST)
	public @ResponseBody List<Map<String,Object>> getAdhocResult(@RequestBody String strArg) {
		LOGGER.info("Sql query="+strArg);
		JSONObject jsonObject = new JSONObject(strArg);
		LOGGER.info("json sqlQuery="+ jsonObject.getString("sqlQuery"));
		BluevigilAnalyticsQueryProcessor baqp=new BluevigilAnalyticsQueryProcessor();
		return baqp.getHbaseData( jsonObject.getString("sqlQuery"));
		
	}
}
