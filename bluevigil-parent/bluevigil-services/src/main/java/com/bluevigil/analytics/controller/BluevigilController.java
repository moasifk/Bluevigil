package com.bluevigil.analytics.controller;

import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.bluevigil.analytics.hbase.BluevigilAnalyticsQueryProcessor;
import com.bluevigil.analytics.model.HttpLog;

@RestController

@RequestMapping("/search")
public class BluevigilController {
	
	Logger LOGGER = Logger.getLogger(BluevigilController.class);

	@CrossOrigin
	@RequestMapping(value = "/getAdhocResult", method = RequestMethod.POST)
	public @ResponseBody List<Map<String,Object>> getAdhocResult(@RequestParam(value = "sqlQuery") String sqlQuery) {
		System.out.println("Sql query="+sqlQuery);
		BluevigilAnalyticsQueryProcessor baqp=new BluevigilAnalyticsQueryProcessor();
		return baqp.getHbaseData(sqlQuery);
		
	}
}
