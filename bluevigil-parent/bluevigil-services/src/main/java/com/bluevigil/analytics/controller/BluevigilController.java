package com.bluevigil.analytics.controller;

import org.apache.log4j.Logger;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
@RestController
@RequestMapping("/search")
public class BluevigilController {
	
	Logger LOGGER = Logger.getLogger(BluevigilController.class);

	@CrossOrigin
	@RequestMapping(value = "/getAdhocResult", method = RequestMethod.POST)
	public String getAdhocResult(@RequestParam(value = "sqlQuery") String sqlQuery) {
		String jsonData = "Sql query result";
		return jsonData;
	}
}
