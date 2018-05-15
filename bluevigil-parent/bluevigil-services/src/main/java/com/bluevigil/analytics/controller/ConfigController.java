package com.bluevigil.analytics.controller;


import java.io.BufferedReader;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;


import com.bluevigil.analytics.config.DynamicJsonProcessor;
import com.bluevigil.analytics.config.ProcessJsonConfig;
import com.bluevigil.analytics.model.JsonParsedfields;


@RestController

@RequestMapping("/config")
public class ConfigController {
	 private static final String SUCCESS_STATUS = "success";
	 private static final String ERROR_STATUS = "error";
	Logger LOGGER = Logger.getLogger(BluevigilController.class);

//	@RequestMapping(value = "/getJsonFields", method = RequestMethod.POST)
//	public String getJsonFields(@RequestParam("file") MultipartFile inputFile) {
//		return "Hello";
//		
//	}
//	
//	@RequestMapping(value = "/hello", method = RequestMethod.POST)
//	public String sayHello() {
//		return "Hello";
//		
//	}
//	@CrossOrigin
	@RequestMapping(value = "/getJsonFields", method = RequestMethod.POST)
	public @ResponseBody List<JsonParsedfields> getJsonFields(@RequestParam("file") MultipartFile inputFile) {
		//System.out.println("In getJsonFields");
		BufferedReader br;
		if (!inputFile.isEmpty()) {
			String line=null;
		    InputStream is;
			try {
				is = inputFile.getInputStream();			
				br = new BufferedReader(new InputStreamReader(is));
				line=br.readLine();
				//System.out.println("Line ="+line);
			}catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				//return Map<String, String>;
			}
		List<JsonParsedfields> jsonFieldsList= DynamicJsonProcessor.parseJsonInputLine(line, "","", new ArrayList<JsonParsedfields>());
		return jsonFieldsList;
		}
		else
			return null;
		
	}
	@RequestMapping(value = "/getJsonConfig", method = RequestMethod.POST)
	public @ResponseBody String getJsonConfig(@RequestParam(value = "fileType") String fileType) {
		//System.out.println("File Type="+fileType);
		String line="{\"server\":\"\",\"bytes_in\":33,\"type\":\"dns\",\"client_prot\":50066,\"beat\":{\"name\":\"AD01\",\"hostname\":\"AD01\",\"version\":\"1.1.1\"}}";
		//return line;
		return ProcessJsonConfig.getJsonConfig(fileType);		
		
	}
	@RequestMapping(value = "/saveJsonConfig", method = RequestMethod.POST)
	public @ResponseBody String saveJsonConfig(@RequestParam(value = "fileType") String fileType,@RequestBody String configJson) {
		//System.out.println("File Type="+fileType);
		//System.out.println("Json="+configJson);
		if(ProcessJsonConfig.saveJsonConfig(fileType, configJson))
			return SUCCESS_STATUS;
		else
			return ERROR_STATUS;		
		
	}
	@RequestMapping(value = "/updateJsonConfig", method = RequestMethod.POST)
	public @ResponseBody String updateJsonConfig(@RequestParam(value = "fileType") String fileType,@RequestBody String configJson) {
		
		//System.out.println("File Type="+fileType);
		//System.out.println("Json="+configJson);
		if(ProcessJsonConfig.updateJsonConfig(fileType, configJson))
			return SUCCESS_STATUS;
		else
			return ERROR_STATUS;
		
	}

}
