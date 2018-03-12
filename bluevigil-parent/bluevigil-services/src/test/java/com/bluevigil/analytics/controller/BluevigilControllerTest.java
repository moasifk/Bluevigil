package com.bluevigil.analytics.controller;

import org.apache.log4j.Logger;
import org.junit.Test;
public class BluevigilControllerTest {
	
	Logger LOGGER = Logger.getLogger(BluevigilController.class);

	@Test
	public void getAdhocResultTest() {
		BluevigilController controller = new BluevigilController();
		System.out.println("Hello");
//		controller.getAdhocResult("select  * from  HBASE_TEST");
	}
}
