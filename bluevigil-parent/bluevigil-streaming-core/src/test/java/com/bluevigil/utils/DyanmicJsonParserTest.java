package com.bluevigil.utils;

import java.util.ArrayList;

import org.junit.Test;

import com.bluevigil.model.DerivedFieldMapping;

import scala.Array;

public class DyanmicJsonParserTest {
	private static BluevigilProperties props = BluevigilProperties.getInstance();
	@Test
	public void testParseDyuidnamicJson() {
		DynamicJsonParser praser = new DynamicJsonParser();
		praser.parseDynamicJson(props.getProperty("line"),"./properties/http_config.json",new ArrayList<DerivedFieldMapping>());
	}

}
