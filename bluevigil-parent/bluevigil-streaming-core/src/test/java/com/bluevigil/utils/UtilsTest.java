package com.bluevigil.utils;

import org.junit.Test;

public class UtilsTest {
	
	@Test
	public void testCreateBluevigilFieldName() {
		Utils bluevigilUtils = new Utils();
		System.out.println(bluevigilUtils.createBluevigilFieldName("id.orig_p",3));
	}
}
