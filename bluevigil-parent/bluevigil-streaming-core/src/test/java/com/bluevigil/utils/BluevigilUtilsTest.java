package com.bluevigil.utils;

import java.io.IOException;

import org.junit.Test;

public class BluevigilUtilsTest {
	
	@Test
	public void testCreateBluevigilFieldName() {
		BluevigilUtils bluevigilUtils = new BluevigilUtils();
//		System.out.println(bluevigilUtils.createBluevigilFieldName("id.orig_p",3));
	}
	
	@Test
	public void testGetIpResolveCity() {
		BluevigilUtils bluevigilUtils = new BluevigilUtils();
//		try {
//			System.out.println(bluevigilUtils.getIpResolveCity("162.168.1.1"));
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
	}
	
	@Test
	public void testFormatJsonValue() {
		BluevigilUtils bluevigilUtils = new BluevigilUtils();
		System.out.println(bluevigilUtils.formatJsonValue("FxXGfP2orWzwwF3RB8"));
	}
}
