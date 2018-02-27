package com.bluecast.bluevigil.utils;
import java.util.*;
import java.io.File;
import java.text.*;
import java.math.*;



public class TimeStampConversion {

	public static void main(String[] args)
	{
		getTime((long)1510665254.454436);
		Double a=Double.parseDouble("1.510665254454436E9");
		DecimalFormat formatter = new DecimalFormat("0.000000");
		String date=formatter .format(a);
		//System.out.println("Date value="+date);
		//System.out.println(Long.parseLong(date));
		//1510665254.454436
		//1510665254.454436
		//1510665254.454436
		//long  ts=(long)Double.parseDouble("1510665261.593545");
		//String date,time,str;
		//String[] date_time = null;
		//Date dateTime = new Date(ts*1000L); 
		//DateFormat jdf = new SimpleDateFormat("yyyy-MM-dd");
		//jdf.setTimeZone(TimeZone.getTimeZone("GMT-4"));
		//date = jdf.format(dateTime);
		//jdf = new SimpleDateFormat("hh.mm aa");
		//time = jdf.format(dateTime);
		//System.out.println("\n"+java_date.trim()+"\n");
		//Calendar  mydate = Calendar.getInstance();
		//mydate.setTimeInMillis(unix_seconds*1000L);
		//System.out.println(mydate.get(Calendar.YEAR)+"-"+mydate.get(Calendar.MONTH)+"-"+mydate.get(Calendar.DAY_OF_MONTH));
		//System.out.println(date+" and "+time);
		
		//str=date+","+time;
		//System.out.println("date="+str.substring(0, str.indexOf(",")));
		//System.out.println("time="+str.substring(str.indexOf(",")+1,str.length() ));
		/*long ts=(long)1510665261.593545;
		Date date = new Date(ts*1000L); 
		SimpleDateFormat jdf = new SimpleDateFormat("yyyy-MM-dd hh.mm aa z");
		//jdf.setTimeZone(TimeZone.getTimeZone("GMT-4"));
		string java_date = jdf.format(date);
		System.out.println("\n"+java_date.trim()+"\n");*/
	}
	public static String getTime(long ts) {
		Date dateTime = new Date(ts*1000L); 
		String date,time;
		SimpleDateFormat jdf = new SimpleDateFormat("hh:mm");
		time = jdf.format(dateTime);
		System.out.println("Time="+time);
		return time;
	}
	public static java.sql.Date getDate(long ts) {
		
		java.sql.Date sqlDate = new java.sql.Date(ts*1000L);
		System.out.println("SQL Date="+sqlDate);   
		
		return sqlDate;
	}
	
	
}
