package com.bluecast.bluevigil.utils;

import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CountryResponse;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;
import static java.util.Arrays.asList;

/**
 * Created by bijay on 1/6/15.
 */
public class IpResolveCountry extends EvalFunc<String> {
  Logger logger = LoggerFactory.getLogger(IpResolveCountry.class);
  private final static String _local = "local";
  DatabaseReader reader;
  private File ipDatabase;
  private static List<String> listCIDR = asList("192.168.","172.16.","172.17.","172.18.","172.19.","172.20.","172.21.","172.22.","172.23.","172.24.","172.25.","172.26.","172.27.","172.28.","172.29.","172.30.","172.31.","10.");
  

public IpResolveCountry(String fileName) {
    
	this.ipDatabase = new File(fileName);
    try {
      this.reader = new DatabaseReader.Builder(ipDatabase).build();
//	  this.listCIDR = Arrays.asList("172.16.","172.17.","172.18.","172.19.","172.20.","172.21.","172.22.","172.23.","172.24.","172.25.","172.26.","172.27.","172.28.","172.29.","172.30.","172.31.","10.");
    } catch (IOException e) {
      System.out.println("Database reader cannot be created");
      e.printStackTrace();
    }
  }


  
public String exec(Tuple tuple) throws IOException {
    String ip = (String) tuple.get(0);
    String countryName = null;
    try {
		Iterator<String> iteratorCIDR = listCIDR.iterator();
		while (iteratorCIDR.hasNext()) {
			if (ip.startsWith(iteratorCIDR.next()))
			{
				countryName=_local;
				break;
			}
		}	

      if (countryName == null || countryName.length() == 0) {
		CountryResponse response = reader.country(InetAddress.getByName(ip));
		countryName = response.getCountry().getName();
	}
      if (countryName == null || countryName.length() == 0) {
        logger.warn("ipAddress:"+ip+" cannot be resolved");
        return null;
      }

    } catch (GeoIp2Exception e) {
      e.printStackTrace();
    }
    return countryName;
  }


  @Override
  public List<String> getCacheFiles() {
    List<String> list = new ArrayList<String>(1);
    list.add(ipDatabase + "#ip_lookup");
    return list;
  }
}