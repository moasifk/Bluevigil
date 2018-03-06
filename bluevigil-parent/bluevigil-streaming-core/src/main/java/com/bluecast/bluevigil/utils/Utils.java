package com.bluecast.bluevigil.utils;

import java.io.IOException;
import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import java.util.Properties;

import java.sql.Connection;
import java.sql.DriverManager;

import java.util.TimeZone;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.joda.time.DateTime;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
//import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class Utils implements Serializable{
	
	public static Producer<String, String> createProducer(String bootstrapServers) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "BluevigilDev");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return new KafkaProducer<String, String>(props);
	}
	/*public static  Connection getHbaseConnection() {
		//Connection con=new Conne
		Configuration conf=HBaseConfiguration.create();
		///*conf.set("hbase.zookeeper.quorum", "nn02.itversity.com,nn01.itversity.com");
		//conf.set("hbase.zookeeper.property.clientPort", "2181");
		//conf.set("zookeeper.znode.parent","/hbase-unsecure");	
		conf.set("hbase.zookeeper.quorum", "localhost");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent","/hbase-unsecure");		
		try {
			return ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
		
	}*/
	
	public static Connection getHbaseConnection() 
	{
		
		try 
		{
			Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
		} 
		catch (ClassNotFoundException e1) 
		{
			System.out.println("Exception Loading Driver");
			e1.printStackTrace();
			return null;
		}
		try
		{
			Connection con = DriverManager.getConnection("jdbc:phoenix:localhost:2181");  //172.31.124.43 is the adress of VM, not needed if ur running the program from vm itself
			System.out.println("Connection Established");
			return con;
		}
		catch(Exception e)
		{
			System.out.println(e.getMessage());
			return null;
		}
	}
	public static  boolean isHbaseTableExists(String hbaseTableName,String hbaseTableColumnFamily) {
		//Connection con=new Conne
		Configuration conf=HBaseConfiguration.create();
		
		/*conf.set("hbase.zookeeper.quorum", "nn02.itversity.com,nn01.itversity.com");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent","/hbase-unsecure");	*/
		conf.set("hbase.zookeeper.quorum", "localhost");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent","/hbase-unsecure");	
		try {
		HBaseAdmin hba = new HBaseAdmin(conf);
		
			if(!hba.tableExists(hbaseTableName)){
				
				HTableDescriptor tableDescriptor = new HTableDescriptor(hbaseTableName);
			    HColumnDescriptor columnDescriptor = new HColumnDescriptor(hbaseTableColumnFamily);
			    tableDescriptor.addFamily(columnDescriptor);
			    System.out.println("In Hbase table creatioin");
			    hba.createTable(tableDescriptor);
			    System.out.println("Hbase table "+hbaseTableName+" has been created");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return true;
		
	}
	public static String getTime(long ts) {
		Date dateTime = new Date(ts*1000L); 
		String date,time;
		SimpleDateFormat jdf = new SimpleDateFormat("hh:mm");
		time = jdf.format(dateTime);
		//System.out.println("Time="+time);
		return time;
	}
	public static java.sql.Date getDate(long ts) {
		
		java.sql.Date sqlDate = new java.sql.Date(ts*1000L);
		//System.out.println("SQL Date="+sqlDate);   
		
		return sqlDate;
	}
	public static  long getUnixTime() {
        long unixTime = 0;
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT+5:30")); //Specify your timezone
        unixTime = date.getTime();
		//unixTime = unixTime / 1000;
		System.out.println("Unix time="+unixTime);
        return unixTime;
    }
	public static  String getCurrentTime() {
        long unixTime = 0;
        //DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        DateTime date = new DateTime();
       // dateFormat.setTimeZone(TimeZone.getTimeZone("GMT+5:30")); //Specify your timezone
        
		//System.out.println("Unix time="+unixTime);
        return date.toString();
    }
	public static String getIpResolveCountry(String  ipAddress) throws IOException {
		IpResolveCountry ipc=new IpResolveCountry("./properties/GeoLite2-Country.mmdb");
		Tuple tuple= TupleFactory.getInstance().newTuple();//("162.168.1.1");
		tuple.append(ipAddress);
		String country= ipc.exec(tuple);
		System.out.println("Country ="+country);
		return country;
	}
	public static String getIpResolveCity(String  ipAddress) throws IOException {
		IpResolveCity ipCity=new IpResolveCity("./properties/GeoLite2-City.mmdb");
		Tuple cityTuple= TupleFactory.getInstance().newTuple();//("162.168.1.1");
		cityTuple.append(ipAddress);
		return ipCity.exec(cityTuple);
		//System.out.println("Country ="+country);
	}
}
