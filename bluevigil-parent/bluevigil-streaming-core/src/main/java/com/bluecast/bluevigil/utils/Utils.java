package com.bluecast.bluevigil.utils;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.google.protobuf.ServiceException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Connection;
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
	public static  Connection getHbaseConnection() {
		//Connection con=new Conne
		Configuration conf=HBaseConfiguration.create();
		/*conf.set("hbase.zookeeper.quorum", "nn02.itversity.com,nn01.itversity.com");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent","/hbase-unsecure");	*/
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

			    hba.createTable(tableDescriptor);
			    System.out.println("Hbase table "+hbaseTableName+" has been created");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return true;
		
	}
	public static String getDateTime(long ts) {
		Date dateTime = new Date(ts*1000L); 
		String date,time;
		SimpleDateFormat jdf = new SimpleDateFormat("yyyy-MM-dd");
		//jdf.setTimeZone(TimeZone.getTimeZone("GMT-4"));
		date = jdf.format(dateTime);
		time = jdf.format(dateTime);
		//System.out.println("\n"+java_date.trim()+"\n");
		//Calendar  mydate = Calendar.getInstance();
		//mydate.setTimeInMillis(unix_seconds*1000L);
		//System.out.println(mydate.get(Calendar.YEAR)+"-"+mydate.get(Calendar.MONTH)+"-"+mydate.get(Calendar.DAY_OF_MONTH));
		return date+","+time;
	}
	public static String getIpResolveCountry(String  ipAddress) throws IOException {
		IpResolveCountry ipc=new IpResolveCountry("./properties/GeoLite2-Country.mmdb");
		Tuple tuple= TupleFactory.getInstance().newTuple();//("162.168.1.1");
		tuple.append(ipAddress);
		return ipc.exec(tuple);
		//System.out.println("Country ="+country);
	}
	public static String getIpResolveCity(String  ipAddress) throws IOException {
		IpResolveCity ipCity=new IpResolveCity("./properties/GeoLite2-City.mmdb");
		Tuple cityTuple= TupleFactory.getInstance().newTuple();//("162.168.1.1");
		cityTuple.append(ipAddress);
		return ipCity.exec(cityTuple);
		//System.out.println("Country ="+country);
	}
}
