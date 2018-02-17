package com.bluecast.bluevigil.streaming;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.bluecast.bluevigil.model.FieldMapping;
import com.bluecast.bluevigil.model.KeyField;
import com.bluecast.bluevigil.model.Mapping;
import com.bluecast.bluevigil.model.QueryFields.QueryField;
import com.bluecast.bluevigil.model.QueryFields.QueryFieldMapping;
import com.bluecast.bluevigil.utils.Utils;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import com.google.protobuf.ServiceException;

import com.bluecast.bluevigil.utils.phoenix_hbase;
import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * Consumer class to condume data from Kafka topic
 * @author asif
 *
 */
public class BluevigilConsumer implements Serializable {
	//Map<String,String> fieldMap=new HashMap<String,String>();

	public void consumeDataFromSource(String soureTopic, final String destTopic, 
			final String bootstrapServers, String zookeeperServer, JavaStreamingContext jssc,FieldMapping mappingData)  {
		HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(soureTopic.split(",")));
		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", bootstrapServers);
		kafkaParams.put("zookeeper.connect", zookeeperServer);
		
		// Create direct Kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
		
		System.out.println("in BlueVigilConsumer");
		// Get the lines, split them into words, count the words and print
		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
			public String call(Tuple2<String, String> line) throws Exception {				
				//System.out.println(line._2());
				String str=line._2;
				System.out.println("Line is ="+str);
				insertToHbase(str.substring(str.indexOf("{")+1, str.indexOf("}")), mappingData);
				return line._2();
			}

		});		
		
		
		
		final Utils utils = new Utils();
		lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			public void call(JavaRDD<String> rdd) throws Exception {
				rdd.foreach(new VoidFunction<String>() {
					public void call(String s) throws Exception {
						Producer<String, String> producer = utils.createProducer(bootstrapServers);
						ProducerRecord<String, String> record = new ProducerRecord<String, String>(destTopic, "key", s);
						RecordMetadata metadata = producer.send(record).get();
						//insertIntoHdfs(s);
					}
				});
			}
		});

		lines.print();

		// Execute the Spark workflow defined above
		jssc.start();
		jssc.awaitTermination();

	}
	public void insertIntoHdfs(String rawString) {
		String[] arr,elements;
		elements=rawString.split(",");
	}
	@SuppressWarnings("deprecation")
	public void insertToHbase(String line,FieldMapping mappingData) throws IOException, ServiceException {
		Connection con=Utils.getHbaseConnection();
		TableName tn=TableName.valueOf(mappingData.getHbaseTable());
		
		Utils.isHbaseTableExists(mappingData.getHbaseTable(),mappingData.getHbaseColumnFamily());
	
		
		Table tbl=con.getTable(tn);
		System.out.println("Hbase table created successfully");
		
		String backEndField,fieldValue,fieldLabel=null,rowKey,hbaseField,multipleFieldValues;
		String[] arr,elements;
		String delimiter=mappingData.getDelimitter();
		List<Mapping> fieldMappingList=mappingData.getMapping();
		//System.out.println("String line ="+line);
		elements=line.split(delimiter);		
		//System.out.println("Element at index 1="+elements[1]);
		
		//Row Key creation using User id and Time Stamp
		rowKey=createHbaseRowKey(mappingData.getKeyFields(),elements);
		System.out.println("Key ="+rowKey);
		Put p = new Put(Bytes.toBytes(rowKey));
		
		
		
		//System.out.println("Key="+rowKeyElmnt1+"|"+rowKeyElmnt2);
		Iterator<Mapping> it=fieldMappingList.iterator();
		while(it.hasNext()) {
			Mapping mappingObj=it.next();
			backEndField=mappingObj.getBackEndField();
			hbaseField=mappingObj.getHbaseField();
			for(int i=0;i<elements.length;i++) {
				if(elements[i].contains(":"))
				{
					 arr=elements[i].split("\":");
					//System.out.println("Element ="+elements[i]);
					 
					if(arr[0].startsWith("\"") ) 
					{
						fieldLabel=arr[0].substring(1,arr[0].length());
					}
					else
					{
						fieldLabel=arr[0].substring(arr[0].length());
					}
					
					if(arr[1].startsWith("[\"") && arr[1].endsWith("\"]")) 
					{
						fieldValue=arr[1].substring(2,arr[1].length()-2);
					}
					else if (arr[1].startsWith("[\"") && arr[1].endsWith("\"")) 
					{
						fieldValue=arr[1].substring(2,arr[1].length()-1);
					}
					else if(arr[1].startsWith("\"") && arr[1].endsWith("\""))
					{
						fieldValue=arr[1].substring(1,arr[1].length()-1);
					}
					else if(arr[1].startsWith("[") && arr[1].endsWith("]")&& arr[1].length()>2)
					{
						fieldValue=arr[1].substring(1,arr[1].length()-1);
					}
					else if(arr[1].startsWith("[") && arr[1].endsWith("]")&& arr[1].length()==2) 
					{
						fieldValue=null;
					}
					else if(arr[1].startsWith("\"") && !arr[1].endsWith("\"")) 
					{
						fieldValue=arr[1].substring(1,arr[1].length());;
					}
					else
					{
						fieldValue=arr[1];
					}
				
					if(fieldLabel.equals(backEndField) && fieldValue!=null) 
					{
						if(mappingObj.getLabel().equals("Timestamp") ||mappingObj.getLabel()=="Timestamp")  
						{
							String dateTime=Utils.getDateTime((long)Double.parseDouble(fieldValue));
							p.add(Bytes.toBytes(mappingData.getHbaseColumnFamily()),Bytes.toBytes("LOG_DATE"),Bytes.toBytes(dateTime.substring(0,dateTime.indexOf(","))));
							p.add(Bytes.toBytes(mappingData.getHbaseColumnFamily()),Bytes.toBytes("LOG_TIME"),Bytes.toBytes(dateTime.substring(dateTime.indexOf(",")+1,dateTime.length())));
						}
						else if(fieldValue!= null &&isIpValid(fieldValue))
						{
							if(mappingObj.getLabel().equals("DestinationIP") ||mappingObj.getLabel()=="DestinationIP")  
							{
								p.add(Bytes.toBytes(mappingData.getHbaseColumnFamily()),Bytes.toBytes("DEST_COUNTRY"),Bytes.toBytes(Utils.getIpResolveCountry(fieldValue)));
								p.add(Bytes.toBytes(mappingData.getHbaseColumnFamily()),Bytes.toBytes("DEST_CITY"),Bytes.toBytes(Utils.getIpResolveCity(fieldValue)));
							}
							else if (mappingObj.getLabel().equals("SourceIP") ||mappingObj.getLabel()=="SourceIP")  
							{
								p.add(Bytes.toBytes(mappingData.getHbaseColumnFamily()),Bytes.toBytes("SOURCE_COUNTRY"),Bytes.toBytes(Utils.getIpResolveCountry(fieldValue)));
								p.add(Bytes.toBytes(mappingData.getHbaseColumnFamily()),Bytes.toBytes("SOURCE_CITY"),Bytes.toBytes(Utils.getIpResolveCity(fieldValue)));
							}
						}
						System.out.println("Field Value="+fieldValue);
						
						p.add(Bytes.toBytes(mappingData.getHbaseColumnFamily()),Bytes.toBytes(mappingObj.getHbaseField()),Bytes.toBytes(fieldValue));
						
					}
				}
				else if(elements[i].startsWith("\"") && elements[i].endsWith("\""))
				{
					//System.out.println("field label is ="+fieldLabel);
					fieldValue=elements[i].substring(1,elements[i].length()-1);
					//System.out.println("field value is ="+fieldValue);
					System.out.println("Field Value="+fieldValue);
					p.add(Bytes.toBytes(mappingData.getHbaseColumnFamily()),Bytes.toBytes(mappingObj.getHbaseField()),Bytes.toBytes(fieldValue));
				}
				else if(elements[i].startsWith("\"") && elements[i].endsWith("\"]"))
				{
					fieldValue=elements[i].substring(1,elements[i].length()-2);
					System.out.println("Field Value="+fieldValue);
					p.add(Bytes.toBytes(mappingData.getHbaseColumnFamily()),Bytes.toBytes(mappingObj.getHbaseField()),Bytes.toBytes(fieldValue));
				}
				
			}
			
		}
		System.out.println("Going to insert into hbase");
		if(!p.isEmpty()) {
			tbl.put(p);
		}
	}
	public String createHbaseRowKey(List<KeyField> keyFieldsList,String[] elements) {
		String keyValue=null,keyField,fieldLabel=null,fieldValue=null;
		String[] arr;
		Iterator<KeyField> it=keyFieldsList.iterator();
		while(it.hasNext()) {
			keyField=it.next().getBackEndField();
			for(int i=0;i<elements.length;i++) 
			{
				//System.out.println("Element ="+elements[i]); 
				if(elements[i].contains(":"))
				{
					arr=elements[i].split("\":");
					
					//System.out.println("Elemnts in arra[i]"+arr[0]+" and "+arr[1]);
					
					if(arr[0].startsWith("\"") )
					{
						fieldLabel=arr[0].substring(1,arr[0].length());
					}
					else
					{
						fieldLabel=arr[0].substring(arr[0].length());
					}
					if(arr[1].startsWith("[\"") && arr[1].endsWith("\"]")) 
					{
						fieldValue=arr[1].substring(2,arr[1].length()-2);
					}
					else if (arr[1].startsWith("[\"") && arr[1].endsWith("\""))  
					{
						fieldValue=arr[1].substring(2,arr[1].length()-1);
					}
					else if(arr[1].startsWith("\"") && arr[1].endsWith("\""))
					{
						fieldValue=arr[1].substring(1,arr[1].length()-1);
					}
					else if(arr[1].startsWith("[") && arr[1].endsWith("]")&& arr[1].length()>2) {
						fieldValue=arr[1].substring(1,arr[1].length()-1);
					}
					else if(arr[1].startsWith("[") && arr[1].endsWith("]")&& arr[1].length()==2) {
						fieldValue=null;
					}
					else if(arr[1].startsWith("\"") && !arr[1].endsWith("\"")) 
					{
						fieldValue=arr[1].substring(1,arr[1].length());;
					}
					else
					{
						fieldValue=arr[1];
					}
				//fieldMap.put(fieldLabel, fieldValue);
				}
				else if(elements[i].startsWith("\"") && elements[i].endsWith("\""))
				{
					fieldValue=elements[i].substring(1,elements[i].length()-1);
					System.out.println("value without :="+fieldValue);
					
				}
				else if(elements[i].startsWith("\"") && elements[i].endsWith("\"]"))
				{
					fieldValue=elements[i].substring(1,elements[i].length()-2);
					System.out.println("value without :="+fieldValue);
				}
				
				
				if(fieldLabel.equals(keyField))
				{
					if(keyValue==null || keyValue.equals("") )
					{
						keyValue=fieldValue;
						break;
					}
					else
					{
						keyValue=keyValue+"|"+fieldValue;
						break;
					}
				}
			
		}
		
	}
		return keyValue;

}
	public boolean isIpValid(final String ip){
		final Pattern pattern;
	    final Matcher matcher;

	    final String IPADDRESS_PATTERN =
			"^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
			"([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
			"([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
			"([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";
	    pattern = Pattern.compile(IPADDRESS_PATTERN);
		  matcher = pattern.matcher(ip);
		  return matcher.matches();
	    }

public void getHbaseData() throws FileNotFoundException, SQLException {
	/*Gson gson = new Gson();
	JsonReader reader = new JsonReader(new FileReader("./properties/queryFields.json"));
	QueryFieldMapping queryMappingData = gson.fromJson(reader,QueryFieldMapping.class); 
	List<QueryField> queryMappingList=queryMappingData.getQueryFields();
	Iterator<QueryField> it=queryMappingList.iterator();
	while(it.hasNext()) {
		System.out.println("Table name ="+it.next().getTableName());
	
		}*/
	//Testing pheonix code
	phoenix_hbase.connectHbase();
}
}
