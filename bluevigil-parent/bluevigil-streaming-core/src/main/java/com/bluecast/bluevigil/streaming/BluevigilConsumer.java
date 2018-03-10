package com.bluecast.bluevigil.streaming;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.json.JSONException;
import org.json.JSONObject;

import com.bluecast.bluevigil.model.FieldMapping;
import com.bluecast.bluevigil.model.Mapping;
import com.bluecast.bluevigil.utils.Utils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * Consumer class to condume data from Kafka topic
 * @author asif
 *
 */
public class BluevigilConsumer implements Serializable {
	private static final long serialVersionUID = 1L;
	static transient Logger LOGGER = Logger.getLogger(BluevigilConsumer.class);
	Properties props=new Properties();
	//Map<String,String> fieldMap=new HashMap<String,String>();
 
	public void consumeDataFromSource(String soureTopic, final String destTopic, 
			final String bootstrapServers, String zookeeperServer, JavaStreamingContext jssc,final FieldMapping mappingData)  {
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
			
			private static final long serialVersionUID = 1L;
			
			public String call(Tuple2<String, String> line) throws Exception {				
				//System.out.println(line._2());
				String str=line._2;
				//System.out.println("Line is ="+str);
				insertToHbase(str, mappingData);
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

		//lines.print();

		// Execute the Spark workflow defined above
		jssc.start();
		jssc.awaitTermination();

	}
	
	
	
	
	@SuppressWarnings("deprecation")
	public void insertToHbase(String line,FieldMapping mappingData) {
		System.out.println("In InsertIntoHbaseTable method");	
		String backEndField,key,str,country,city;	
		//try {	
			
			JSONObject json;
			try {
				json = new JSONObject(line);
			
			Map<String,Object> rawObjectMap=new HashMap<String,Object>();
		    @SuppressWarnings("unchecked")
			Iterator<String> keys = json.keys();
		    while (keys.hasNext()) {
		    	key=keys.next();		    	
		        rawObjectMap.put(key,json.get(key));	        
		        
		    	}
			
		   // rowKey=createHbaseRowKey(mappingData.getKeyFields(),rawObjectMap);
			//String sqlQuery="upsert into "+mappingData.getHbaseTable();
		    Map<String,Object> queryMap=new HashMap<String,Object>();
		    	
		    Object fieldValue;
		    
			Set<String> mapKeySet = rawObjectMap.keySet();
			List<Mapping> fieldMappingList=mappingData.getMapping();
			Iterator<Mapping> it=fieldMappingList.iterator();
			while(it.hasNext()) {
				Mapping mappingObj=it.next();
				backEndField=mappingObj.getBackEndField();
				//hbaseField=mappingObj.getHbaseField();
				try {
				if(mapKeySet.contains(backEndField))
				{
					fieldValue=rawObjectMap.get(backEndField);
					//System.out.println("field is="+backEndField);
					if(mappingObj.getHbaseField()=="LOG_TIMESTAMP" ||mappingObj.getHbaseField().equals("LOG_TIMESTAMP"))
					{
						Double a=Double.parseDouble(fieldValue.toString());
						DecimalFormat formatter = new DecimalFormat("0.000000");
						String ts=formatter.format(a);
						queryMap.put(mappingObj.getHbaseField(),ts);
						queryMap.put("LOG_DATE",ts);
						queryMap.put("LOG_TIME",Utils.getTime((long)Double.parseDouble(ts)));
					}
					else if(fieldValue!= null &&isIpValid(fieldValue.toString()))
					{
						
						if(mappingObj.getHbaseField().equals("DEST_IP") ||mappingObj.getHbaseField()=="DEST_IP") 
						{
							country=Utils.getIpResolveCountry(fieldValue.toString());
							city=Utils.getIpResolveCity(fieldValue.toString());
							queryMap.put(mappingObj.getHbaseField(), fieldValue);
							
							if(!mappingData.getLogFileName().equals("conn"))
							{
								
								queryMap.put("DEST_COUNTRY", (Object)country);
								
								queryMap.put("DEST_CITY",city );
							}
							else if (mappingData.getLogFileName().equals("conn"))
							{
								queryMap.put("DEST_LOCATION", country);
							}
						}
						else if (mappingObj.getHbaseField().equals("SOURCE_IP") ||mappingObj.getHbaseField()=="SOURCE_IP")
						{
							country=Utils.getIpResolveCountry(fieldValue.toString());
							city=Utils.getIpResolveCity(fieldValue.toString());
							queryMap.put(mappingObj.getHbaseField(), fieldValue);
							
							if(!mappingData.getLogFileName().equals("conn"))
							{
								queryMap.put("SOURCE_COUNTRY", country);
								queryMap.put("SOURCE_CITY", city);
							}
							else if (mappingData.getLogFileName().equals("conn"))
							{
								queryMap.put("SOURCE_LOCATION", country);
							}
						}
						
					}
					
					else if(fieldValue.toString().contains(","))
					{
						if(fieldValue.toString().startsWith("[\"")&& fieldValue.toString().endsWith("\"]"))
						{
							str=fieldValue.toString().substring(1, fieldValue.toString().length()-1);
							queryMap.put(mappingObj.getHbaseField(), str.replace("\"", ""));
							
						}
						else if(fieldValue.toString().startsWith("[")&& fieldValue.toString().endsWith("]"))
						{
							str=fieldValue.toString().substring(1, fieldValue.toString().length()-1);
							queryMap.put(mappingObj.getHbaseField(), str);
						}						
						
					}
					else if(fieldValue.toString().startsWith("[\"")&& fieldValue.toString().endsWith("\"]"))
					{
						queryMap.put(mappingObj.getHbaseField(), fieldValue.toString().substring(2, fieldValue.toString().length()-2));
					}
					else if(fieldValue.toString().startsWith("[")&& fieldValue.toString().endsWith("]"))
					{
						queryMap.put(mappingObj.getHbaseField(), fieldValue.toString().substring(1, fieldValue.toString().length()-1));
					}
					else
					{
						queryMap.put(mappingObj.getHbaseField(),fieldValue);
					}
					
					
				}
			} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					LOGGER.error(e.getMessage());
				}
				
			}
			queryMap.put("INSERT_DATE_TIME",Utils.getCurrentTime());
			System.out.println("Current date and time="+Utils.getCurrentTime());
			queryMap.put("FILE_HANDLE",mappingData.getLogFileName()+"_"+Utils.getUnixTime());
			System.out.println("File handle value="+mappingData.getLogFileName()+"_"+Utils.getUnixTime());
			insertQuery(mappingData.getHbaseTable(),fieldMappingList,queryMap);
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				LOGGER.error(e.getMessage());
			
			}
			
	}
	public void insertQuery(String hbaseTableName,List<Mapping> fieldMappingList,Map<String,Object> queryMap)
	{
		try {
			System.out.println("In InsertQuery Method");
			Connection con=Utils.getHbaseConnection();
			Set<String> queryFieldSet = queryMap.keySet();
			Collection<Object> objCollection=queryMap.values(); 
			String queryFields="";
			String valueFields="";
			Iterator<String> it=queryFieldSet.iterator();
			while(it.hasNext())
			{
				String field=it.next();
				System.out.println("query field value="+field);
				if(queryFields=="")
				{
					queryFields=queryFields+field;
					valueFields=valueFields+"?";
				}
				else
				{
					queryFields=queryFields+","+field;
					
					valueFields=valueFields+",?";
				}
			}			
			
			String sqlQuery="upsert into "+hbaseTableName+" ("+queryFields+") values("+valueFields+")";
			System.out.println("Sql Query ="+sqlQuery);
		
			PreparedStatement stmt = con.prepareStatement(sqlQuery);
			//Statement stmt=con.createStatement();
			
			Iterator<String> fieldIt=queryFieldSet.iterator();
			Iterator<Object> valueIt=objCollection.iterator();
			int i=1;
			String field;
			while(fieldIt.hasNext())
			{
				Iterator<Mapping> mappingObjIt=fieldMappingList.iterator();
				field=fieldIt.next();
				while(mappingObjIt.hasNext())
				{
					Mapping mappingObj=mappingObjIt.next();
					if(field.equals(mappingObj.getHbaseField())|| field==mappingObj.getHbaseField())
					{
						if(mappingObj.getType().equals("int")|| mappingObj.getType()=="int")
						{
							int fieldVal=(Integer)valueIt.next();
							System.out.println("Field value="+fieldVal);
							stmt.setInt(i,fieldVal );
							
						}
						else if(mappingObj.getType().equals("String")|| mappingObj.getType()=="String")
						{
							String fieldVal=valueIt.next().toString();
							System.out.println("Field value="+fieldVal);
							stmt.setString(i,fieldVal);
						}
						else if(mappingObj.getType().equals("boolean")|| mappingObj.getType()=="boolean")
						{
							boolean fieldVal=(Boolean)valueIt.next();
							System.out.println("Field value="+fieldVal);
							stmt.setBoolean(i, fieldVal);
						}
						else if(mappingObj.getType().equals("double")|| mappingObj.getType()=="double")
						{
							double fieldVal=(Double)valueIt.next();
							System.out.println("Field value="+fieldVal);
							stmt.setDouble(i, fieldVal);
						}
						else if(mappingObj.getType().equals("long")|| mappingObj.getType()=="long")
						{
							long fieldVal=(Long)valueIt.next();
							System.out.println("Field value="+fieldVal);
							stmt.setLong(i, fieldVal);
						}
						
						i++;
						break;
						
					}
					
					else if(field.equals("LOG_DATE")|| field=="LOG_DATE")
					{
						String fieldVal=valueIt.next().toString();
						System.out.println("Field value="+fieldVal);
						stmt.setDate(i,Utils.getDate((long)Double.parseDouble(fieldVal)));
						i++;
						break;
					}
					else if(field.equals("LOG_TIME")|| field=="LOG_TIME")
					{
						String fieldVal=valueIt.next().toString();
						System.out.println("Field value="+fieldVal);
						stmt.setString(i, fieldVal);
						i++;
						break;
					}
					else if(field.equals("DEST_COUNTRY")|| field=="DEST_COUNTRY")
					{
						System.out.println("dest_country=");
						Object fieldVal=valueIt.next();
						if(fieldVal==null || fieldVal=="")
						{
							
							stmt.setString(i,null);
						}
						else
						{	
							System.out.println("Field value="+fieldVal);
							stmt.setString(i,fieldVal.toString());
						}
						i++;
						break;
					}
					else if(field.equals("DEST_LOCATION")|| field=="DEST_LOCATION")
					{
						System.out.println("DEST_LOCATION=");
						Object fieldVal=valueIt.next();
						if(fieldVal==null || fieldVal=="")
						{
							
							stmt.setString(i,null);
						}
						else
						{	
							System.out.println("Field value="+fieldVal);
							stmt.setString(i,fieldVal.toString());
						}
						i++;
						break;
					}
					else if(field.equals("DEST_CITY")|| field=="DEST_CITY")
					{
						Object fieldVal=valueIt.next();
						if(fieldVal==null || fieldVal=="")
						{
							
							stmt.setString(i,null);
						}
						else
						{	
							System.out.println("Field value="+fieldVal);
							stmt.setString(i,fieldVal.toString());
						}
						i++;
						break;
						
					}
					else if(field.equals("SOURCE_COUNTRY")|| field=="SOURCE_COUNTRY")
					{
						Object fieldVal=valueIt.next();
						if(fieldVal==null || fieldVal==""|| fieldVal.equals(null))
						{
							
							stmt.setString(i,null);
						}
						else
						{	
							System.out.println("Field value="+fieldVal);
							stmt.setString(i,fieldVal.toString());
						}
						i++;
						break;
					}
					else if(field.equals("SOURCE_LOCATION")|| field=="SOURCE_LOCATION")
					{
						Object fieldVal=valueIt.next();
						if(fieldVal==null || fieldVal==""|| fieldVal.equals(null))
						{
							
							stmt.setString(i,null);
						}
						else
						{	
							System.out.println("Field value="+fieldVal);
							stmt.setString(i,fieldVal.toString());
						}
						i++;
						break;
					}
					else if(field.equals("SOURCE_CITY")|| field=="SOURCE_CITY")
					{
						Object fieldVal=valueIt.next();
						if(fieldVal==null || fieldVal==""|| fieldVal.equals(null))
						{
							
							stmt.setString(i,null);
						}
						else
						{	
							System.out.println("Field value="+fieldVal);
							stmt.setString(i,fieldVal.toString());
						}
						i++;
						break;
						
					}
					else if(field.equals("INSERT_DATE_TIME")|| field=="INSERT_DATE_TIME")
					{
						String fieldVal=valueIt.next().toString();
						System.out.println("Field value="+fieldVal);
						stmt.setString(i,fieldVal);
						i++;
						break;
						
					}
					else if(field.equals("FILE_HANDLE")|| field=="FILE_HANDLE")
					{
						String fieldVal=valueIt.next().toString();
						System.out.println("Field value="+fieldVal);
						stmt.setString(i,fieldVal);
						i++;
						break;
						
					}
				}
				
				
			}
			//System.out.println("I value="+i);
			
			
			System.out.println("Going to insert into table");
			stmt.executeUpdate();
			con.commit();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOGGER.error(e.getMessage());
		}
		
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

}
