package com.bluevigil.streaming;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.bluevigil.model.DerivedFieldMapping;
import com.bluevigil.model.FieldMapping;
import com.bluevigil.model.LogFileConfig;
import com.bluevigil.model.RowKeyField;
import com.bluevigil.utils.BluevigilConstant;
import com.bluevigil.utils.BluevigilProperties;
import com.bluevigil.utils.BluevigilUtils;
import com.bluevigil.utils.DynamicJsonParser;
import com.google.gson.Gson;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * Consumer class to consume data from Kafka topic
 * 
 * @author asif
 *
 */
public class BluevigilConsumer implements Serializable {
	private static final long serialVersionUID = 1L;
	static transient Logger LOGGER = Logger.getLogger(BluevigilConsumer.class);
	private static BluevigilProperties props;

	public void consumeDataFromSource(JavaSparkContext jsc,
			String inputJson) {
		LOGGER.info("BluevigilConsumer - consumeDataFromSource started");
		props = BluevigilProperties.getInstance();
		JavaStreamingContext jssc = new JavaStreamingContext(jsc,
				Durations.seconds(Integer.parseInt(props.getProperty(BluevigilConstant.STREAMING_DURATION_INTERVAL))));
		// Parsing json
		Gson gson = new Gson();
		LogFileConfig mappingData = gson.fromJson(inputJson, LogFileConfig.class);
		String soureTopic = mappingData.getSourceTopic();
		final String destTopic = mappingData.getDestinationTopic();
		Iterator<RowKeyField> rowkeyFieldMappingItr = mappingData.getRowKeyFields().iterator();
		// Rowkey field list, in fields order given in input config file
		final List<String> rowkeyFieldList = new ArrayList<String>();
		RowKeyField rowkeyField = null;
		// Rowkey fields list based on row key field mapping
		while (rowkeyFieldMappingItr.hasNext()) {
			rowkeyField = rowkeyFieldMappingItr.next();
			rowkeyFieldList.add(rowkeyField.getOrder(), rowkeyField.getBackEndField());
		}

		// Required fields list from the config file
		Iterator<FieldMapping> fieldMappingItr = mappingData.getFieldMapping().iterator();
		FieldMapping fieldMapping = null;
		final Map<Integer, String> backendFieldMap = new TreeMap<Integer, String>();
		// Backend field mapping list
		while (fieldMappingItr.hasNext()) {
			fieldMapping = fieldMappingItr.next();
			if (fieldMapping.isRequired()) {
				backendFieldMap.put(fieldMapping.getOrder(), fieldMapping.getBackEndField());
			}
		}
		
		// Derived fields mapping list
		final List<DerivedFieldMapping> derivedFieldMappingList = mappingData.getDerivedFieldMapping();
				
		// Derived mapping details added to backed field mapping list
		Iterator<DerivedFieldMapping> derivedFieldMappingItr = derivedFieldMappingList.iterator();
		DerivedFieldMapping derivedFieldMapping = null;
		while (derivedFieldMappingItr.hasNext()) {
			derivedFieldMapping = derivedFieldMappingItr.next();
			backendFieldMap.put(derivedFieldMapping.getOrder(), derivedFieldMapping.getDerivedField());
		}
		
		// Broadcasting country details for GeoIp enrichment
		// Reading coutry details from the HDFS location
		JavaRDD<String> maxmindCountryDetails = jsc.textFile(props.getProperty("mmdb.geoLocation.Country"));
		JavaPairRDD<Long, String> countryDataPairRDD = maxmindCountryDetails.mapToPair(new PairFunction<String, Long, String>() {
			// Iterating over each line
			// "1.0.0.0","1.0.0.255","16777216","16777471","AU","Australia"
			String stringIpVal;
			long longIpVal;
			String country;
			public Tuple2<Long, String> call(String line) throws Exception {
				String[] lineValues = line.split(BluevigilConstant.COMMA);
				stringIpVal = lineValues[2];
				country = lineValues[5];
				longIpVal = Long.parseLong(stringIpVal.substring(1, stringIpVal.length()-1));
				country = country.substring(1, country.length()-1);
				return new Tuple2<Long, String>(longIpVal, country);
			}
		});
		// Converting countryDataPairRDD to map
		Map<Long, String> countryDataMap = countryDataPairRDD.collectAsMap();
		Long[] countryIpArray = countryDataMap.keySet().toArray(new Long[countryDataMap.keySet().size()]);
		// Sorting array of country ips for binary search
		Arrays.sort(countryIpArray);
		// Broadcasting country data
		final Broadcast<Long[]> broadcastedCountryIpArray = jsc.broadcast(countryIpArray);
		final Broadcast<Map<Long, String>> broadcastedCountryDataMap = jsc.broadcast(countryDataMap);
		// Broadcasting completed
		
		// Consume data from Kafka
		LOGGER.info("Configuring Kafka properties");
		final String bootstrapServers = props.getProperty(BluevigilConstant.BOOTSTRAP_SERVERS);
		String zookeeperServer = props.getProperty(BluevigilConstant.ZOOKEEPER_SERVERS);
		HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(soureTopic.split(BluevigilConstant.COMMA)));
		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put(BluevigilConstant.CONFIG_METADATA_BROKER_LIST, bootstrapServers);
		kafkaParams.put(BluevigilConstant.CONFIG_ZOOKEEPER_CONNECT, zookeeperServer);
		LOGGER.info("Configuring Kafka properties - Done");
		
		// Create direct Kafka stream with brokers and topics
		LOGGER.info("Cosnuming data from Kafka topic");
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
		LOGGER.info("Kafka read - Done");
		
		// Filter messages for empty lines and null values
		LOGGER.info("Filter data from Kafka for empty lines");
		JavaPairDStream<String, String> filteredMessages = messages.filter(new Function<Tuple2<String,String>, Boolean>() {
			public Boolean call(Tuple2<String, String> line) throws Exception {
				return line._2 != null && line._2.trim().length() > 0;
			}
		});
		LOGGER.info("Filtering - Done");
		
		LOGGER.info("Parsing json data");
		JavaDStream<Map<String, String>> parsedJsonMapDStream = filteredMessages.map(new Function<Tuple2<String, String>, Map<String, String>>() {
			private static final long serialVersionUID = 1L;
			public Map<String, String> call(Tuple2<String, String> line) throws Exception {
				String jsonRecord = line._2;
				
				// Parsing json data - started
				Map<String, String> parsedJsonMap = DynamicJsonParser.parseJsonInputLine(jsonRecord,
						backendFieldMap, BluevigilConstant.EMPTY_STRING, new HashMap<String, String>());
				// Initial Parsing of json data - Done
				Map<String, String> finalParsedJsonMap  = DynamicJsonParser.getParsedJsonMapWithDerivedFields(broadcastedCountryIpArray, broadcastedCountryDataMap,
						parsedJsonMap, derivedFieldMappingList);
				// Derived fields data added ParsedJsonMap
				return parsedJsonMap;
			}
		});
		LOGGER.info("Parsing completed");
		
		LOGGER.info("Writing parsed data to Kafka");
		JavaDStream<String> formattedLine = parsedJsonMapDStream.map(new Function<Map<String,String>, String>() {
			String formattedLine = BluevigilConstant.EMPTY_STRING;
			
			public String call(Map<String, String> parsedJsonMap) throws Exception {
				LOGGER.info("Creating Kafka producer config");
				Producer<String, String> producer = createProducer(bootstrapServers);
				LOGGER.info("Creating Kafka producer config - Done");
				LOGGER.info("Formatting parsed json data");
				formattedLine = BluevigilUtils.createLineFromParsedJson(BluevigilConstant.COMMA, parsedJsonMap,
						backendFieldMap);
				ProducerRecord<String, String> finalRecord = new ProducerRecord<String, String>(destTopic, "key",
						formattedLine);
				LOGGER.info("Writing parsed data to Kafka");
				producer.send(finalRecord);
				LOGGER.info("Writing to Kafka completed");
				return formattedLine;
			}
		});
		LOGGER.info("Writing parsed data to Kafka completed");
		
		LOGGER.info("Create Hbase Put object and write to hbase");
		final String hbaseTableName = mappingData.getHbaseTable();
		JavaDStream<Put> hbasePutObjects = null;
		final String ZOOKEEPER_QUORUM = props.getProperty(BluevigilConstant.ZOOKEEPER_QUORUM);
		final String CLIENT_PORT = props.getProperty(BluevigilConstant.ZOOKEEPER_PORT);
		if (BluevigilUtils.isHbaseTableExists(hbaseTableName)) {
			LOGGER.info("Hbase Table "+hbaseTableName+" exists");
			hbasePutObjects = parsedJsonMapDStream.map(new Function<Map<String,String>, Put>() {
				
				public Put call(Map<String, String> parsedJsonMap) throws Exception {
					Put putObj = createHbaseObject(rowkeyFieldList, parsedJsonMap);
					Configuration config = HBaseConfiguration.create();
					config.set(BluevigilConstant.CONFIG_ZOOKEEPER_QUORUM, ZOOKEEPER_QUORUM);
			        config.set(BluevigilConstant.CONFIG_ZOOKEEPER_CLIENT_PORT, CLIENT_PORT);
			        config.set(BluevigilConstant.CONFIG_ZNODE_PARENT, "/hbase-unsecure");
			        Connection connection = null;
			        Table table = null;
			        try {
			            connection = ConnectionFactory.createConnection(config);
			            table = connection.getTable(TableName.valueOf(hbaseTableName));
			            table.put(putObj);
			        } catch (Exception e) {
			        	LOGGER.error("Exception in establishing Hbase connection");
			        } finally {
			            try {
			                if (table != null) {
			                	table.close();
			                }

			                if (connection != null && !connection.isClosed()) {
			                    connection.close();
			                }
			            } catch (Exception e2) {
			            	LOGGER.error("Exception in closing Hbase connection");
			            }
			        }
					
					return putObj;
				}
			});
		} else {
			LOGGER.error("Hbase table doesn't exists - exiting");
		}
		LOGGER.info("Create Hbase Put object and write to hbase - completed");
		formattedLine.print();
		jssc.start();
		jssc.awaitTermination();
	}
	
	public Producer<String, String> createProducer(String bootstrapServers) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "BluevigilDev");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return new KafkaProducer<String, String>(props);
	}
	
	/**
	 * @param rowkeyFieldList
	 *            List of row key fields defined during the input file config
	 * @param parsedJsonMap
	 *            Json map containing parsed data for each field
	 * @return put Hbase put object with all column qualifier name and data
	 */
	public Put createHbaseObject(List<String> rowkeyFieldList, Map<String, String> parsedJsonMap) {
		Map<String, String> parsedJsonMapCopy = new HashMap<String, String>();
		props = BluevigilProperties.getInstance();
		// Copying parsed json map content to a new map as we need to remove
		// row key content from parsed json map.
		parsedJsonMapCopy.putAll(parsedJsonMap);
		// Creating row key string
		StringBuffer rowKey = new StringBuffer(BluevigilConstant.EMPTY_STRING);
		for (int i = 0; rowkeyFieldList.size() > i; i++) {
			rowKey.append(parsedJsonMapCopy.get(rowkeyFieldList.get(i)) + "|");
			parsedJsonMapCopy.remove(rowkeyFieldList.get(i));
		}
		Put put = new Put(Bytes.toBytes(rowKey.toString()));

		String columnFamily = props.getProperty("bluevigil.hbase.column.family.name.primary");
		for (Map.Entry<String, String> entry : parsedJsonMapCopy.entrySet()) {
			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
		}
		return put;
	}
	
}