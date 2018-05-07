package com.bluevigil.streaming;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.bluevigil.model.DerivedFieldMapping;
import com.bluevigil.model.FieldMapping;
import com.bluevigil.model.LogFileConfig;
import com.bluevigil.model.RowKeyField;
import com.bluevigil.utils.BluevigilConstant;
import com.bluevigil.utils.BluevigilProperties;
import com.bluevigil.utils.DynamicJsonParser;
import com.bluevigil.utils.Utils;
import com.sun.tools.internal.jxc.gen.config.Config;

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
	private static BluevigilProperties props = BluevigilProperties.getInstance();

	public void consumeDataFromSource(String soureTopic, final String destTopic, JavaStreamingContext jssc,
			final LogFileConfig mappingData) {
		final String bootstrapServers = props.getProperty("bluevigil.bootstrap.servers");
		String zookeeperServer = props.getProperty("bluevigil.zookeeper.servers");
		Iterator<FieldMapping> fieldMappingItr = mappingData.getFieldMapping().iterator();
		Iterator<RowKeyField> rowkeyFieldMappingItr = mappingData.getRowKeyFields().iterator();
		// Required fields list from the config file
		final Map<Integer, String> backendFieldMap = new TreeMap<Integer, String>();
		// Rowkey field list, in fields order given in input config file
		final List<String> rowkeyFieldList = new ArrayList<String>();
		FieldMapping fieldMapping = null;
		RowKeyField rowkeyField = null;

		// Rowkey fields list based on row key field mapping
		while (rowkeyFieldMappingItr.hasNext()) {
			rowkeyField = rowkeyFieldMappingItr.next();
			rowkeyFieldList.add(rowkeyField.getOrder(), rowkeyField.getBackEndField());
		}

		// Backend field mapping list
		while (fieldMappingItr.hasNext()) {
			fieldMapping = fieldMappingItr.next();
			if (fieldMapping.isRequired()) {				
				{
					backendFieldMap.put(fieldMapping.getOrder(), fieldMapping.getBackEndField());
				}			
			}
		}
		
		//Jsoon parser Tester
		/*System.out.println("test="+mappingData.getDerivedFieldMapping().toString());
		String jsonRecordTest="{\"ts\":1510665261.593545,\"uid\":\"CLPj1c2y2azVqpC1Tg\",\"nested\":{\"id.orig_host\":{\"host\":\"192.168.100.9\",\"id.orig_port\":45224},\"id.resp_host\":\"216.58.208.78\"},\"id.resp_port\":80}";
		Map<String, String> parsedJsonMap = DynamicJsonParser.parseJsonInputLine(jsonRecordTest,
				backendFieldMap, BluevigilConstant.EMPTY_STRING, new HashMap<String, String>(),mappingData.getDerivedFieldMapping());
		*/
		//Jsoon parser Tester end
		
		HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(soureTopic.split(",")));
		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", bootstrapServers);
		kafkaParams.put("zookeeper.connect", zookeeperServer);

		// Create direct Kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

//		Configuration config = HBaseConfiguration.create();
//		config.set("hbase.zookeeper.quorum", props.getProperty("bluevigil.zookeeper.quorum"));
//		config.set("hbase.zookeeper.property.clientPort", props.getProperty("bluevigil.zookeeper.port"));
//		Connection connection = null;
//		Table table = null;
//		try {
			
//			final Table finalHTable = table;
			final String tableName = mappingData.getHbaseTable();
			
			LOGGER.info("Consume data available in the source topic");
			JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
				private static final long serialVersionUID = 1L;
				Connection connection = null;
				Table table = null;
				Producer<String, String> producer = null;				
				public void Function() {
					Configuration config = HBaseConfiguration.create();
					config.set("hbase.zookeeper.quorum", props.getProperty("bluevigil.zookeeper.quorum"));
					
					try {
						connection = ConnectionFactory.createConnection(config);
						table = connection.getTable(TableName.valueOf(tableName));
						producer = Utils.createProducer(bootstrapServers);						
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				public String call(Tuple2<String, String> line) throws Exception {
					String jsonRecord = line._2;
					System.out.println("in call method");
					// Parse the input json line
					Map<String, String> parsedJsonMap = DynamicJsonParser.parseJsonInputLine(jsonRecord,
							backendFieldMap, BluevigilConstant.EMPTY_STRING, new HashMap<String, String>(),mappingData.getDerivedFieldMapping());
					// Create Hbase Put object with the parsed data
					table.put(DynamicJsonParser.createHbaseObject(rowkeyFieldList, parsedJsonMap));
					
					// Create a comma separated line in the defined column order
					String formattedLine = Utils.createLineFromParsedJson(BluevigilConstant.COMMA, parsedJsonMap,
							backendFieldMap);

					// Send the comma separated line to Kafka to consume by web
					// UI
					ProducerRecord<String, String> finalRecord = new ProducerRecord<String, String>(destTopic, "key",
							formattedLine);
					producer.send(finalRecord);
					return formattedLine;
				}

			});
			System.out.println(lines.count());
//		} catch (Exception e) {
//			e.printStackTrace();
//		} finally {
//			try {
//				if (table != null) {
//					table.close();
//				}
//
//				if (connection != null && !connection.isClosed()) {
//					connection.close();
//				}
//			} catch (Exception e2) {
//				e2.printStackTrace();
//			}
//		}
		jssc.start();
		jssc.awaitTermination();
	}
}