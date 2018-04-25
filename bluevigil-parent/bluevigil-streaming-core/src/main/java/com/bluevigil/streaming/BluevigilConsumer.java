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
import java.util.Properties;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
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

import com.bluevigil.model.FieldMapping;
import com.bluevigil.model.LogFileConfig;
import com.bluevigil.model.RowKeyField;
import com.bluevigil.utils.BluevigilConstant;
import com.bluevigil.utils.BluevigilProperties;
import com.bluevigil.utils.DynamicJsonParser;
import com.bluevigil.utils.Utils;

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

	public void consumeDataFromSource(String soureTopic, final String destTopic, final String bootstrapServers,
			String zookeeperServer, JavaStreamingContext jssc, LogFileConfig mappingData) {

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
				backendFieldMap.put(fieldMapping.getOrder(), fieldMapping.getBackEndField());
			}
		}

		HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(soureTopic.split(",")));
		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", bootstrapServers);
		kafkaParams.put("zookeeper.connect", zookeeperServer);

		// Create direct Kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);
		
		Configuration hbaseConfig = HBaseConfiguration.create();
		HTable hTable = null;
		try {
			hTable = new HTable(hbaseConfig, "student");
		} catch (IOException e) {
			e.printStackTrace();
		}
		final HTable finalHTable = hTable;
		final Producer<String, String> producer = Utils.createProducer(bootstrapServers);
		LOGGER.info("Consume data available in the source topic");
		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
			private static final long serialVersionUID = 1L;

			public String call(Tuple2<String, String> line) throws Exception {
				String jsonRecord = line._2;
				// Parse the input json line
				Map<String, String> parsedJsonMap = DynamicJsonParser.parseJsonInputLine(jsonRecord, backendFieldMap,
						BluevigilConstant.EMPTY_STRING, new HashMap<String, String>());
				// Create Hbase Put object with the parsed data
				finalHTable.put(DynamicJsonParser.createHbaseObject(rowkeyFieldList, parsedJsonMap));
				
				// Create a comma separated line in the defined column order
				String formattedLine = Utils.createLineFromParsedJson(BluevigilConstant.COMMA, parsedJsonMap, backendFieldMap);
				
				// Send the comma separated line to Kafka to consume by web UI
				ProducerRecord<String, String> finalRecord = new ProducerRecord<String, String>(destTopic, "key", formattedLine);
				producer.send(finalRecord);
				return formattedLine;
			}

		});

		jssc.start();
		jssc.awaitTermination();
	}
}