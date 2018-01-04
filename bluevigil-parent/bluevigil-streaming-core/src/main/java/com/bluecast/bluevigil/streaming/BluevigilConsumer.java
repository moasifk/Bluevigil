package com.bluecast.bluevigil.streaming;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

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

import com.bluecast.bluevigil.utils.Utils;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class BluevigilConsumer implements Serializable {

	private static String BOOTSTRAP_SERVERS = "localhost:9092";
	private static String ZOOKEEPER_SERVER = "localhost:2181";

	public void consumeDataFromSource(String soureTopic, final String destTopic, JavaStreamingContext jssc) {
		HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(soureTopic.split(",")));
		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", BOOTSTRAP_SERVERS);
		kafkaParams.put("zookeeper.connect", ZOOKEEPER_SERVER);

		// Create direct Kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

		// Get the lines, split them into words, count the words and print
		JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
			public String call(Tuple2<String, String> line) throws Exception {
				System.out.println(line._2());
				return line._2();
			}

		});

		final Utils utils = new Utils();
		lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			public void call(JavaRDD<String> rdd) throws Exception {
				rdd.foreach(new VoidFunction<String>() {
					public void call(String s) throws Exception {
						Producer<String, String> producer = utils.createProducer();
						ProducerRecord<String, String> record = new ProducerRecord<String, String>(destTopic, "key", s);
						RecordMetadata metadata = producer.send(record).get();
					}
				});
			}
		});

		lines.print();

		// Execute the Spark workflow defined above
		jssc.start();
		jssc.awaitTermination();

	}

}
