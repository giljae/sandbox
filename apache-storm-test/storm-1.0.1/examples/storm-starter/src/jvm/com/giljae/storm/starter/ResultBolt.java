package com.giljae.storm.starter;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


/**
 * Result Bolt
 *
 * @author Giljae Joo
 * @date 2016. 6. 28. 오후 5:42:09
 * @version 1.0
 */
public class ResultBolt extends BaseBasicBolt {
	private static final String TOPIC = "result";

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String resultJson = input.getStringByField("json");
		System.out.println("\n\n");
		System.out.println("===================== Result ====================");
		System.out.println(resultJson);
		System.out.println("=================================================");
		System.out.println("\n\n");
		collector.emit(new Values(resultJson));

		// Kafka message
		// set up the producer
		KafkaProducer<String, String> producer;
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "52.78.15.181:9092");
		properties.put("acks", "all");
		properties.put("retries", "0");
		properties.put("batch.size", "16384");
		properties.put("linger.ms", "0");
		properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<>(properties);

		try {
			producer.send(new ProducerRecord<String, String>(TOPIC, resultJson));
		} catch (Throwable throwable) {
			System.out.printf("%s", throwable.getStackTrace());
		} finally {
			producer.flush();
			producer.close();
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("resultJson"));
	}
}
