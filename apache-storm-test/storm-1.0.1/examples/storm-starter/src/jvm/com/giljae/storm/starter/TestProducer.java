package com.giljae.storm.starter;

import com.google.common.io.Resources;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;


/**
 * Kafka test producer
 *
 * @author Giljae Joo
 * @date 2016. 6. 28. 오전 11:47:56
 * @version 1.0
 */
public class TestProducer {
	private static final String TOPIC = "test2";

	public static void main(String[] args) throws IOException {
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
			StringBuilder message = new StringBuilder();
			// 첫번째
			message.append("hostip:1.1.1.1,");
			message.append("name:test,");
			message.append("body:blah,");
			message.append("timestamp:"+new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Timestamp(System.currentTimeMillis())));
			producer.send(new ProducerRecord<String, String>(TOPIC, message.toString()));
			Thread.sleep(10);
			// 두번째
			message = new StringBuilder();
			message.append("hostip:2.2.2.2,");
			message.append("name:test,");
			message.append("body:blah,");
			message.append("timestamp:"+new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Timestamp(System.currentTimeMillis())));
			producer.send(new ProducerRecord<String, String>(TOPIC, message.toString()));
			Thread.sleep(10);
			// 세번째
			message = new StringBuilder();
			message.append("hostip:3.3.3.3,");
			message.append("name:test,");
			message.append("body:blah,");
			message.append("timestamp:"+new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Timestamp(System.currentTimeMillis())));
			producer.send(new ProducerRecord<String, String>(TOPIC, message.toString()));
		} catch (Throwable throwable) {
			System.out.printf("%s", throwable.getStackTrace());
		} finally {
			producer.flush();
			producer.close();
		}

	}
}
