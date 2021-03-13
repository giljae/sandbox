package com.giljae.storm.starter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


/**
 * Test Parellel Topology
 *
 * @author Giljae Joo
 * @date 2016. 6. 28. 오후 5:09:13
 * @version 1.0
 */
public class TestParellelTopology {
	public static void main(String[] args) throws Exception {

		String zkUrl = "52.78.15.181:2181"; // zookeeper url
		String brokerUrl = "52.78.15.181:9092";

		System.out.println("Using Kafka zookeeper url: " + zkUrl + " broker url: " + brokerUrl);

		ZkHosts hosts = new ZkHosts(zkUrl);
		SpoutConfig spoutConfig = new SpoutConfig(hosts, "test1", "/test1", UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		// Topology
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafkaSpout", kafkaSpout, 1);
		builder.setBolt("jsonFormatterBolt", new JsonFormatterBolt(), 3).shuffleGrouping("kafkaSpout");
		builder.setBolt("dataMapperBolt", new DataMapperBolt(), 3).fieldsGrouping("jsonFormatterBolt",
				new Fields("json"));
		builder.setBolt("resultBolt", new ResultBolt(), 3).fieldsGrouping("dataMapperBolt", new Fields("json"));

		Config conf = new Config();
		conf.setDebug(true);
		List<String> nimbus_seeds = new ArrayList<String>();
		nimbus_seeds.add("52.78.15.181");

		// =============================
		// cluster mode
		// =============================
		conf.put(Config.NIMBUS_HOST, "52.78.15.181");
		// conf.put(Config.STORM_LOCAL_DIR, "your storm local dir");
		conf.put(Config.NIMBUS_THRIFT_PORT, 6627);
		conf.put(Config.STORM_ZOOKEEPER_PORT, 2181);
		conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList(new String[] { "52.78.15.181" }));
		StormSubmitter.submitTopology("parellelTopology", conf, builder.createTopology());

	}
}
