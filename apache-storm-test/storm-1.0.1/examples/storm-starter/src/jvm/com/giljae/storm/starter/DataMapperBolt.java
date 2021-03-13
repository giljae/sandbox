package com.giljae.storm.starter;

import java.util.Random;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


/**
 * Data Mapper bolt
 *
 * @author Giljae Joo
 * @date 2016. 6. 28. 오후 5:33:39
 * @version 1.0
 */
public class DataMapperBolt extends BaseBasicBolt {
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String json = input.getStringByField("json");
		JSONParser parser = new JSONParser();
		JSONObject jsonObj = null;

		try {
			jsonObj = (JSONObject) parser.parse(json);
			jsonObj.put("body", "TANGO");
			json = jsonObj.toString();
		} catch (ParseException pe) {
			pe.printStackTrace();
		}

		// Sleep 처리
		try {
			Random random = new Random();
			Thread.sleep(random.nextInt(10) * 1000);
		} catch (InterruptedException ie) {
			ie.printStackTrace();
		}

		collector.emit(new Values(json));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("json"));
	}
}
