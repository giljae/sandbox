package com.giljae.storm.starter;

import java.util.Collections;
import java.util.Random;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


/**
 * JSON Formatter Bolt
 *
 * @author Giljae Joo
 * @date 2016. 6. 28. 오후 5:25:45
 * @version 1.0
 */
public class JsonFormatterBolt extends BaseBasicBolt {
	private static final long serialVersionUID = 1L;

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String[] splitArray = input.getString(0).split(",");
		String hostIp = "";
		String name = "";
		String body = "";
		String timestamp = "";

		for (int i = 0; i < splitArray.length; i++) {
			if (splitArray[i].contains("hostip"))
				hostIp = splitArray[i];
			if (splitArray[i].contains("name"))
				name = splitArray[i];
			if (splitArray[i].contains("body"))
				body = splitArray[i];
			if (splitArray[i].contains("timestamp"))
				timestamp = splitArray[i];
		}
		
		System.out.println("hostIp:"+hostIp);
		System.out.println("name:"+name);
		System.out.println("body:"+body);
		System.out.println("timestamp:"+timestamp);
		
		StringBuilder json = new StringBuilder(); // 테스트를 위한 코드
		json.append("{");
		json.append("\"hostip\":\"" + hostIp.split(":")[1] + "\",");
		json.append("\"name\":\"" + name.split(":")[1] + "\",");
		json.append("\"body\":\"" + body.split(":")[1] + "\",");
		json.append("\"timestamp\":\"" + timestamp.split(":")[1] + "\"");
		json.append("}");
		
		//Sleep 처리
		try {
			Random random = new Random();
			Thread.sleep(random.nextInt(10)*1000);
		}catch (InterruptedException ie) {
			ie.printStackTrace();
		}

		collector.emit(new Values(json.toString()));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("json"));
	}
}
