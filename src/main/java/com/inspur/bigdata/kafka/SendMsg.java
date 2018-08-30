package com.inspur.bigdata.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SendMsg {
	public static void sendKafka(String sendTopic, String brokerList, String message) {
		Properties props = new Properties();
		props.put("bootstrap.servers", brokerList);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer kafkaProducer = new KafkaProducer<String, String>(props);
//		message = String.valueOf(System.currentTimeMillis());
		ProducerRecord<String, String> data = new ProducerRecord(sendTopic, null, message);
		kafkaProducer.send(data);
		kafkaProducer.close();
	}
	public static void main(String []args){
		System.out.println("Hello. My friend.");
		if(args.length < 3){
			System.out.println("Please enter the param: [Topic] [broker-list] [message]");
			return;
		}
		String topic = args[0];
		String brokerList = args[1];
		String message = args[2];
		SendMsg.sendKafka(topic, brokerList, message);
		
	}

}
