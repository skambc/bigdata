package com.inspur.bigdata.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerTest extends Thread{
	
	private Properties  props = new Properties();
	private KafkaProducer<String, String> producer;
	private String topic;
	private String SERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringSerializer";
	
	public ProducerTest(String topic){
		props.put("key.serializer", SERIALIZER_CLASS); //指定key的序列化类
		props.put("value.serializer", SERIALIZER_CLASS);//指定value的序列化类
		props.put("bootstrap.servers", "hd1.bigdata:6667,");//kafka服务地址
		producer = new KafkaProducer<String,String>(props);
		this.topic = topic;
	}
	
	public void run(){
		int msgNO = 1;
		while(true){
			String msgStr = new String("\"msgNO\":" + msgNO);
			System.out.println(msgStr);
			ProducerRecord<String, String> msg = new ProducerRecord<String, String>
			(topic, null, msgStr);
			producer.send( msg ); //发送消息
			msgNO++;
		}
	}
	
	public static void main(String []args){
		ProducerTest producerTest = new ProducerTest("test20171012");
		producerTest.start();
		System.out.println("out");
	}
}
