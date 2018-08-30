package com.inspur.bigdata.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class ConsumerTest {

	public void consumer() {
		Properties props = new Properties();
		String topic = "test20171012";
		
		// zookeeper地址
		props.put("zookeeper.connect", "10.166.16.135:2181");
		
		// 消费者都会属于一个消费组，指定消费组id
		props.put("group.id", "consumerTest");

		// 设置使用最开始的offset偏移量为该group.id的最早；
		// 默认是latest，即该topic最新一个消息的offset；
		// 如果采用latest，消费者只能得到其启动后，生产者生产的消息；
		props.put("auto.offset.reset", "smallest");
		
		// 设置key以及value的解析（反序列）类
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		ConsumerConfig config =new ConsumerConfig(props);
	    ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);
	    
	    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, java.util.List<KafkaStream<byte[], byte[]>>> consumerMap = 
        		consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            System.out.println("Receive->[" + new String(it.next().message()) + "]");
        }
	}

	public static void main(String[] args) {
		ConsumerTest test = new ConsumerTest();
		test.consumer();

	}
}
